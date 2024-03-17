use std::{
    mem,
    time::{Duration, Instant},
};

use bincode::Options;
use log::trace;
use serialport::{DataBits, FlowControl, Parity, SerialPort, StopBits};
use tokio_stream::wrappers::UnboundedReceiverStream;

//
// Standard serial communication settings for the PMS5003
//
const BAUD_RATE: u32 = 9600;
const DATA_BITS: DataBits = DataBits::Eight;
const FLOW_CONTROL: FlowControl = FlowControl::None;
const PARITY: Parity = Parity::None;
const STOP_BITS: StopBits = StopBits::One;

// Fixed delay when waiting for new data from the sensor. The sensor generates
// around 70 samples per minute, or one every 860ms, so this value is 1/4 of
// that, intended to minimize the chance of delaying a measurement, but not
// over-polling.
const PMS5003_POLL_DELAY: Duration = Duration::from_millis(215);

// Valid sensor frames begin with the sequence b"BM"
const START_OF_FRAME_0: u8 = b'B';
const START_OF_FRAME_1: u8 = b'M';
// ...followed by the frame size
const EXPECT_FRAME_SIZE: usize = mem::size_of::<Frame>();
const FULL_FRAME_SIZE: usize = EXPECT_FRAME_SIZE + 4;

/// A PMS5003 data frame, verbatim
#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
pub struct Frame {
    /// PM1.0 concentration in standard units
    pub pm10_standard: u16,
    /// PM2.5 concentration in standard units
    pub pm25_standard: u16,
    /// PM10.0 concentration in standard units
    pub pm100_standard: u16,

    /// PM1.0 concentration in environmental units
    pub pm10_env: u16,
    /// PM2.5 concentration in environmental units
    pub pm25_env: u16,
    /// PM10.0 concentration in environmental units
    pub pm100_env: u16,

    /// Number of 0.3µm particles detected per 0.1L unit of air
    pub particles_03um: u16,
    /// Number of 0.5µm particles detected per 0.1L unit of air
    pub particles_05um: u16,
    /// Number of 1.0µm particles detected per 0.1L unit of air
    pub particles_10um: u16,
    /// Number of 2.5µm particles detected per 0.1L unit of air
    pub particles_25um: u16,
    /// Number of 5.0µm particles detected per 0.1L unit of air
    pub particles_50um: u16,
    /// Number of 10.0µm particles detected per 0.1L unit of air
    pub particles_100um: u16,

    _unused: u16,
    checksum: u16,
}

/// A stream of data from the sensor
pub type FrameStream = UnboundedReceiverStream<Result<Frame, Error>>;

/// The final type for the bincode decoder
type Decoder = bincode::config::WithOtherIntEncoding<
    bincode::config::WithOtherEndian<bincode::DefaultOptions, bincode::config::BigEndian>,
    bincode::config::FixintEncoding,
>;

pub struct Pms5003 {
    serial_port: Box<dyn SerialPort>,
    decoder: Decoder,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("decoding frame: {0}")]
    Bincode(#[from] bincode::Error),

    // `serialport` returns this error type when opening, rather than its custom
    // error type.
    #[error("checking serial port: {0}")]
    FuturesIO(#[from] futures_io::Error),

    #[error("reading from serial port: {0}")]
    SerialPort(#[from] serialport::Error),

    #[error("timed out waiting for valid frame")]
    Timeout,

    #[error("invalid frame checksum (expected {expected}, decoded {decoded}")]
    Checksum { expected: u16, decoded: u16 },
}

impl Pms5003 {
    /// Instantiate a low-level interface to the PMS5003 sensor
    pub fn new<PATH: Into<std::borrow::Cow<'static, str>>>(
        path: PATH,
    ) -> Result<Self, serialport::Error> {
        let serial_port = serialport::new(path, BAUD_RATE)
            .data_bits(DATA_BITS)
            .flow_control(FLOW_CONTROL)
            .parity(PARITY)
            .stop_bits(STOP_BITS)
            .open()?;
        let decoder = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();

        Ok(Pms5003 {
            serial_port,
            decoder,
        })
    }

    /// Return a data stream from the PMS5003 sensor.  Each value is a
    /// `Result<>` containing a valid `Frame`, or an error (which will be the
    /// final value).
    pub fn data_stream(
        mut self,
        timeout: Duration,
    ) -> UnboundedReceiverStream<Result<Frame, Error>> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // The monitor is spawned as a standalone thread as it performs blocking IO
        std::thread::spawn(move || loop {
            if let Err(e) = tx.send(self.get_frame(timeout)) {
                // Silently exit if unable to relay frame
                trace!("failed to relay frame: {e}");
                break;
            }
        });

        rx.into()
    }

    /// Poll the serial port for a valid PMS5003 frame. A frame must be recieved
    /// within the specified timeout.
    fn get_frame(&mut self, timeout: Duration) -> Result<Frame, Error> {
        // The portion of the checksum that is always the same. No need to sum
        // the initial four bytes
        const SUM_BASE: u16 = b'B' as u16
            + b'M' as u16
            + (EXPECT_FRAME_SIZE & 0xff) as u16
            + (EXPECT_FRAME_SIZE >> 8 & 0xff) as u16;

        // This is designed as a state engine due to the very primitive serial
        // interface (which doesn't provide buffering).
        #[derive(Debug)]
        enum ReadState {
            // Expecting b'B' (first start-of-frame byte)
            SOF1,
            // Expecting b'M' (second start-of-frame byte)
            SOF2,
            // Expecting frame size (fixed)
            Size,
            // Expecting frame data
            Data,
        }

        let start_time = Instant::now();
        let mut framebuf = [0u8; FULL_FRAME_SIZE];
        let mut state = ReadState::SOF1;

        loop {
            if start_time.elapsed() > timeout {
                break Err(Error::Timeout);
            }

            let readbuf = match state {
                ReadState::SOF1 => &mut framebuf[0..=0],
                ReadState::SOF2 => &mut framebuf[1..=1],
                ReadState::Size => &mut framebuf[2..=3],
                ReadState::Data => &mut framebuf[4..],
            };

            // Block until enough bytes are ready
            let bytes_ready = self.serial_port.bytes_to_read()? as usize;
            trace!("{bytes_ready} bytes ready");
            if bytes_ready < readbuf.len() {
                trace!(
                    "{state:?} bytes_ready({bytes_ready}) < {} for {state:?}. Waiting {PMS5003_POLL_DELAY:?} before polling", readbuf.len(),
                );
                std::thread::sleep(PMS5003_POLL_DELAY);
                continue;
            } else {
                trace!("{state:?} bytes_ready({bytes_ready})");
            }

            // This function doesn't return a useful success value, despite its
            // documentation.
            let _ = self.serial_port.read(readbuf)?;

            match state {
                ReadState::SOF1 => {
                    // Expect b'B'
                    if readbuf[0] == START_OF_FRAME_0 {
                        state = ReadState::SOF2;
                        // Otherwise, keep looking for b'B'
                    }
                }
                ReadState::SOF2 => match readbuf[0] {
                    // Expect b'M'
                    START_OF_FRAME_1 => state = ReadState::Size,
                    // Previous byte false start-of-frame, and this is the actual?
                    START_OF_FRAME_0 => continue,
                    // Previous byte was false start-of-frame.
                    _ => state = ReadState::SOF1,
                },
                ReadState::Size => {
                    // readbuf is 2 bytes here, so the slice-to-array conversion
                    // can't fail
                    let frame_size = usize::from(u16::from_be_bytes(readbuf.try_into().unwrap()));
                    if frame_size == EXPECT_FRAME_SIZE {
                        state = ReadState::Data;
                    } else {
                        // Size was garbage. Just wait for another start-of-frame marker
                        state = ReadState::SOF1;
                    }
                }
                ReadState::Data => {
                    // Validate the checksum
                    let frame: Frame = self.decoder.deserialize(readbuf)?;
                    let expected: u16 =
                        SUM_BASE + readbuf.iter().take(26).copied().map(u16::from).sum::<u16>();
                    break if expected != frame.checksum {
                        // Just return an error rather than waiting for another
                        // frame at this point. The sensor may need to be
                        // reopened or reset.
                        Err(Error::Checksum {
                            expected,
                            decoded: frame.checksum,
                        })
                    } else {
                        Ok(frame)
                    };
                }
            }
        }
    }
}
