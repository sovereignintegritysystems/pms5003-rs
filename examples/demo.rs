use std::time::Duration;

use pms5003::{FrameStream, Pms5003};

use anyhow::Result;
use clap::Parser;
use log::{debug, error};
use tokio_stream::StreamExt;

#[derive(Debug, Parser)]
struct Opts {
    /// Serial device path
    #[arg(long, short, default_value = "/dev/serial0")]
    device: String,

    /// Timeout for serial operations
    #[arg(long, short, default_value = "10s")]
    timeout: humantime::Duration,

    /// Granularity
    #[arg(long, short, default_value = "1m")]
    granularity: humantime::Duration,

    /// Recent (complete) samples to show
    #[arg(long, short, default_value = "5")]
    recent: u64,

    /// Operational mode
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Mode {
    /// Raw PM5003 output
    Raw,
    /// Summarized measurements and computed AQI
    Aqi,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Go ahead and report log messages from the sensor module (defaulting to
    // WARN level).
    env_logger::init();

    let opts = Opts::parse();

    let frame_stream = Pms5003::new(opts.device.clone())?.data_stream(opts.timeout.into());
    match opts.mode {
        Mode::Raw => raw(frame_stream).await,
        Mode::Aqi => aqi(frame_stream, &opts).await,
    }
}

async fn raw(mut frame_stream: FrameStream) -> Result<()> {
    while let Some(result) = frame_stream.next().await {
        let frame = result?;
        println!("{frame:?}");
    }

    Ok(())
}

async fn aqi(frame_stream: FrameStream, opts: &Opts) -> Result<()> {
    let granularity: Duration = opts.granularity.into();
    // Add one to allow for a partial sample period
    let retention = Duration::from_secs(granularity.as_secs() * (opts.recent + 1));

    println!(
        "Aggregating samples into {} periods, showing last {} complete periods.",
        opts.granularity,
        opts.recent + 1
    );
    println!();

    let monitor = pms5003::aqi::Monitor::new(frame_stream, granularity, retention).await?;

    loop {
        let mut n_reports = 0;
        loop {
            match monitor.recent(5).await {
                Ok(samples) => {
                    if n_reports > 0 && samples.len() > 1 {
                        println!("-----");
                    }
                    for sample in samples.iter().rev() {
                        println!("{sample}");
                    }
                    n_reports += 1;
                }
                Err(e) => {
                    error!("Error: {e}");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        debug!("pausing before reopening serial port");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
