use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aqi::AirQuality;
use chrono::{offset::Local, DateTime};
use log::{debug, warn};
use tokio::{
    select,
    sync::{
        mpsc::{self, error::SendError},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use crate::pms5003::{self, Frame, FrameStream};

/// An AQI monitor that can be queried for current or recent air quality
/// statistics.
pub struct Monitor {
    tx: mpsc::UnboundedSender<MonitorQuery>,
    task: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Pms5003(#[from] pms5003::Error),
    #[error(transparent)]
    Send(#[from] SendError<MonitorQuery>),
}

impl Monitor {
    /// Create and start the monitor as a background task
    pub async fn new(
        mut frame_stream: FrameStream,
        granularity: Duration,
        retention: Duration,
    ) -> Result<Self, serialport::Error> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<MonitorQuery>();

        let task = tokio::spawn(async move {
            let mut state = AqiState::new(granularity, retention);
            let mut failure = None;

            loop {
                select! {
                    Some(frame_result) = frame_stream.next() => {
                        match frame_result {
                            Ok(frame) =>  state.add_frame(&frame),
                            Err(e) => {
                                warn!("Error receiving frame: {e}");
                                // Save it for the next status query
                                failure = Some(e);
                                continue;
                            }
                        };
                    }
                    reply_tx = rx.recv() => {
                        match reply_tx {
                            None => break,
                            Some(query) => match query {
                                MonitorQuery::Recent(reply_tx, n_samples) => {
                                    if let Some(failure) = failure.take() {
                                        let _ = reply_tx.send(Err(failure));
                                        // Terminate the task once the error has been relayed
                                        break;
                                    } else if reply_tx.send(Ok(state.recent(n_samples))).is_err() {
                                        // Receiver no longer available
                                        break;
                                    };
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Self { tx, task })
    }

    /// Request recent air quality snapshots
    pub async fn recent(&self, n_samples: usize) -> Result<Vec<AqiSample>, Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(MonitorQuery::Recent(tx, n_samples))?;
        Ok(rx.await.unwrap()?)
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        debug!("Stopping sensor monitor task");
        self.task.abort();
    }
}

/// Track a rolling set of Aqi calculations over a 24-hour period
#[derive(Default)]
pub struct AqiState {
    slots: Vec<Option<FrameStats>>,
    last_slot: Option<usize>,
    granularity: u64,
}

#[derive(Debug)]
pub struct AqiSample {
    /// The sample's interval index.  0 indicates that the sample is in the
    /// current interval (which is likely incomplete), 1 is the pervious
    /// interval, etc.  Interval durations are specified by the granularity
    /// given when the monitor was created. E.g., one minute.
    interval: usize,

    start: SystemTime,
    samples: u32,
    pm10_sum: u32,
    pm25_sum: u32,
    pm10_aqi: Option<u32>,
    pm25_aqi: Option<u32>,
}

impl std::fmt::Display for AqiSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start: DateTime<Local> = self.start.into();
        write!(
            f,
            "{} {:3} samples={:3} pm10_sum={:2} pm25_sum={:2} pm10_aqi={:2} pm25_aqi={:2}",
            start.format("%H:%M"),
            if self.interval == 0 { "CUR" } else { "" },
            self.samples,
            self.pm10_sum,
            self.pm25_sum,
            self.pm10_aqi.unwrap_or_default(),
            self.pm25_aqi.unwrap_or_default(),
        )
    }
}

impl AqiState {
    pub fn new(granularity: Duration, retention: Duration) -> Self {
        let granularity = granularity.as_secs();
        let retention = retention.as_secs();
        let n_slots = (retention / granularity) as usize;

        AqiState {
            slots: vec![None; n_slots],
            last_slot: None,
            granularity,
        }
    }

    pub fn add_frame(&mut self, frame: &Frame) {
        let (cur_slot, _) = self.cur_slot();
        if Some(cur_slot) == self.last_slot {
            // Modify the slot in-place
            if let Some(Some(slot)) = self.slots.get_mut(cur_slot) {
                *slot = slot.add_frame(frame);
            } else {
                // Something should always be present
                unreachable!()
            }

            // For grins, here's the same thing written as a transform
            // ```
            // self.slots.get_mut(cur_slot).and_then(|slot_option| {
            //     slot_option
            //         .as_mut()
            //         .map(|slot_value| *slot_value = slot_value.add_frame(frame))
            // });
            // ```
        } else {
            // Replace this slot
            self.slots[cur_slot] = Some(frame.into());
        }

        self.last_slot = Some(cur_slot);
    }

    pub fn recent(&self, n_samples: usize) -> Vec<AqiSample> {
        let mut samples = vec![];
        let (cur_slot, time) = self.cur_slot();

        for interval in 0usize..n_samples {
            let n_slots = self.slots.len();
            let offset = (cur_slot + n_slots.checked_sub(interval).unwrap()) % n_slots;
            if let Some(Some(slot)) = self.slots.get(offset) {
                let pm10_aqi = aqi::pm10(slot.pm10_standard_sum as f64 / slot.samples as f64)
                    .as_ref()
                    .map(AirQuality::aqi)
                    .ok();
                let pm25_aqi = aqi::pm2_5(slot.pm25_standard_sum as f64 / slot.samples as f64)
                    .as_ref()
                    .map(AirQuality::aqi)
                    .ok();
                let start = SystemTime::UNIX_EPOCH
                    + Duration::from_secs(time - self.granularity * interval as u64);
                samples.push(AqiSample {
                    interval,
                    start,
                    samples: slot.samples,
                    pm10_sum: slot.pm10_standard_sum,
                    pm25_sum: slot.pm25_standard_sum,
                    pm10_aqi,
                    pm25_aqi,
                })
            }
        }

        samples
    }

    /// Return the rotating index and "floored" timestamp for the current time
    fn cur_slot(&self) -> (usize, u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let this_timeslot = now - (now % self.granularity);
        // Resolve to rotating index
        let cur_slot = ((this_timeslot / self.granularity) % self.slots.len() as u64) as usize;
        (cur_slot, this_timeslot)
    }
}

#[derive(Debug, Clone, Copy)]
struct FrameStats {
    samples: u32,
    pm10_standard_sum: u32,
    pm25_standard_sum: u32,
}

#[derive(Debug)]
pub enum MonitorQuery {
    Recent(
        oneshot::Sender<Result<Vec<AqiSample>, pms5003::Error>>,
        usize,
    ),
}

impl From<&Frame> for FrameStats {
    fn from(frame: &Frame) -> Self {
        FrameStats {
            samples: 1,
            pm10_standard_sum: frame.pm10_standard as u32,
            pm25_standard_sum: frame.pm25_standard as u32,
        }
    }
}

impl FrameStats {
    pub fn add_frame(mut self, frame: &Frame) -> Self {
        self.samples += 1;
        self.pm10_standard_sum += frame.pm10_standard as u32;
        self.pm25_standard_sum += frame.pm25_standard as u32;
        self
    }
}
