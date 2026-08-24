use std::{sync::Arc, time::Duration};

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio::{sync::Semaphore, task::JoinSet, time::interval};

use crate::infrai_queue::{InfraiQueue, QueueClientError, QueueMessage};

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GameJob {
    PlayerAsset { asset_id: String, bytes: u64 },
    LiveEvent { event_id: String, players: u32 },
    Moderation { report_id: String, severity: u8 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobDecision {
    GenerateAsset { asset_id: String },
    FanOutEvent { event_id: String },
    EscalateReport { report_id: String },
    DropOversizedAsset { asset_id: String },
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Queue(#[from] QueueClientError),
    #[error("message {message_id} has invalid game job JSON: {source}")]
    InvalidJob { message_id: String, source: serde_json::Error },
    #[error("worker task ended unexpectedly: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub fn decide(job: GameJob) -> JobDecision {
    match job {
        GameJob::PlayerAsset { asset_id, bytes } if bytes > 25 * 1024 * 1024 => {
            JobDecision::DropOversizedAsset { asset_id }
        }
        GameJob::PlayerAsset { asset_id, .. } => JobDecision::GenerateAsset { asset_id },
        GameJob::LiveEvent { event_id, .. } => JobDecision::FanOutEvent { event_id },
        GameJob::Moderation { report_id, severity } if severity >= 8 => {
            JobDecision::EscalateReport { report_id }
        }
        GameJob::Moderation { report_id, .. } => JobDecision::EscalateReport { report_id },
    }
}

pub async fn run_batch(queue: InfraiQueue, concurrency: usize, jobs_per_second: u32) -> Result<Vec<JobDecision>, WorkerError> {
    let messages = queue.consume(concurrency as u16, 45).await?;
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut cadence = interval(Duration::from_secs_f64(1.0 / jobs_per_second.max(1) as f64));
    let mut tasks = JoinSet::new();

    for message in messages {
        cadence.tick().await;
        let permit = permits.clone().acquire_owned().await.expect("semaphore stays open");
        let client = queue.clone();
        tasks.spawn(async move {
            let _permit = permit;
            handle_message(client, message).await
        });
    }

    let mut decisions = Vec::new();
    while let Some(result) = tasks.join_next().await {
        decisions.push(result??);
    }
    Ok(decisions)
}

async fn handle_message(queue: InfraiQueue, message: QueueMessage) -> Result<JobDecision, WorkerError> {
    let job: GameJob = serde_json::from_value::<GameJob>(message.payload).map_err(|source| {
        WorkerError::InvalidJob { message_id: message.message_id.clone(), source }
    })?;
    let decision = decide(job);
    queue.ack(&message.message_id).await?;
    Ok(decision)
}

pub fn example_asset_job() -> Value {
    serde_json::json!({"kind": "player_asset", "asset_id": "skin-482", "bytes": 1048576})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_player_asset_is_dropped_before_generation() {
        let job = GameJob::PlayerAsset { asset_id: "upload-9".into(), bytes: 26 * 1024 * 1024 };
        assert_eq!(
            decide(job),
            JobDecision::DropOversizedAsset { asset_id: "upload-9".into() }
        );
    }
}

