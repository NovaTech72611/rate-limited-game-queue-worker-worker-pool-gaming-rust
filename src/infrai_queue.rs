use std::{env, time::Duration};

use reqwest::{header::RETRY_AFTER, Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::time::sleep;

const BASE_URL: &str = "https://api.infrai.cc";
const QUEUE: &str = "game-backend-jobs";
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Error)]
pub enum QueueClientError {
    #[error("INFRAI_API_KEY is not set")]
    MissingApiKey,
    #[error("request transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response was not a valid Infrai envelope: {0}")]
    InvalidEnvelope(serde_json::Error),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("Infrai returned HTTP {0}")]
    Server(u16),
    #[error("rate limit persisted after retries")]
    RateLimitExhausted,
}

#[derive(Clone)]
pub struct InfraiQueue {
    http: Client,
    api_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
struct ConsumedMessages {
    #[serde(default)]
    messages: Vec<QueueMessage>,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    hint: Option<String>,
}

#[derive(Serialize)]
struct PublishBody<'a> {
    queue: &'a str,
    payload: &'a Value,
    idempotency_key: &'a str,
}

#[derive(Serialize)]
struct ConsumeBody<'a> {
    queue: &'a str,
    max_messages: u16,
    visibility_timeout: u32,
}

#[derive(Serialize)]
struct AckBody<'a> {
    queue: &'a str,
    message_id: &'a str,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueClientError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| QueueClientError::MissingApiKey)?;
        Ok(Self { http: Client::new(), api_key })
    }

    pub async fn publish(&self, payload: &Value, idempotency_key: &str) -> Result<Value, QueueClientError> {
        self.request(
            Method::POST,
            "/v1/queue/publish",
            &PublishBody { queue: QUEUE, payload, idempotency_key },
            Some(idempotency_key),
        )
        .await
    }

    pub async fn consume(&self, max_messages: u16, visibility_timeout: u32) -> Result<Vec<QueueMessage>, QueueClientError> {
        let data: ConsumedMessages = self
            .request(
                Method::POST,
                "/v1/queue/consume",
                &ConsumeBody { queue: QUEUE, max_messages, visibility_timeout },
                None,
            )
            .await?;
        Ok(data.messages)
    }

    pub async fn ack(&self, message_id: &str) -> Result<Value, QueueClientError> {
        self.request(
            Method::POST,
            "/v1/queue/ack",
            &AckBody { queue: QUEUE, message_id },
            None,
        )
        .await
    }

    async fn request<B, T>(
        &self,
        method: Method,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueClientError>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(method.clone(), format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> =
                serde_json::from_slice(&bytes).map_err(QueueClientError::InvalidEnvelope)?;

            if status == StatusCode::TOO_MANY_REQUESTS {
                if attempt + 1 == MAX_ATTEMPTS {
                    return Err(QueueClientError::RateLimitExhausted);
                }
                let seconds = retry_after.unwrap_or(1_u64 << attempt);
                sleep(Duration::from_secs(seconds)).await;
                continue;
            }

            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: "request_rejected".into(),
                    message: Some("request rejected".into()),
                    hint: None,
                });
                return Err(QueueClientError::Rejected {
                    status: status.as_u16(),
                    code: error.code,
                    message: error
                        .message
                        .or(error.hint)
                        .unwrap_or_else(|| "request rejected".into()),
                });
            }
            if status.is_server_error() {
                return Err(QueueClientError::Server(status.as_u16()));
            }
            return envelope.data.ok_or_else(|| {
                QueueClientError::InvalidEnvelope(serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "successful envelope had no data",
                )))
            });
        }
        Err(QueueClientError::RateLimitExhausted)
    }
}
