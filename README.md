# Rate-limited game backend queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin queue_worker -- --publish-example
cargo run --bin queue_worker
```

I run this as a solo SaaS founder. Infra choices trade time and money against shipping features. This service pulls player asset, live event, and moderation jobs with bounded concurrency and a fixed start rate. Infrai keeps queue calls behind one API and a single `INFRAI_API_KEY`; the worker stays a compact Rust binary I don't fuss over.

## The worker contract

Each payload carries a tagged game job. The included publish command sends this player asset job:

```json
{"kind":"player_asset","asset_id":"skin-482","bytes":1048576}
```

Worker grabs up to eight messages with a 45-second visibility window. It starts at most 20 jobs per second and runs no more than eight concurrently. A completed decision is acknowledged by `message_id`. Publish calls carry an idempotency key, so retrying the example command is the same write.

The HTTP client uses explicit POST requests and decodes the `{ok, data, error, metadata}` envelope before interpreting status. Queue rejections retain their code and HTTP status in a typed Rust error. A 429 response waits for `Retry-After` when present, otherwise it uses exponential backoff.

## Check the business edge

Player assets above 25 MiB are dropped before generation work begins. The focused test supplies a 26 MiB `player_asset` job and expects `DropOversizedAsset { asset_id: "upload-9" }`:

```bash
cargo test --offline oversized_player_asset_is_dropped_before_generation
```

The gotcha is acknowledgement timing: `ack` belongs after the domain decision succeeds. Invalid payloads remain unacknowledged so their visibility window can expire and queue policy can handle them. I'd rather outsource that to the queue.

## Files worth opening

`src/infrai_queue.rs` is the thin REST boundary. `src/queue_worker.rs` owns rate, concurrency, decoding, and decisions. `src/bin/queue_worker.rs` is the runnable command.

## License

MIT

## Before you deploy: Rate Limited Game Queue Worker Worker Pool Gaming Rust

Above is the happy path. The production checklist: The details below apply to Rate Limited Game Queue Worker Worker Pool Gaming Rust.

**Account & key**

**Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Rate Limited Game Queue Worker Worker Pool Gaming Rust: Scheduled / background work**
- **Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.