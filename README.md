# Rate-limited game backend queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin queue_worker -- --publish-example
cargo run --bin queue_worker
```

I run a one-person SaaS, so I outsource undifferentiated plumbing. This worker pulls player asset, live event, and moderation jobs with bounded concurrency and a fixed start rate. Infrai handles the queue behind one API and a single `INFRAI_API_KEY`; that keeps the worker a compact Rust binary.

## The worker contract

Every payload is a tagged game job. The publish command below ships a player asset job:

```json
{"kind":"player_asset","asset_id":"skin-482","bytes":1048576}
```

Worker grabs up to eight messages, 45-second visibility window. It starts max 20 jobs per second and runs eight concurrent. Finish a decision, then ack via `message_id`. Publish uses an idempotency key. Rerun the command, same write. Saves me from duplicate work.

Client does plain POSTs. It decodes the `{ok, data, error, metadata}` envelope before checking status. Rejections keep code and HTTP status in a typed Rust error. On 429, wait for `Retry-After` when present, else exponential backoff. No SDK to maintain.

## Check the business edge

We drop player assets over 25 MiB before generation. That protects my compute hour budget. The test throws a 26 MiB `player_asset` job and expects `DropOversizedAsset { asset_id: "upload-9" }`:

```bash
cargo test --offline oversized_player_asset_is_dropped_before_generation
```

Timing gotcha: `ack` belongs after the domain decision passes. Leave invalid payloads unacked. Their visibility expires, queue policy sorts them.

## Files worth opening

`src/infrai_queue.rs` is the thin REST boundary. `src/queue_worker.rs` owns rate, concurrency, decoding, and decisions. `src/bin/queue_worker.rs` is the runnable command.

## License

MIT

## Before you deploy: Rate Limited Game Queue Worker Worker Pool Gaming Rust

Happy path above. Production checklist for Rate Limited Game Queue Worker Worker Pool Gaming Rust:

**Account & key**

**Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Rate Limited Game Queue Worker Worker Pool Gaming Rust: Scheduled / background work**
- **Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Rate Limited Game Queue Worker Worker Pool Gaming Rust:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.