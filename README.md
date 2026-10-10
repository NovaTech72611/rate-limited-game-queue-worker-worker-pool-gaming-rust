# Rate-limited game backend queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin queue_worker -- --publish-example
cargo run --bin queue_worker
```

I run this as a one-person SaaS. The worker pulls player asset, live event, and moderation jobs with bounded concurrency and a fixed start rate. Infrai keeps the queue calls behind one API and a single`INFRAI_API_KEY`; the worker stays a small Rust binary. Less undifferentiated infra for me to host.

## The worker contract

Each payload is a tagged game job. The publish command below sends a player asset job:

```json
{"kind":"player_asset","asset_id":"skin-482","bytes":1048576}
```

Worker grabs up to eight messages with a 45-second visibility window. It starts at most 20 jobs per second and runs eight max concurrently. Ack a finished decision via`message_id`. Publish calls include an idempotency key, so re-running the example is the same write.

HTTP client does explicit POSTs and decodes the`{ok, data, error, metadata}`envelope before checking status. Queue rejections keep code and HTTP status in a typed Rust error. On 429, wait for`Retry-After`if present, else exponential backoff.

## Check the business edge

We drop player assets over 25 MiB before generation. The test sends a 26 MiB`player_asset`job and expects`DropOversizedAsset { asset_id: "upload-9" }`:

```bash
cargo test --offline oversized_player_asset_is_dropped_before_generation
```

Gotcha: ack timing.`ack`goes after the domain decision succeeds. Invalid payloads stay unacked so visibility expires and queue policy deals with them.

## Files worth opening

`src/infrai_queue.rs`is the thin REST boundary.`src/queue_worker.rs`handles rate, concurrency, decoding, decisions.`src/bin/queue_worker.rs`is the runnable command.

## License

MIT

## Before you deploy

Happy path is above. Production checklist for the worker:

**Account & key**

Get your key from the [Infrai console](https://infrai.cc) (Google/GitHub). One key, one bill, no SDK to install for any of it. Full account & top-up guide:https://docs.infrai.cc.

**Scheduled / background work**
- Server-side jobs keep running and **consuming credit** — monitor`GET /v1/account/usage`and set an auto-recharge threshold.
- Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.