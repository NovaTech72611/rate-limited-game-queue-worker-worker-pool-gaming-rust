use game_backend_queue_worker::{
    infrai_queue::InfraiQueue,
    queue_worker::{example_asset_job, run_batch},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = InfraiQueue::from_env()?;
    if std::env::args().any(|arg| arg == "--publish-example") {
        let published = queue.publish(&example_asset_job(), "player-asset-skin-482").await?;
        println!("published: {published}");
        return Ok(());
    }

    for decision in run_batch(queue, 8, 20).await? {
        println!("completed: {decision:?}");
    }
    Ok(())
}

