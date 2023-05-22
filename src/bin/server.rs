//! server端入口文件
//! 命令行参数解析 传递参数给mini::server
use mini_redis::{server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    // 设置日志
    set_up_logging()?;

    // 命令行解析
    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // 监听端口号
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    // 调用mini_redis::server运行
    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

// 命令行解析
#[derive(Parser, Debug)]
#[clap(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    #[clap(long)]
    port: Option<u16>,
}

// 使用tracing库记录日志
#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    tracing_subscriber::fmt::try_init()
}
