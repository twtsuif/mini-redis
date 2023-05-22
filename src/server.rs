//! 最小的Redis实现
//! 提供一个异步的run方法，监听客户端连接。为每一个连接生成一个任务

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// 服务端监听状态 
/// 在run方法中创建
/// 包括一个run方法 执行TCP监听和初始化每个连接的状态
#[derive(Debug)]
struct Listener {
    /// 共享的数据库句柄
    /// 
    /// 包括k-v存储以及发布订阅的广播通道
    /// 包含一个围绕Arc的wrapper 内部的Db结构体可以被检索和传递给每个连接状态
    db_holder: DbDropGuard,

    /// TCP监听器，外部run方法提供
    listener: TcpListener,

    /// 限制最大的连接数量
    ///
    /// 一个Semaphore被用于限制最大连接数，在尝试接受一个新连接之前，从semaphore中获得许可，如果没有空余，则等待。
    /// 当处理器完成时，许可被返回给semaphore
    limit_connections: Arc<Semaphore>,

    /// 广播终止信号给所有的连接
    ///
    /// 由外部的run方法提供 优雅地关闭所有连接
    /// 当一个连接任务生成时，被分配一个广播接收器句柄
    /// 幽雅关闭被初始化时，()值通过broadcast::sender发送，每一个在线连接收到时，达到一个安全的终止状态然后结束任务。
    notify_shutdown: broadcast::Sender<()>,

    /// 作为优雅关闭的一部分，等待客户端连接完成处理
    /// 
    /// 一旦所有的Sender超出范围，Tokio的管道将关闭，接收端收到None，探测所有的客户端句柄是否完成。
    /// 初始化连接句柄时，分配一个shutdown_complete_tx的克隆，当服务端监听器关闭，会丢弃由这个字段持有的发送者。
    /// 一旦所有处理程序完成，所有Sender克隆将被删除，导致recv()方法返回None
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 连接前处理器 从connection读取请求并应用命令给db
#[derive(Debug)]
struct Handler {
    /// 共享数据库句柄
    ///
    /// 当一个命令从连接中接受时，与db一起作用。命令的实现在cmd模块下，每一个连接命令都需要与db交互
    db: Db,

    /// Redis协议编解码器装饰的TCP连接 使用一个缓冲的TcpStream实现
    /// 
    /// 当监听器接收到入站连接时，TcpStream被传递给connect::new初始化相关缓冲区
    /// Connect允许处理器在帧级别操作，将字节级协议解析细节封装给Connection
    connection: Connection,

    /// 监听关闭的通知
    /// 
    /// 在监听器中与sender配对的broadcast::Receiver包装器，连接处理器处理连接请求，直到对等端断开连接或者收到关闭通知
    /// 后一种情况下，对等端处理的任何工作都将继续，直到安全状态后连接才终止。
    shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}


/// 硬编码
const MAX_CONNECTIONS: usize = 250;

/// 运行Redis服务端
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 当收到shutdown future信号时，我们必须向所有的在线连接发送信息。使用广播通道。
    // 下面的调用忽略了广播的接收方，当需要接收方时，subscribe()方法可以创建
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // 初始化监听器状态
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // 并发运行服务端并监听shutdown信号，服务端任务运行直到错误发生，所以在普通的情况下，select!语句直到收到shutdown信号才停止
    //
    // `select!` statements are written in the form of:
    //
    // ```
    // <result of async op> = <async op> => <step to perform with result>
    // ```
    //
    // All `<async op>` statements are executed concurrently. Once the **first**
    // op completes, its associated `<step to perform with result>` is
    // performed.
    tokio::select! {
        res = server.run() => {
            // 如果在这里收到错误，从TCP监听器中多次接受连接失败，服务器尝试放弃并且关闭。
            // 处理单个连接时遇到的错误不会到达这里。
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // 关闭信号被收到
            info!("shutting down");
        }
    }

    // 抽离出shutdown_complete的接收者和传播者，显式drop掉。
    // 不这样的话，下面的await将永远不会完成。
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // drop之后，所有的被订阅任务将会收到shutdown信号，开始退出
    drop(notify_shutdown);
    // drop最后的Sender，因此下面的Receiver可以完成
    drop(shutdown_complete_tx);

    // 等待所有在线连接完成处理，由于监听器持有的Sender句柄已经删除，仅剩下的Sender实例由连接处理程序持有。
    // 当它们drop时，mpsc管道将会关闭，recv()方法将返回None
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// 对于每个连接生成一个任务
    ///
    /// 如果accepting返回错误，这里将返回Err
    /// 可能有很多原因，这些原因会随着时间推移而恢复，比如操作系统达到内部socket的最大数量
    /// 进程无法检测到瞬时错误何时自行解决，处理这个问题的一个策略是后退策略，也就是我们在这里做的
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 等待一个permit变为可用
            // acquire_owned返回一个绑定到信号量的permit，当permit被drop时，自动返回给信号量
            // 信号量关闭的时候acquire_owned()返回Err，我们不会关闭信号量，所以unwrap()是安全的
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // 接受一个新的socket，将会尝试执行错误处理，accept方法内部尝试恢复错误，所以这里的错误是不可恢复
            let socket = self.accept().await?;

            // 创建处理器状态
            let mut handler = Handler {
                // 获得共享数据库句柄
                db: self.db_holder.db(),

                // 初始化连接状态，分配缓冲区来执行Redis协议的帧解析
                connection: Connection::new(socket),

                // 接受关闭信号的通知
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // 一旦所有的克隆被drop，通知接收端一半
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 生成一个新任务来处理连接，Tokio的任务像异步的绿色线程，可以并发执行
            tokio::spawn(async move {
                // 处理连接，遇到错误则log
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                // 将permit移动到任务中，并在完成后drop，这返回permit给信号量
                drop(permit);
            });
        }
    }

    /// 接受入站连接
    ///
    /// 错误通过后退和重试机制来处理，在这里，一个指数回退策略被使用。
    /// 第一次失败后等待1秒，第二次失败后等待2秒，接下来都是双倍时间，6次以上，函数返回错误
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // 尝试接受几次
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            // 暂停执行直到backoff结束
            time::sleep(Duration::from_secs(backoff)).await;

            // backoff加倍
            backoff *= 2;
        }
    }
}

impl Handler {
    /// 处理一个连接
    ///
    /// 从socket读取并处理请求帧，将响应写回socket。
    /// 目前没有实现流水线(没有交叉帧的情况下每个连接同时处理多个请求的能力)，详见https://redis.io/topics/pipelining
    /// 接收到关闭信号时，连接继续被处理直到到达安全状态才会被终止
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 只要没有接收到关闭信号，就尝试读入新请求帧
        while !self.shutdown.is_shutdown() {
            // 在读取请求帧时也要监听关闭信号
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // 如果收到关闭信号则返回，将导致任务终止
                    return Ok(());
                }
            };

            // 如果从read_frame()方法中返回None，则对端关闭了socket，任务可以终止
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Convert the redis frame into a command struct. This returns an error if the frame is not a valid redis command or it is an unsupported command.
            // 将帧转化为命令结构，如果是无效命令返回错误
            let cmd = Command::from_frame(frame)?;

            // 记录cmd目标的日志
            // The syntax here is a shorthand provided by the `tracing` crate. It can be thought of as similar to:
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` provides structured logging, so information is "logged" as key-value pairs.
            debug!(?cmd);

            // 使用命令执行的工作，这可能会导致数据库状态发生变化
            // 连接被传递到apply函数，该函数允许命令直接向连接写入响应帧，在发布订阅模式下，可以向对等端发送多个帧
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
