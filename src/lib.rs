//! The major components are:
//!
//! * `server`: Redis server implementation. Includes a single `run` function
//!   that takes a `TcpListener` and starts accepting redis client connections.
//!
//! * `clients/client`: an asynchronous Redis client implementation. Demonstrates how to
//!   build clients with Tokio.
//!
//! * `cmd`: implementations of the supported Redis commands.
//!
//! * `frame`: represents a single Redis protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

/// 默认端口号
pub const DEFAULT_PORT: u16 = 6379;

/// 大多数函数返回的错误类型
///
/// 应用程序可能需要考虑专门的错误处理或者将错误类型定义为错误原因的枚举
/// 我们的例子使用一个Box封装的std::error::Error即可
/// 出于性能，要避免在任何hot path中boxing，例如在parse中定义了一个自定义的错误enum，这是因为socket接受部分帧时，错误在正常执行期间被击中和处理。
///  `std::error::Error` is implemented for `parse::Error` which allows it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 一个专门的Result类型，为了方便。
pub type Result<T> = std::result::Result<T, Error>;
