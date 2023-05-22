use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 从远方的对等端发送和接受帧
/// 
/// 当实现网络协议时，该协议上的消息通常由称为帧的几个较小的消息组成
/// Connection的目的是读写底层TcpStream的帧，为了读取帧，Connection使用一个内部缓冲区，直到有足够的字节来创建一个完整的帧，然后创建帧返回给调用者
/// 然后将缓冲区的内容写入socket
#[derive(Debug)]
pub struct Connection {
    // 被BufWriter修饰的TcpStream，提供写级别的缓冲，被Tokio实现的BufWriter足够我们的需要
    stream: BufWriter<TcpStream>,

    // 读帧的缓冲区
    buffer: BytesMut,
}

impl Connection {
    /// 创建新的Connect，由socket支持。初始化读写缓冲区。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 默认4KB的读缓冲区，对我们来说够了。 更大的读缓冲区可能更好。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从底层的流中读取单帧
    ///
    /// 这个方法将会等待，直到接收到足够的数据来解析一帧。帧被解析之后，读缓冲区的任何数据一直被保留，直到下一次调用read_frame()。
    /// 成功，收到的帧被返回。如果TcpStream以某种不会将帧分成两半的方式关闭，返回None。否则，返回错误
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓冲数据中解析一个帧，如果足够的数据被缓冲，则返回帧
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 没有足够的缓冲数据组成一帧，尝试从socket里读取更多数据
            // 成功则返回字节数，0表示流结束
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远端关闭连接。为了实现干净的关闭，读缓冲区中应该没有数据。如果有的化，意味着对端在发送帧时关闭了socket
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// 尝试从缓冲区中解析帧 
    /// 如果缓冲区有了足够的数据，返回帧然后清除缓冲区的数据
    /// 如果没有足够的数据被缓冲，返回Ok(None)，如果缓冲数据不是一个合法的帧，返回Err
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor用于跟踪缓冲区的当前位置，Cursor还从bytes库中实现了Buf，bytes包提供了许多用于处理字节的有用工具
        let mut buf = Cursor::new(&self.buffer[..]);

        // 第一步检查是否有足够的数据组成单帧，通常比帧进行完整解析要快的多，允许我们跳过分配数据结构来保存帧，除非我们已经接收了完整的帧。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // check函数将光标移动到帧的末尾。
                // 调用check之前，游标的位置被设置为0，所以通过检查游标的位置来获取帧的长度。
                let len = buf.position() as usize;

                // 将Cursor传递给Frame::parse之前，将位置重置为0
                buf.set_position(0);

                // 解析缓冲区的帧。分配必要的结构表示帧。
                // 编码的帧无效则返回错误，终止当前的连接，但不影响任何其他的客户端连接。
                let frame = Frame::parse(&mut buf)?;

                // 从读缓冲区中丢弃解析的数据
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // 向调用者返回解析的帧
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.stream.write_u8(b'*').await?;

                // 编码数组的长度
                self.write_decimal(val.len() as u64).await?;

                // 迭代并编码数组中的每个entry
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 帧类型是字面量，直接对值编码
            _ => self.write_value(frame).await?,
        }

        // 确保将编码帧写入socket，上面的调用是对缓冲流的调用和写操作。调用flush将缓冲区剩余的内容写入套接字。
        self.stream.flush().await
    }

    /// 将帧内容写入流
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // Encoding an `Array` from within a value cannot be done using a
            // recursive strategy. In general, async fns do not support
            // recursion. Mini-redis has not needed to encode nested arrays yet,
            // so for now it is skipped.
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 将一个10进制帧写入流
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // 将值转化为字符串
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
