use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Db实例的Wrapper，存在的意义是有序清理Db实例，当这个结构体被drop时，向后台清理任务发送关闭信号
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// DbHolder结构体被drop时，Db实例将被停止
    db: Db,
}

/// 跨所有连接共享的服务端状态
///
/// Db包含一个HashMap存储k-v数据和所有的broadcast::Sender值作为在线的发布订阅管道
///
/// Db实例是一个共享状态的句柄，克隆是浅的，并且指挥导致原子red数增加
/// 创建Db值时，将生成后台任务，用于在请求的持续时间过期后将值过期。
/// 该任务将一直运行直到所有的Db实例都被删除。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 共享状态的句柄，后台任务也将会有一个Arc<Shared>
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由mutex互斥锁保护，是std::sync::Mutex的而非tokio的互斥锁。
    /// 这是因为当拥有互斥锁时没有异步操作正在被执行，此外，临界区非常小。
    /// Tokio的互斥锁大多期望在锁被跨.await字段点持有的时使用，其他的所有情况最好使用std锁。
    /// 如果这个临界区没有包含任何异步操作，但是时间很长，则包括等待互斥锁的整个操作被认为阻塞操作，tokio::task::spawn_blocking应该使用。
    state: Mutex<State>,

    /// 通知后台任务处理的entry过期。后台任务等待这个通知然后检查过期值或者shutdown信号
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 键值数据
    entries: HashMap<String, Entry>,

    /// 发布订阅键空间
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 追踪键的TTL
    ///
    /// BTreeMap用于维护按到期时间排序的expirations，允许后台任务迭代此映射来查找下一个即将过期的值
    /// 可以为同一时刻创建多个expiration，虽然这不太可能，因此Instant对于密钥来说是不够的，使用唯一的过期标识符next_id来打破这些联系
    expirations: BTreeMap<(Instant, u64), String>,

    /// 标识符用于下一次过期，每个过期都与唯一标识符相关联。
    next_id: u64,

    /// 当Db实例关闭时为True。所有的Db值被drop，值设置为True表示后台任务退出。
    shutdown: bool,
}

/// 键值数据中的条目Entry
#[derive(Debug)]
struct Entry {
    /// 唯一标识这个条目
    id: u64,

    /// 存储数据
    data: Bytes,

    /// 条目过期并且应该从数据库中移除的Instant
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// 创建一个DbHolder，封装Db实例，当被drop时，Db的清洗任务将被停止
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享数据库，内部是一个Arc，所以克隆指挥增加refcount
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 通知Db实例关闭清洗过期键的任务
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建一个新的空的Db实例，分配共享状态和生成后台任务来管理过期的key
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 设置后台任务
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 根据键获取值
    /// 如果没有key关联的值将返回None，比如键未分配值和值过期
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取锁，得到entry并克隆值
        // 由于数据是Bytes，这里的克隆只是浅克隆
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置key关联的值以及可选的过期时间
    /// 如果一个值已经关联了该key，将被移除
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // 获取并增加下一个插入的ID，在锁的保护下，确保每个set操作关联唯一标识符
        let id = state.next_id;
        state.next_id += 1;

        // 如果set成为下一个过期的键，后台任务需要被通知，能够更新它的状态
        // 任务是否需要通知是在set路线中计算的
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // key过期的Instant
            let when = Instant::now() + duration;

            // 只有当新插入的expiration是下一个要退出的key时，才通知工作的任务。工作者需要唤醒来更新状态。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            // 跟踪过期时间
            state.expirations.insert((when, id), key.clone());
            when
        });

        // 将entry插入到HashMap
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        // 如果这里有一个先前key关联的值并且拥有过期时间，在expirations中关联的entry也必须被溢出，避免数据泄露
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // 清除过期时间
                state.expirations.remove(&(when, prev.id));
            }
        }

        // 通知后台任务之前释放互斥锁，避免后台任务唤醒后由于该函数仍然持有互斥锁而无法获取互斥锁，减少竞争
        drop(state);

        if notify {
            // 只有后台任务需要更新状态来反映新的expiration时才通知这后台任务。
            self.shared.background_task.notify_one();
        }
    }

    /// 返回请求通道的Receiver，用于接收由PUBLISH命令广播的值
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取互斥锁
        let mut state = self.shared.state.lock().unwrap();

        // 如果所请求的通道没有entry，则创建一个新的广播通道并且与key关联。如果已经存在，返回关联的Receiver
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // 没有广播通道存在，所以创建一个
                //
                // 该通道的容量为1024条消息，一条消息被存储直到所有的订阅者看到。意味着缓慢的订阅者可能导致消息被无限保留
                // 通道被填满时，发布将导致旧消息被丢弃，防止缓慢订阅者阻塞整个系统
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 发布消息到通道，返回在通道上监听的订阅者的数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// 清除后台任务关闭的信号。由DbShutdown的Drop实现调用
    fn shutdown_purge_task(&self) {
        // 后台任务必须被告知关闭。设置State::shutdown为True并且通知任务来完成。
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 给后台任务发信号之前先Drop锁，确保后台任务不会因为无法获取互斥锁而唤醒，减少竞争
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}
