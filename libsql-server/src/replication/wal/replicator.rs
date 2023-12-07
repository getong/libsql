#![allow(dead_code)]

use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;
use std::sync::Arc;

use futures_core::Stream;
use libsql_replication::frame::{Frame, FrameBorrowed, FrameMut};
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal, WrappedWal};
use libsql_sys::wal::{
    CheckpointCallback, CheckpointMode, Sqlite3Db, Sqlite3Wal, Sqlite3WalManager, Wal, BusyHandler,
};
use parking_lot::Mutex;
use rusqlite::ffi::{
    sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL,
};
use zerocopy::{FromZeroes, AsBytes};

use crate::connection::libsql::open_conn_enable_checkpoint;
use crate::replication::FrameNo;

type Result<T, E = Error> = std::result::Result<T, E>;

const SQLD_LOG_OK: i32 = 200;
const SQLD_NEED_SNAPSHOT: i32 = 201;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("need snapshot")]
    NeedSnaphot,
    #[error("io error")]
    Io(#[from] std::io::Error),
}

type ReplicatorWal = WrappedWal<ReplicatorWrapper, Sqlite3Wal>;

#[derive(Clone)]
struct ReplicatorWrapper {
    out_sink: tokio::sync::mpsc::Sender<Frame>,
    next_frame_no: NonZeroU64,
}

impl<T: Wal> WrapWal<T> for ReplicatorWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        _db: &mut Sqlite3Db,
        _mode: CheckpointMode,
        _busy_handler: Option<&mut dyn BusyHandler>,
        _sync_flags: u32,
        _buf: &mut [u8],
        _checkpoint_cb: Option<&mut dyn CheckpointCallback>,
    ) -> libsql_sys::wal::Result<(u32, u32)> {
        let base_replication_index = get_base_frame_no(wrapped)?;
        if self.next_frame_no.get() <= base_replication_index {
            return Err(rusqlite::ffi::Error::new(SQLD_NEED_SNAPSHOT));
        } else {
            let Some(last_frame_in_wal) = wrapped.last_fame_index() else {
                return Err(rusqlite::ffi::Error::new(SQLD_LOG_OK));
            };
            // this is the offset of the frame in the wal
            wrapped.begin_read_txn().unwrap();
            let size_after = wrapped.db_size();
            let start_frame_no = (self.next_frame_no.get() - base_replication_index as u64) as u32;
            for i in start_frame_no..=last_frame_in_wal.get() {
                let mut frame = FrameBorrowed::new_box_zeroed();
                match wrapped.read_frame(NonZeroU32::new(i).unwrap(), frame.page_mut()) {
                    Ok(()) => {
                        frame.header_mut().frame_no = (base_replication_index + i as u64).into();
                        if i == last_frame_in_wal.get() {
                            frame.header_mut().size_after = size_after.into();
                        }
                        let frame = FrameMut::from(frame);
                        self.out_sink.blocking_send(frame.into()).unwrap();
                    }
                    Err(_) => todo!(),
                }
            }
            wrapped.end_read_txn();
        }

        Err(rusqlite::ffi::Error::new(SQLD_LOG_OK))
    }
}

fn get_base_frame_no<T: Wal>(wal: &mut T) -> libsql_sys::wal::Result<FrameNo> {
    let mut header = Sqlite3DbHeader::new_zeroed();
    wal.db_file().read_at(header.as_bytes_mut(), 0)?;
    Ok(header.replication_index.get())
}

pub struct Replicator {
    conn: Arc<Mutex<libsql_sys::Connection<ReplicatorWal>>>,
    receiver: tokio::sync::mpsc::Receiver<Frame>,
}

impl Replicator {
    pub fn new(db_path: &Path, next_frame_no: FrameNo) -> crate::Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let wal_manager = WalWrapper::new(
            ReplicatorWrapper {
                next_frame_no: NonZeroU64::new(next_frame_no)
                    .unwrap_or(NonZeroU64::new(1).unwrap()),
                out_sink: sender,
            },
            Sqlite3WalManager::default(),
        );

        let conn = open_conn_enable_checkpoint(db_path, wal_manager, None, u32::MAX)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            receiver,
        })
    }

    fn stream_frames(&mut self) -> impl Stream<Item = Result<Frame>> + '_ {
        let conn = self.conn.clone();
        let mut handle = tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            // force wal openning
            unsafe {
                let rc = sqlite3_wal_checkpoint_v2(
                    conn.handle(),
                    std::ptr::null_mut(),
                    SQLITE_CHECKPOINT_FULL,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
                match rc {
                    SQLD_NEED_SNAPSHOT => Err(Error::NeedSnaphot),
                    SQLD_LOG_OK => Ok(()),
                    _ => panic!(),
                }
            }
        });

        async_stream::stream! {
            loop {
                tokio::select! {
                    ret = &mut handle => {
                        if let Err(e) = ret.unwrap() {
                            yield Err(e);
                        }
                        // drain remaining frames
                        while let Ok(frame) = self.receiver.try_recv() {
                            yield Ok(frame);
                        }
                        return
                    }
                    Some(frame) = self.receiver.recv() => {
                        yield Ok(frame)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rusqlite::ffi::SQLITE_CHECKPOINT_TRUNCATE;
    use tokio_stream::StreamExt;

    use crate::{connection::libsql::open_conn, replication::wal::replication_index_injector::{ReplicationIndexInjectorWal, ReplicationIndexInjectorWrapper}};

    use super::*;


    #[tokio::test]
    async fn basic_stream_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let conn = open_conn(tmp.path(), Sqlite3WalManager::default(), None).unwrap();

        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        let mut replicator = Replicator::new(tmp.path(), 0).unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            1
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            2
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            3
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn stream_frame_after_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_manager = ReplicationIndexInjectorWal::new(ReplicationIndexInjectorWrapper, Sqlite3WalManager::default());
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, u32::MAX).unwrap();
        conn.busy_timeout(std::time::Duration::from_millis(100))
            .unwrap();

        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        let mut replicator = Replicator::new(tmp.path(), 0).unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            1
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            2
        );
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            3
        );
        assert!(stream.next().await.is_none());

        unsafe {
            let mut in_log = 0;
            let mut backfilled = 0;
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_TRUNCATE,
                &mut in_log,
                &mut backfilled,
            );
            assert_eq!(rc, 0);
            // all frames were backfilled
            assert_eq!(in_log, 0);
            assert_eq!(backfilled, 0);
        }

        conn.execute("insert into test values (123)", ()).unwrap();

        let mut replicator = Replicator::new(tmp.path(), 0).unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(Error::NeedSnaphot)
        ));

        // frame 4 is the injected frame_no frame, it's part of the snapshot
        let mut replicator = Replicator::new(tmp.path(), 5).unwrap();
        let stream = replicator.stream_frames();
        tokio::pin!(stream);
        assert_eq!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap()
                .header()
                .frame_no
                .get(),
            5
        );
        assert!(stream.next().await.is_none());
    }
}
