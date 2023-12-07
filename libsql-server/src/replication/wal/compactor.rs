#![allow(dead_code)]

use std::num::NonZeroU32;

use libsql_replication::frame::{FrameHeader, FrameBorrowed};
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::{CheckpointCallback, Wal, BusyHandler, Sqlite3WalManager};
use libsql_sys::wal::wrapper::{WrapWal, WalWrapper};
use zerocopy::FromBytes;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::replication::snapshot_store::{SnapshotStore, SnapshotBuilder};

type CompactorWal = WalWrapper<CompactorWrapper, Sqlite3WalManager>;

#[derive(Clone)]
struct CompactorWrapper {
    store: SnapshotStore,
    name: NamespaceName,
}

impl CompactorWrapper {
    pub fn new(store: SnapshotStore, name: NamespaceName) -> Self { Self { store, name } }
}

impl<T: Wal> WrapWal<T> for CompactorWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
    ) -> libsql_sys::wal::Result<(u32, u32)> {
        struct CompactorCallback<'a> {
            inner: Option<&'a mut dyn CheckpointCallback>,
            builder: Option<SnapshotBuilder>,
            base_frame_no: Option<FrameNo>,
        }

        impl<'a> CheckpointCallback for CompactorCallback<'a> {
            fn frame(
                &mut self,
                page: &[u8],
                page_no: NonZeroU32,
                frame_no: NonZeroU32,
            ) -> libsql_sys::wal::Result<()> {
                dbg!(frame_no, page_no);
                // We retrive the base_replication_index. The first time this method is being
                // called, it must be with page 1, patched with the current replication index,
                // because we just injected it.
                let base_frame_no = match self.base_frame_no {
                    None => {
                        assert_eq!(page_no.get(), 1);
                        // first frame must be newly injected frame , with the final frame_index
                        let header = Sqlite3DbHeader::read_from_prefix(page).unwrap();
                        let base_frame_no = dbg!(header.replication_index.get()) - frame_no.get() as u64;
                        self.base_frame_no = Some(base_frame_no);
                        dbg!(base_frame_no)
                    }
                    Some(frame_no) => frame_no,

                };
                let absolute_frame_no = base_frame_no + frame_no.get() as u64;
                dbg!(absolute_frame_no);
                let frame = FrameBorrowed::from_parts(
                    &FrameHeader {
                        checksum: 0.into(), // TODO!: handle checksum
                        frame_no: absolute_frame_no.into(),
                        page_no: page_no.get().into(),
                        size_after: 0.into(),
                    },
                    page,
                );

                self.builder.as_mut().unwrap().add_frame(&frame).unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.frame(page, page_no, frame_no);
                }

                Ok(())
            }

            fn finish(&mut self) -> libsql_sys::wal::Result<()> {
                self.builder.take().unwrap().finish(self.base_frame_no.unwrap() + 1).unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.finish();
                }

                Ok(())
            }
        }

        wrapped.begin_read_txn()?;
        let db_size = wrapped.db_size();
        wrapped.end_read_txn();

        let builder = self.store.builder(self.name.clone(), db_size).unwrap();
        let mut cb = CompactorCallback {
            inner: checkpoint_cb,
            builder: Some(builder),
            base_frame_no: None,
        };

        wrapped.checkpoint(db, mode, busy_handler, sync_flags, buf, Some(&mut cb))
    }
}

#[cfg(test)]
mod test {
    use libsql_sys::wal::Sqlite3WalManager;
    use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_TRUNCATE};
    use tempfile::tempdir;
    use libsql_sys::wal::WalManager;

    use crate::connection::libsql::open_conn_enable_checkpoint;
    use crate::replication::wal::replication_index_injector::ReplicationIndexInjectorWrapper;

    use super::*;

    #[tokio::test]
    async fn compact_wal_simple() {
        let tmp = tempdir().unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(CompactorWrapper::new(store.clone(), name.clone()))
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();
        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_TRUNCATE,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }

        dbg!();
        assert!(store.find(&name, 1).unwrap().is_some());
    }
}
