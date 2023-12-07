//! This module contains the ReplicationIndexInjectorWal. This WAL implementation ensures that the
//! page 1 of the db file always reflect the replication index at the moment it was written to the
//! wal or checkpointed to the main database file.
//!
//! On calls to `insert_frames`, it checks for the first page the the header list if it's page 1.
//! If it is, then it patches the frame index with the frame_no computed from it's relative
//! position to the last page 1 in the wal. The page1 that is passed to us contains the freshest
//! version of page 1 before we patch it. There are two options: either page 1 is already in the
//! wal (as reported by find_frame), in this case, we can increment the value of the replication
//! index by how many frames were written to the wal since the last page 1 was written, or, if it's
//! not present in the WAL (find_frame returned None), then this page came from the main database
//! file, so we increment its replication index by the frames absolute position in the wal.
//!
//! On checkpoint, we retrieve the latest version of page_1 (either from the WAL, or from the main
//! database file), and we patch it's replication index. We then re-inject the patched page 1 at
//! the tail of the WAL, and perform the checkpoint. We wrap the checkpoint callback with our own
//! that ensures that the first checkpointed page is the patched page 1, to ensure that no-one has
//! written to the db in between us injecting page 1 and checkpointing. This is because the last
//! frame to be injected *MUST* be a patched page 1 reflecting all the changes that happened
//! before. If we detect a change, we return an error and abort the checkpoint, which will be
//! attempted again later on.
//!
//! We keep patching page 1s on every call to frames, because a partial checkpoint may occur, where
//! a patched page 1 in injected, but not the remaining pages. By maintaining the rolling
//! replication index in page 1, we can select the page 1 that represent the actual replication
//! index, either from the WAL or from the main database file.
#![allow(dead_code)]

use std::mem::size_of;

use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::wrapper::{WalWrapper, WrapWal};
use libsql_sys::wal::{
    BusyHandler, CheckpointCallback, CheckpointMode, PageHeaders, Wal,
};
use rusqlite::ffi::{libsql_pghdr, SQLITE_SYNC_NORMAL, SQLITE_BUSY};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::LIBSQL_PAGE_SIZE;

pub type ReplicationIndexInjectorWal<W> = WalWrapper<ReplicationIndexInjectorWrapper, W>;

#[derive(Clone)]
pub struct ReplicationIndexInjectorWrapper;

impl<W: Wal> WrapWal<W> for ReplicationIndexInjectorWrapper {
    fn insert_frames(
        &mut self,
        wrapped: &mut W,
        page_size: std::ffi::c_int,
        page_headers: &mut libsql_sys::wal::PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: std::ffi::c_int,
    ) -> libsql_sys::wal::Result<()> {
        unsafe {
            // Sqlite passes us the pages in ascending frame_no number, if there is any page 1,
            // then it's the first one.
            if let Some((1, page)) = page_headers.iter_mut().next() {
                let offset = match wrapped.find_frame(1.try_into().unwrap())? {
                    Some(last_page_1_offset) => {
                        // page1 was already written to the log before, update the new version
                        // with the last offset
                        let current_offset =
                            wrapped.last_fame_index().map(|x| x.get()).unwrap_or(0);
                        current_offset - last_page_1_offset.get()
                    }
                    None => wrapped.last_fame_index().map(|x| x.get()).unwrap_or(0),
                };
                let mut header = Sqlite3DbHeader::read_from_prefix(page).unwrap();
                header.replication_index += (offset as u64 + 1).try_into().unwrap();
                // we need to copy the patched header back to the page, because the page doesn't necessarily follow the
                // alignment requirement.
                page[..size_of::<Sqlite3DbHeader>()].copy_from_slice(header.as_bytes());
            }
        }

        let ret = wrapped.insert_frames(page_size, page_headers, size_after, is_commit, sync_flags);
        ret
    }

    fn checkpoint(
        &mut self,
        wrapped: &mut W,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
    ) -> libsql_sys::wal::Result<(u32, u32)> {
        let checkpointed = wrapped.count_checkpointed();
        let frames_in_wal = wrapped.last_fame_index().unwrap_or(0).get();
    
        if checkpointed == frames_in_wal {
            // we already checkpointed all the frames in the wal, there is nothing to do.
            return Ok((checkpointed, frames_in_wal))
        }
        // This callback ensure that the first with which the checkpoint is performed by the
        // wrapped wal is the page 1 we just injected.
        struct EnsurePage1IsFirstCb<'a> {
            inner: Option<&'a mut dyn CheckpointCallback>,
            inject_offset: u32,
            biggest_seen_offset: u32,
        }

        impl CheckpointCallback for EnsurePage1IsFirstCb<'_> {
            fn frame(&mut self, _frame: &[u8], _page_no: std::num::NonZeroU32, frame_no: std::num::NonZeroU32) -> libsql_sys::wal::Result<()> {
                dbg!();
                if dbg!(frame_no.get()) > dbg!(self.inject_offset) {
                    dbg!();
                    return Err(rusqlite::ffi::Error::new(SQLITE_BUSY));
                }
                self.biggest_seen_offset = self.biggest_seen_offset.max(frame_no.get());
                Ok(())
            }

            fn finish(&mut self) -> libsql_sys::wal::Result<()> {
                if self.biggest_seen_offset != self.inject_offset {
                    Err(rusqlite::ffi::Error::new(SQLITE_BUSY))
                } else {
                    Ok(())
                }
            }
        }

        let ret = inject_replication_index(wrapped);
        dbg!();
        wrapped.end_read_txn();
        let inject_offset = ret?;

        dbg!();
        dbg!(wrapped.checkpoint(
                    db,
                    mode,
                    busy_handler,
                    sync_flags,
                    buf,
                    Some(&mut EnsurePage1IsFirstCb {
                        inner: checkpoint_cb,
                        inject_offset,
                        biggest_seen_offset: 0,
                    }),
                ))
    }

    fn close<M: libsql_sys::wal::WalManager<Wal = W>>(
        &mut self,
        manager: &M,
        wrapped: &mut W,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: std::ffi::c_int,
        scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        self.checkpoint(
            wrapped,
            db,
            CheckpointMode::Truncate,
            None,
            SQLITE_SYNC_NORMAL as _,
            scratch.unwrap(),
            None,
        )?;
        manager.close(wrapped, db, sync_flags, None)
    }
}

#[repr(C)]
#[derive(FromZeroes, FromBytes, AsBytes)]
struct Page1 {
    header: Sqlite3DbHeader,
    data: [u8; LIBSQL_PAGE_SIZE as usize - size_of::<Sqlite3DbHeader>()],
}

pub(super) fn inject_replication_index<W: Wal>(wal: &mut W) -> libsql_sys::wal::Result<u32> {
    wal.begin_read_txn()?;
    wal.begin_write_txn()?;

    // The reason we have this Page1 struct is to make sure we get a properly aligned buffer and
    // patch the header
    let mut page1 = Page1::new_zeroed();
    let last_page1_frame_no = match wal.find_frame(1.try_into().unwrap())? {
        Some(fno) => {
            wal.read_frame(fno, page1.as_bytes_mut())?;
            fno.get()
        }
        None => {
            wal.db_file().read_at(page1.as_bytes_mut(), 0)?;
            0
        }
    };

    let last_frame = wal.last_fame_index().map_or(0, |x| x.get());
    let frames_since_last_checkpoint = last_frame - wal.count_checkpointed();
    dbg!(frames_since_last_checkpoint);
    dbg!(page1.header.replication_index.get());
    page1.header.replication_index += ((frames_since_last_checkpoint - last_page1_frame_no) as u64 + 1).into();

    let mut header = libsql_pghdr {
        pPage: std::ptr::null_mut(),
        pData: page1.as_bytes_mut().as_mut_ptr() as _,
        pExtra: std::ptr::null_mut(),
        pCache: std::ptr::null_mut(),
        pDirty: std::ptr::null_mut(),
        pPager: std::ptr::null_mut(),
        pgno: 1,
        pageHash: 0x02, // DIRTY
        flags: 0,
        nRef: 0,
        pDirtyNext: std::ptr::null_mut(),
        pDirtyPrev: std::ptr::null_mut(),
    };

    let mut headers = unsafe { PageHeaders::from_raw(&mut header) };

    let db_size = wal.db_size();

    wal.insert_frames(
        LIBSQL_PAGE_SIZE as _,
        &mut headers,
        db_size, // the database doesn't change; there's always a page 1.
        true,
        SQLITE_SYNC_NORMAL, // we'll checkpoint right after, no need for full sync
    )?;

    Ok(wal.last_fame_index().unwrap().get())
}

#[cfg(test)]
mod test {
    use std::io::Read;
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use std::{os::unix::prelude::FileExt, sync::Arc};

    use aws_sdk_s3::operation::get_bucket_replication;
    use itertools::Itertools;
    use libsql_sys::wal::{Sqlite3WalManager, WalManager};
    use rusqlite::ffi::{sqlite3_wal_checkpoint_v2, SQLITE_CHECKPOINT_FULL, SQLITE_IOERR_WRITE, SQLITE_CHECKPOINT_RESTART, wal_manager_impl, sqlite3};
    use tempfile::tempdir;
    use tokio::sync::Notify;

    use crate::connection::libsql::{open_conn, open_conn_enable_checkpoint};
    use crate::replication::FrameNo;

    use super::*;

    fn checkpoint<W>(conn: &libsql_sys::Connection<W>) -> (i32, i32, i32) {
        let mut backfilled = 0;
        let mut in_wal = 0;
        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_RESTART,
                &mut backfilled,
                &mut in_wal,
            );

            (rc, backfilled, in_wal)
        }
    }

    fn read_replication_index(path: &Path) -> FrameNo {
        let mut f = std::fs::File::open(path.join("data")).unwrap();
        let mut header = Sqlite3DbHeader::new_zeroed();
        f.read_exact(header.as_bytes_mut()).unwrap();
        header.replication_index.get()
    }

    #[test]
    fn inject_replication_index() {
        let tmp = tempdir().unwrap();
        let wal_manager = ReplicationIndexInjectorWal::new(
            ReplicationIndexInjectorWrapper,
            Sqlite3WalManager::default(),
        );
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();
        conn.execute("create table test (x)", ()).unwrap();
        for _ in 0..10 {
            conn.execute("insert into test values (42)", ()).unwrap();
        }

        let mut pages_in_log = 0;
        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                &mut pages_in_log,
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }

        let db_path = tmp.path().join("data");
        let mut f = std::fs::File::open(db_path).unwrap();
        let mut header = Sqlite3DbHeader::new_zeroed();
        f.read_exact(header.as_bytes_mut()).unwrap();
        assert_eq!(header.replication_index.get(), pages_in_log as u64)
    }

    #[test]
    fn inject_while_holding_write_lock() {
        let tmp = tempdir().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let path = tmp.path().to_path_buf();
        std::thread::spawn(move || {
            let mut conn1 = open_conn(&path, Sqlite3WalManager::default(), None).unwrap();
            let txn = conn1
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .unwrap();
            txn.execute("create table test (x)", ()).unwrap();
            txn.execute("insert into test values (12)", ()).unwrap();
            rx.recv().unwrap();
            txn.commit().unwrap();
        });

        // wait a bit to make sure the thread acquires the lock first
        std::thread::sleep(Duration::from_millis(100));

        let wal_manager = ReplicationIndexInjectorWal::new(
            ReplicationIndexInjectorWrapper,
            Sqlite3WalManager::default(),
        );
        let conn2 = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();
        conn2.busy_timeout(Duration::from_secs(1)).unwrap();

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn2.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 5);
        }

        tx.send(()).unwrap();

        std::thread::sleep(Duration::from_millis(100));

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn2.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }
    }

    #[test]
    fn checkpoint_on_close() {
        let tmp = tempdir().unwrap();
        let wal_manager = ReplicationIndexInjectorWal::new(
            ReplicationIndexInjectorWrapper,
            Sqlite3WalManager::default(),
        );
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();

        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        let mut header = Sqlite3DbHeader::new_zeroed();
        let file = std::fs::File::open(tmp.path().join("data")).unwrap();
        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.replication_index.get(), 0);
        drop(conn);
        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.replication_index.get(), 4);
    }

    #[test]
    fn partial_checkpoint() {
        #[derive(Clone)]
        struct CheckpointFailWrapper(Arc<AtomicBool>);

        impl<W: Wal> WrapWal<W> for CheckpointFailWrapper {
            fn checkpoint(
                &mut self,
                wrapped: &mut W,
                db: &mut libsql_sys::wal::Sqlite3Db,
                mode: libsql_sys::wal::CheckpointMode,
                busy_handler: Option<&mut dyn BusyHandler>,
                sync_flags: u32,
                // temporary scratch buffer
                buf: &mut [u8],
                _checkpoint_cb: Option<&mut dyn CheckpointCallback>,
            ) -> libsql_sys::wal::Result<(u32, u32)> {
                struct FailCb(Arc<AtomicBool>);

                impl CheckpointCallback for FailCb {
                    fn frame(
                        &mut self,
                        _frame: &[u8],
                        _page_no: NonZeroU32,
                        _frame_no: NonZeroU32,
                    ) -> libsql_sys::wal::Result<()> {
                        if self.0.load(Ordering::SeqCst) {
                            Err(libsql_sys::wal::Error::new(SQLITE_IOERR_WRITE))
                        } else {
                            Ok(())
                        }
                    }

                    fn finish(&mut self) -> libsql_sys::wal::Result<()> {
                        Ok(())
                    }
                }

                wrapped.checkpoint(
                    db,
                    mode,
                    busy_handler,
                    sync_flags,
                    buf,
                    Some(&mut FailCb(self.0.clone())),
                )
            }
        }

        let tmp = tempdir().unwrap();
        let enabled = Arc::new(AtomicBool::new(true));
        let wal_manager = WalWrapper::new(
            CheckpointFailWrapper(enabled.clone()),
            Sqlite3WalManager::default(),
        );
        let wal_manager =
            ReplicationIndexInjectorWal::new(ReplicationIndexInjectorWrapper, wal_manager);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();

        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();
        conn.execute("insert into test values (123)", ()).unwrap();

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, SQLITE_IOERR_WRITE);
        }

        let mut header = Sqlite3DbHeader::new_zeroed();
        let file = std::fs::File::open(tmp.path().join("data")).unwrap();
        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.replication_index.get(), 5);

        enabled.store(false, Ordering::SeqCst);

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conn.handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }

        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        // last injection failed, a new page1 has been injected
        assert_eq!(header.replication_index.get(), 6);
    }

    #[test]
    fn multi_connections_edit_page_1() {
        let tmp = tempdir().unwrap();
        let conns = (0..3)
            .map(|_| {
                let wal_manager = ReplicationIndexInjectorWal::new(
                    ReplicationIndexInjectorWrapper,
                    Sqlite3WalManager::default(),
                );
                open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000000).unwrap()
            })
            .collect_vec();

        conns[0].execute("create table test (x)", ()).unwrap();
        conns
            .iter()
            .enumerate()
            .for_each(|(i, c)| {
                // load page 1 into cache somehow
                c.query_row("select count(*) from test", (), |_| Ok(())).unwrap();
                c.execute(&format!("create table test{i} (x)"), ()).unwrap();
        });

        unsafe {
            let rc = sqlite3_wal_checkpoint_v2(
                conns[0].handle(),
                std::ptr::null_mut(),
                SQLITE_CHECKPOINT_FULL,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(rc, 0);
        }

        let mut header = Sqlite3DbHeader::new_zeroed();
        let file = std::fs::File::open(tmp.path().join("data")).unwrap();
        file.read_exact_at(header.as_bytes_mut(), 0).unwrap();
        assert_eq!(header.replication_index.get(), 9);
    }

    #[tokio::test]
    async fn write_between_inject_and_checkpoint() {
        #[derive(Clone)]
        struct HoldCheckpointWrapper(Arc<tokio::sync::Notify>, bool);

        impl<W: Wal> WrapWal<W> for HoldCheckpointWrapper {
            fn checkpoint(
                    &mut self,
                    wrapped: &mut W,
                    db: &mut libsql_sys::wal::Sqlite3Db,
                    mode: libsql_sys::wal::CheckpointMode,
                    busy_handler: Option<&mut dyn BusyHandler>,
                    sync_flags: u32,
                    // temporary scratch buffer
                    buf: &mut [u8],
                    checkpoint_cb: Option<&mut dyn CheckpointCallback>,
                ) -> libsql_sys::wal::Result<(u32, u32)> {
                if self.1 {
                    dbg!();
                    self.0.notify_waiters();
                    tokio::runtime::Handle::current().block_on(self.0.clone().notified());
                    dbg!();
                    self.1 = false;
                }
                dbg!(wrapped.checkpoint(db, mode, busy_handler, sync_flags, buf, checkpoint_cb))
            }
        }

        let tmp = tempdir().unwrap();

        let wal_manager = ReplicationIndexInjectorWal::new(
            ReplicationIndexInjectorWrapper,
            Sqlite3WalManager::default()
        );
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, u32::MAX).unwrap();

        conn.execute("create table test (c)", ()).unwrap();
        conn.execute("insert into test values (43)", ()).unwrap();

        let notify = Arc::new(Notify::new());
        let handle = tokio::task::spawn_blocking({
            let notify = notify.clone();
            let wal_manager = ReplicationIndexInjectorWal::new(
                ReplicationIndexInjectorWrapper,
                WalWrapper::new(HoldCheckpointWrapper(notify, true), Sqlite3WalManager::default()));
            let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, u32::MAX).unwrap();
            move || {
                let mut backfilled = 0;
                let mut in_wal = 0;
                unsafe {
                    let rc = sqlite3_wal_checkpoint_v2(
                        conn.handle(),
                        std::ptr::null_mut(),
                        SQLITE_CHECKPOINT_RESTART,
                        &mut backfilled,
                        &mut in_wal,
                    );
                    assert_eq!(rc, SQLITE_BUSY);
                }

                assert_eq!(in_wal, -1);
                assert_eq!(backfilled, -1);

                backfilled = 0;
                in_wal = 0;

                unsafe {
                    let rc = sqlite3_wal_checkpoint_v2(
                        conn.handle(),
                        std::ptr::null_mut(),
                        SQLITE_CHECKPOINT_RESTART,
                        &mut backfilled,
                        &mut in_wal,
                    );
                    assert_eq!(rc, 0);
                }

                assert_eq!(in_wal, 6);
                assert_eq!(backfilled, 6);
            }});

        // Frame was injected, but not checkpointed yet
        notify.notified().await;
        conn.execute("insert into test values (43)", ()).unwrap();
        // proceed with checkpoint
        notify.notify_waiters();

        handle.await.unwrap();
    }

    #[test]
    fn multiple_injections() {
        let tmp = tempdir().unwrap();
        let wal_manager = Sqlite3WalManager::default()
            .wrap(ReplicationIndexInjectorWrapper);
        let conn = open_conn_enable_checkpoint(tmp.path(), wal_manager, None, 1000).unwrap();
        conn.execute("create table test (x)", ()).unwrap();
        conn.execute("insert into test values (12)", ()).unwrap();

        let (rc, backfilled, in_wal) = checkpoint(&conn);
        assert_eq!(rc, 0);
        assert_eq!(backfilled, 4);
        assert_eq!(in_wal, 4);

        assert_eq!(read_replication_index(tmp.path()), 4);

        conn.execute("insert into test values (12)", ()).unwrap();
        conn.execute("insert into test values (12)", ()).unwrap();

        let (rc, backfilled, in_wal) = checkpoint(&conn);
        assert_eq!(rc, 0);
        assert_eq!(backfilled, 3);
        assert_eq!(in_wal, 3);

        assert_eq!(read_replication_index(tmp.path()), 7);
    }
}
