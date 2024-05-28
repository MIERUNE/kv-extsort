use std::{
    collections::BinaryHeap,
    sync::{atomic::AtomicBool, Arc},
};

use bytemuck::Pod;
use log::warn;

use crate::{chunk::FileChunk, Result};

// pub fn merge_chunks_by_naive_picking<K, E>(
//     canceled: Arc<AtomicBool>,
//     chunks: Vec<FileChunk<K>>,
//     mut add_fn: impl FnMut((K, Vec<u8>)) -> Result<(), E>,
// ) -> Result<(), E>
// where
//     K: Ord + Pod + Copy + Send + Sync,
// {
//     let tmp_file_paths = chunks
//         .iter()
//         .map(|chunk| chunk.path().to_owned())
//         .collect::<Vec<_>>();
//
//     let mut chunk_iters = chunks
//         .into_iter()
//         .map(|chunk| Ok(chunk.iter()?.peekable()))
//         .collect::<Result<Vec<_>, E>>()?;
//
//     loop {
//         let mut min_key = None;
//         let mut min_key_idx = None;
//         let mut found_ranout = false;
//
//         if canceled.load(std::sync::atomic::Ordering::Relaxed) {
//             break;
//         }
//
//         for (idx, iter) in chunk_iters.iter_mut().enumerate() {
//             match iter.peek() {
//                 Some(Ok((key, _))) => {
//                     if min_key.is_none()
//                         || key < min_key.as_ref().expect("min_key should have value")
//                     {
//                         min_key = Some(*key);
//                         min_key_idx = Some(idx);
//                     }
//                 }
//                 Some(Err(_)) => {
//                     min_key_idx = Some(idx);
//                     break;
//                 }
//                 None => {
//                     found_ranout = true;
//                 }
//             }
//         }
//
//         if let Some(min_key_idx) = min_key_idx {
//             match chunk_iters[min_key_idx].next() {
//                 Some(Ok((key, value))) => {
//                     add_fn((key, value))?;
//                 }
//                 Some(Err(e)) => {
//                     return Err(e);
//                 }
//                 None => unreachable!(),
//             }
//         } else {
//             break;
//         }
//
//         if found_ranout {
//             // remove ran-out iterators
//             chunk_iters.retain_mut(|it| it.peek().is_some());
//         }
//     }
//
//     for path in tmp_file_paths {
//         if std::fs::remove_file(&path).is_err() {
//             warn!("Failed to remove file: {:?}", path);
//         }
//     }
//
//     match canceled.load(std::sync::atomic::Ordering::Relaxed) {
//         false => Ok(()),
//         true => Err(crate::Error::Canceled),
//     }
// }

struct HeapItem<K: Ord> {
    key: K,
    iter_idx: usize,
    value: Vec<u8>,
}

impl<K: Ord> Ord for HeapItem<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.key.cmp(&self.key)
    }
}

impl<K: Ord> PartialOrd for HeapItem<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Eq for HeapItem<K> {}

impl<K: Ord> PartialEq for HeapItem<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

pub fn merge_chunks_with_binary_heap<K, E>(
    canceled: Arc<AtomicBool>,
    chunks: Vec<FileChunk<K>>,
    mut add_fn: impl FnMut((K, Vec<u8>)) -> Result<(), E>,
) -> Result<(), E>
where
    K: Ord + Pod + Copy + Send + Sync,
{
    let tmp_file_paths = chunks
        .iter()
        .map(|chunk| chunk.path().to_owned())
        .collect::<Vec<_>>();

    let mut heap = BinaryHeap::with_capacity(chunks.len());

    let mut chunk_iters = chunks
        .into_iter()
        .map(|chunk| chunk.iter())
        .collect::<Result<Vec<_>, E>>()?;

    for (idx, iter) in chunk_iters.iter_mut().enumerate() {
        match iter.next() {
            Some(Ok((key, value))) => heap.push(HeapItem {
                key,
                value,
                iter_idx: idx,
            }),
            None => {}
            Some(Err(e)) => {
                return Err(e);
            }
        }
    }

    loop {
        if canceled.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        let Some(&HeapItem { iter_idx, .. }) = heap.peek() else {
            break;
        };

        match chunk_iters[iter_idx].next() {
            Some(Ok((key, value))) => {
                let mut head = heap.peek_mut().expect("must not be none");
                add_fn((head.key, std::mem::replace(&mut head.value, value)))?;
                head.key = key;
            }
            None => {
                let HeapItem { key, value, .. } = heap.pop().expect("must not be none");
                add_fn((key, value))?;
            }
            Some(Err(e)) => {
                return Err(e);
            }
        }
    }

    for path in tmp_file_paths {
        if std::fs::remove_file(&path).is_err() {
            warn!("Failed to remove file: {:?}", path);
        }
    }

    match canceled.load(std::sync::atomic::Ordering::Relaxed) {
        false => Ok(()),
        true => Err(crate::Error::Canceled),
    }
}
