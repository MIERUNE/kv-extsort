use std::{
    error::Error,
    sync::{atomic::AtomicBool, Arc},
};

use bytemuck::Pod;
use crossbeam_channel::{bounded, unbounded, Receiver, Select, Sender};
use log::debug;

use crate::{
    chunk::{FileChunk, FileChunkDir, MemChunk},
    merge::merge_chunks_with_binary_heap,
    Result,
};

pub struct SortConfig {
    pub(crate) max_chunk_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) merge_k: usize,
    pub(crate) canceled: Arc<AtomicBool>,
}

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 512 * 1024 * 1024,
            concurrency: 8,
            merge_k: 16,
            canceled: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl SortConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_cancel_flag(self, canceled: Arc<AtomicBool>) -> Self {
        Self { canceled, ..self }
    }

    pub fn get_cancel_flag(&self) -> Arc<AtomicBool> {
        self.canceled.clone()
    }

    pub(crate) fn ensure_not_canceled<E>(&self) -> Result<(), E> {
        if self.canceled.load(std::sync::atomic::Ordering::Relaxed) {
            Err(crate::Error::Canceled)
        } else {
            Ok(())
        }
    }

    pub fn max_chunk_bytes(self, max_chunk_bytes: usize) -> Self {
        assert!(
            max_chunk_bytes > 0,
            "max_chunk_bytes must be greater than 0"
        );
        Self {
            max_chunk_bytes,
            ..self
        }
    }

    pub fn concurrency(self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be greater than 0");
        Self {
            concurrency,
            ..self
        }
    }

    pub fn merge_k(self, merge_k: usize) -> Self {
        assert!(merge_k >= 2, "merge_k must not be less than 2");
        Self { merge_k, ..self }
    }
}

pub fn sort<K, E>(
    source: impl Iterator<Item = std::result::Result<(K, Vec<u8>), E>> + Send,
    config: SortConfig,
) -> SortedIter<K, E>
where
    K: Ord + Pod + Copy + Send + Sync,
    E: Error + Send + 'static,
{
    let (output_tx, output_rx) = bounded(config.concurrency * 16);
    let chunk_dir = match FileChunkDir::<K>::new() {
        Ok(chunk_dir) => Arc::new(chunk_dir),
        Err(e) => {
            let _ = output_tx.send(Err(e));
            return SortedIter::new(output_rx, None);
        }
    };
    let (file_chunk_tx, file_chunk_rx) = unbounded();
    let chunk_dir = chunk_dir.clone();

    {
        let chunk_dir = chunk_dir.clone();
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.concurrency + 1)
            .build()
            .unwrap()
            .install(|| {
                start_sorting_stage(&config, source, chunk_dir.clone(), file_chunk_tx);
                start_merging_stage(&config, file_chunk_rx, chunk_dir.clone(), output_tx);
            });
    }

    SortedIter::new(output_rx, Some(chunk_dir))
}

pub struct SortedIter<K: Pod, E> {
    output_rx: Receiver<Result<(K, Vec<u8>), E>>,
    done: bool,
    #[allow(dead_code)]
    chunk_dir: Option<Arc<FileChunkDir<K>>>,
}

impl<K: Pod, E> SortedIter<K, E> {
    fn new(
        output_rx: Receiver<Result<(K, Vec<u8>), E>>,
        chunk_dir: Option<Arc<FileChunkDir<K>>>,
    ) -> Self {
        SortedIter {
            output_rx,
            chunk_dir,
            done: false,
        }
    }
}

impl<K: Pod, E> Iterator for SortedIter<K, E> {
    type Item = Result<(K, Vec<u8>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.output_rx.recv() {
            Ok(Ok(v)) => Some(Ok(v)),
            Ok(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            Err(_) => {
                self.done = true;
                None
            }
        }
    }
}

fn mem_to_file_chunk<K: Pod + Ord, E>(
    buffer: Vec<(K, Vec<u8>)>,
    chunk_dir: Arc<FileChunkDir<K>>,
) -> Result<FileChunk<K>, E> {
    let mem_chunk = MemChunk::from_unsorted(buffer);
    let mut file_chunk = chunk_dir.add_chunk()?;
    mem_chunk.write_to_file(&mut file_chunk)?;
    Ok(file_chunk.finalize())
}

fn start_sorting_stage<K, E>(
    config: &SortConfig,
    source: impl Iterator<Item = std::result::Result<(K, Vec<u8>), E>> + Send,
    chunk_dir: Arc<FileChunkDir<K>>,
    chunk_tx: Sender<Result<FileChunk<K>, E>>,
) where
    K: Ord + Pod + Copy + Send + Sync,
    E: Send + 'static,
{
    debug!("Sorting stage started.");

    let item_header_size = std::mem::size_of::<Vec<u8>>();
    let mut chunk_size = 0;

    let mut buffer = Vec::new();

    for res in source {
        match res {
            Ok((key, value)) => {
                let item_size = item_header_size + value.len();
                if chunk_size + item_size >= config.max_chunk_bytes {
                    let buffer = std::mem::take(&mut buffer);
                    let chunk_dir = chunk_dir.clone();
                    let chunk_tx = chunk_tx.clone();
                    if let Err(e) = config.ensure_not_canceled() {
                        let _ = chunk_tx.send(Err(e));
                        return;
                    }
                    rayon::spawn(move || {
                        let _ = chunk_tx.send(mem_to_file_chunk(buffer, chunk_dir));
                    });
                    chunk_size = 0;
                }
                chunk_size += item_size;
                buffer.push((key, value));
            }
            Err(e) => {
                let _ = chunk_tx.send(Err(crate::Error::Source(e)));
            }
        }
    }

    // last chunk
    if !buffer.is_empty() {
        if let Err(e) = config.ensure_not_canceled() {
            let _ = chunk_tx.send(Err(e));
            return;
        }
        rayon::spawn(move || {
            let _ = chunk_tx.send(mem_to_file_chunk(buffer, chunk_dir));
        });
    }
}

fn start_merging_stage<K, E>(
    config: &SortConfig,
    chunk_rx: Receiver<Result<FileChunk<K>, E>>,
    chunk_dir: Arc<FileChunkDir<K>>,
    output_tx: Sender<Result<(K, Vec<u8>), E>>,
) where
    K: Ord + Pod + Copy + Send + Sync,
    E: Send + 'static,
{
    debug!("Merging stage started.");

    let (merged_tx, merged_rx) = unbounded::<Result<FileChunk<K>, E>>();
    let mut pending = Vec::new();
    let mut source_finished = false;
    let mut num_running_merges = 0;

    let mut recv_select = Select::new();
    recv_select.recv(&chunk_rx); // select index=0
    recv_select.recv(&merged_rx); // select index=1

    loop {
        let idx = recv_select.ready();
        match idx {
            // Receive chunks from the sorting stage
            0 => match chunk_rx.try_recv() {
                Ok(Ok(chunk)) => {
                    debug!("Received chunk: items={}", chunk.len());
                    pending.push(chunk)
                }
                Ok(Err(e)) => {
                    let _ = output_tx.send(Err(e));
                    break;
                }
                Err(_) => {
                    debug!("All chunks received from the sorting stage");
                    source_finished = true;
                    recv_select.remove(0);
                }
            },
            // Receive merged chunks
            1 => match merged_rx.try_recv() {
                Ok(Ok(chunk)) => {
                    debug!("Received merged chunk: items={}", chunk.len());
                    num_running_merges -= 1;
                    pending.push(chunk)
                }
                Ok(Err(e)) => {
                    let _ = output_tx.send(Err(e));
                    break;
                }
                Err(_) => {
                    panic!("merged_rx should not be closed at this point")
                }
            },
            _ => unreachable!(),
        }

        if let Err(e) = config.ensure_not_canceled() {
            let _ = output_tx.send(Err(e));
        }

        // Plan to merge
        let total_chunks = pending.len() + num_running_merges;
        let num_merge = if source_finished {
            if pending.len() > config.merge_k {
                (total_chunks - config.merge_k + 1).min(config.merge_k)
            } else if num_running_merges == 0 {
                break;
            } else {
                continue;
            }
        } else if total_chunks >= config.merge_k * 2 - 1 {
            if pending.len() >= config.merge_k {
                config.merge_k
            } else {
                continue;
            }
        } else {
            continue;
        };

        pending.sort_by_key(|chunk| chunk.len());
        let remaining = pending.split_off(num_merge.min(pending.len()));
        let merging = std::mem::replace(&mut pending, remaining);

        let merged_tx = merged_tx.clone();
        let mut chunk_writer = match chunk_dir.add_chunk() {
            Ok(chunk_writer) => chunk_writer,
            Err(e) => {
                let _ = output_tx.send(Err(e));
                break;
            }
        };

        // Start merging
        debug!("Start merging {} chunks", merging.len());
        num_running_merges += 1;
        let canceled = config.canceled.clone();
        rayon::spawn(move || {
            match merge_chunks_with_binary_heap(canceled, merging, |(key, value)| {
                chunk_writer.push(&key, &value)
            }) {
                Ok(()) => {
                    let _ = merged_tx.send(Ok(chunk_writer.finalize()));
                }
                Err(e) => {
                    let _ = merged_tx.send(Err(e));
                }
            }
        });
    }

    let canceled = config.canceled.clone();
    if canceled.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }

    debug!("Start iteration by merging {} chunks", pending.len());
    rayon::spawn(move || {
        if let Err(e) = merge_chunks_with_binary_heap(canceled, pending, |(key, value)| {
            let _ = output_tx.send(Ok((key, value)));
            Ok(())
        }) {
            let _ = merged_tx.send(Err(e));
        }
        drop(chunk_dir);
    });
}
