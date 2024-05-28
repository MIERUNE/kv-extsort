use kv_extsort::{Result, SortConfig};
use log::{debug, error};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::{convert::Infallible, time::Instant};

#[test]
fn test_simple() -> Result<(), Infallible> {
    env_logger::init();

    // Data source
    let size = 200_000;
    let make_iter = || {
        let body_size = 2046;

        let mut rng = SmallRng::from_entropy();
        type T = (i32, Vec<u8>);
        let source = (0..size).map(move |_| {
            let u: i32 = rng.gen();
            let s = vec![0; body_size];
            (u, s)
        });

        let head_size = std::mem::size_of::<T>();
        let total_size = size * (head_size + body_size);
        debug!("Total size: {} MiB", total_size / 1024 / 1024);

        source
    };

    let t = Instant::now();
    let config = SortConfig::new()
        .max_chunk_bytes(2 * 1024 * 1024)
        .concurrency(num_cpus::get())
        .merge_k(16);

    // Setup cancellation
    let canceled = config.get_cancel_flag();
    ctrlc::set_handler(move || {
        canceled.store(true, std::sync::atomic::Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    // Sort
    let mut prev_key = None;
    let mut count = 0;
    for res in kv_extsort::sort(make_iter().map(Result::<_, Infallible>::Ok), config) {
        match res {
            Ok((_key, _value)) => {
                // validate if sorted
                count += 1;
                if let Some(p) = prev_key {
                    assert!(p <= _key);
                    prev_key = Some(_key);
                }
            }
            Err(e) => {
                error!("{:?}", e);
                break;
            }
        }
    }

    debug!("count: {count}");
    assert!(count == size, "count: {} != size: {}", count, size);
    debug!("Time elapsed: {:?}", t.elapsed());

    Ok(())
}
