use std::ptr;
use std::sync::Arc;

use super::ASYNC_RUNTIME;
use super::filter::{self, FilterExpr};
use super::jni_utils::extract_storage_config;
use arrow::array::{Array, StructArray};
use arrow::ffi;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use jni::EnvUnowned;
use jni::errors::{Error as JniError, Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong};
use super::store::{StorageConfig, resolve_store, needs_file_size_hint};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::PageIndexPolicy;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::errors::ParquetError;

// ===== Parquet direct reader (no DataFusion) =====

/// Number of concurrent row-group streams used by the S3 reader.
/// Mirrors what DataFusion's `ParquetExec` does internally (one stream per
/// `target_partitions`, defaulting to `num_cpus`). Capped to keep S3
/// connection pool usage reasonable.
fn s3_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
        .min(16)
}

enum ReaderKind {
    /// Batches arriving from N spawned tokio tasks, one per row-group chunk.
    Async(mpsc::Receiver<Result<RecordBatch, ParquetError>>),
}

struct ParquetReaderState {
    kind: ReaderKind,
    /// Rows remaining before the global limit is hit. `None` means unlimited.
    remaining: Option<usize>,
    plan: String,
}

/// Convert any Display error to a JNI error.
fn err(e: impl std::fmt::Display) -> JniError {
    JniError::ParseFailed(format!("{e}"))
}

// ---------------------------------------------------------------------------
// JNI entry points
// ---------------------------------------------------------------------------

/// Open a Parquet file using the parquet crate directly.
/// Local files use synchronous I/O; remote URLs use async I/O via object_store.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_openReader(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    projected_columns: JObjectArray,
    batch_size: jint,
    limit: jlong,
    filter_handle: jlong,
    config_json: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let file_path_str = file_path.try_to_string(env)?;
        let storage_config = extract_storage_config(env, &config_json)?;

        let projected_cols: Option<Vec<String>> = if projected_columns.is_null() {
            None
        } else {
            let len = projected_columns.len(env)?;
            let mut cols = Vec::with_capacity(len);
            for i in 0..len {
                let obj = projected_columns.get_element(env, i)?;
                let jstr: JString = JString::cast_local(env, obj)?;
                cols.push(jstr.try_to_string(env)?);
            }
            Some(cols)
        };

        let filter: Option<Arc<FilterExpr>> = if filter_handle != 0 {
            Some(Arc::new(unsafe { &*(filter_handle as *const FilterExpr) }.clone()))
        } else {
            None
        };

        let plan = build_plan_string(&file_path_str, &projected_cols, batch_size, limit, &filter);

        let (store, object_path) = resolve_store(&file_path_str, &storage_config).map_err(err)?;
        let concurrency = if file_path_str.starts_with("s3://") { s3_concurrency() } else { local_concurrency() };
        let needs_head = needs_file_size_hint(&file_path_str);
        let kind = open_async(store, object_path, projected_cols, batch_size, limit, filter, concurrency, needs_head)?;

        let remaining = if limit > 0 { Some(limit as usize) } else { None };
        let state = Box::new(ParquetReaderState { kind, remaining, plan });
        Ok(Box::into_raw(state) as jlong)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Open multiple Parquet files, fetching metadata for all files in parallel.
/// All files are read through a single mpsc channel.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_openReaderMulti(
    mut env: EnvUnowned,
    _class: JClass,
    file_paths: JObjectArray,
    projected_columns: JObjectArray,
    batch_size: jint,
    limit: jlong,
    filter_handle: jlong,
    config_json: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let num_files = file_paths.len(env)?;
        let mut paths = Vec::with_capacity(num_files);
        for i in 0..num_files {
            let obj = file_paths.get_element(env, i)?;
            let jstr: JString = JString::cast_local(env, obj)?;
            paths.push(jstr.try_to_string(env)?);
        }
        let storage_config = extract_storage_config(env, &config_json)?;

        let projected_cols: Option<Vec<String>> = if projected_columns.is_null() {
            None
        } else {
            let len = projected_columns.len(env)?;
            let mut cols = Vec::with_capacity(len);
            for i in 0..len {
                let obj = projected_columns.get_element(env, i)?;
                let jstr: JString = JString::cast_local(env, obj)?;
                cols.push(jstr.try_to_string(env)?);
            }
            Some(cols)
        };

        let filter: Option<Arc<FilterExpr>> = if filter_handle != 0 {
            Some(Arc::new(unsafe { &*(filter_handle as *const FilterExpr) }.clone()))
        } else {
            None
        };

        let plan = build_plan_string(&paths.join(", "), &projected_cols, batch_size, limit, &filter);

        let kind = open_multi(&paths, &storage_config, projected_cols, batch_size, limit, filter, s3_concurrency())?;

        let remaining = if limit > 0 { Some(limit as usize) } else { None };
        let state = Box::new(ParquetReaderState { kind, remaining, plan });
        Ok(Box::into_raw(state) as jlong)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

fn local_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(8)
}

/// Spawn worker tasks for a single file, sending decoded batches into `tx`.
/// Returns the number of surviving row groups (0 means the file was fully pruned).
async fn spawn_file_workers(
    obj_reader: ParquetObjectReader,
    arrow_meta: ArrowReaderMetadata,
    projected_cols: &Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: &Option<Arc<FilterExpr>>,
    max_concurrency: usize,
    tx: &mpsc::Sender<Result<RecordBatch, ParquetError>>,
) -> Result<usize, JniError> {
    let metadata = arrow_meta.metadata().clone();
    let arrow_schema = arrow_meta.schema().clone();
    let parquet_schema = arrow_meta.parquet_schema().clone();

    let surviving: Vec<usize> = (0..metadata.num_row_groups())
        .filter(|&rg_idx| {
            if let Some(expr) = filter {
                filter::row_group_matches(expr, metadata.row_group(rg_idx), &parquet_schema)
            } else {
                true
            }
        })
        .collect();

    if surviving.is_empty() {
        return Ok(0);
    }

    let n_workers = max_concurrency.min(surviving.len()).max(1);
    let chunks: Vec<Vec<usize>> = (0..n_workers)
        .map(|w| surviving.iter().skip(w).step_by(n_workers).copied().collect())
        .filter(|c: &Vec<usize>| c.is_empty() == false)
        .collect();

    for chunk in chunks {
        let mut builder =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                obj_reader.clone(),
                arrow_meta.clone(),
            );

        builder = builder.with_batch_size(batch_size as usize);
        if limit > 0 {
            builder = builder.with_limit(limit as usize);
        }

        if let Some(cols) = projected_cols {
            let indices: Vec<usize> = cols
                .iter()
                .map(|name| arrow_schema.index_of(name).map_err(err))
                .collect::<std::result::Result<_, _>>()?;
            let mask = ProjectionMask::roots(&parquet_schema, indices);
            builder = builder.with_projection(mask);
        }

        builder = builder.with_row_groups(chunk.clone());

        if let Some(expr) = filter {
            let row_filter = filter::build_row_filter(expr, arrow_schema.clone(), &parquet_schema);
            builder = builder.with_row_filter(row_filter);
        }

        let mut stream = builder.build().map_err(err)?;
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(async {
                while let Some(reader) = stream.next_row_group().await.transpose() {
                    let reader = match reader {
                        Ok(r) => r,
                        Err(e) => { let _ = tx.send(Err(e)).await; break; }
                    };
                    for batch in reader {
                        if tx.send(batch.map_err(Into::into)).await.is_err() {
                            return;
                        }
                    }
                }
            });
            if let Err(panic) = futures::FutureExt::catch_unwind(result).await {
                let msg = match panic.downcast_ref::<&str>() {
                    Some(s) => s.to_string(),
                    None => match panic.downcast_ref::<String>() {
                        Some(s) => s.clone(),
                        None => "unknown panic in parquet worker".to_string(),
                    },
                };
                let _ = tx.send(Err(ParquetError::General(msg))).await;
            }
        });
    }

    Ok(surviving.len())
}

/// Async reader shared by both local and remote paths.
///
/// Reads file metadata once, prunes row groups with the predicate's column
/// statistics, then splits surviving row groups into N chunks
/// (N = `max_concurrency`). Each chunk gets its own `ParquetRecordBatchStream`
/// running as a spawned tokio task, with decoded batches sent through an mpsc
/// channel. Within each task, `next_row_group()` pipelines I/O and decode.
fn open_async(
    store: Arc<dyn ObjectStore>,
    object_path: object_store::path::Path,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
    needs_head: bool,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async move {
        let inner = async {
            let mut obj_reader = ParquetObjectReader::new(store.clone(), object_path.clone());
            if needs_head {
                let meta = store.head(&object_path).await.map_err(err)?;
                obj_reader = obj_reader.with_file_size(meta.size as u64);
            }

            let arrow_meta = ArrowReaderMetadata::load_async(
                &mut obj_reader, ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
            ).await.map_err(err)?;

            let n_workers = max_concurrency;
            let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

            spawn_file_workers(
                obj_reader, arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx,
            ).await?;

            drop(tx);
            Ok::<_, JniError>(rx)
        };

        tokio::time::timeout(std::time::Duration::from_secs(300), inner)
            .await
            .map_err(|_| err("Parquet async open timed out after 300s"))?
    })?;

    Ok(ReaderKind::Async(rx))
}

/// Open multiple Parquet files with parallel metadata fetching.
/// All metadata is fetched concurrently, then workers are spawned across all files,
/// feeding a single mpsc channel.
fn open_multi(
    file_paths: &[String],
    config: &StorageConfig,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async {
        let mut file_infos = Vec::with_capacity(file_paths.len());
        for path in file_paths {
            let (store, object_path) = resolve_store(path, config).map_err(err)?;
            file_infos.push((store, object_path));
        }

        let needs_head = file_paths.first().map_or(false, |p| needs_file_size_hint(p));
        let meta_options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let meta_futs: Vec<_> = file_infos.iter().map(|(store, path)| {
            let store = Arc::clone(store);
            let path = path.clone();
            let opts = meta_options.clone();
            async move {
                let mut reader = ParquetObjectReader::new(store.clone(), path.clone());
                if needs_head {
                    let head = store.head(&path).await.map_err(err)?;
                    reader = reader.with_file_size(head.size as u64);
                }
                let meta = ArrowReaderMetadata::load_async(&mut reader, opts).await.map_err(err)?;
                Ok::<_, JniError>((reader, meta))
            }
        }).collect();
        let all_meta = futures::future::try_join_all(meta_futs).await?;

        let n_workers = max_concurrency;
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

        for (obj_reader, arrow_meta) in all_meta {
            spawn_file_workers(
                obj_reader, arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx,
            ).await?;
        }
        drop(tx);

        Ok::<_, JniError>(rx)
    })?;

    Ok(ReaderKind::Async(rx))
}


fn build_plan_string(
    file_path: &str,
    projected_cols: &Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: &Option<Arc<FilterExpr>>,
) -> String {
    let mut parts = Vec::new();
    let basename = file_path.rsplit('/').next().unwrap_or(file_path);
    parts.push(format!("file=[{basename}]"));
    if let Some(cols) = projected_cols {
        parts.push(format!("projection=[{}]", cols.join(", ")));
    }
    if let Some(expr) = filter {
        parts.push(format!("filter=[{expr}]"));
    }
    if limit > 0 {
        parts.push(format!("limit={limit}"));
    }
    parts.push(format!("batch_size={batch_size}"));
    format!("ParquetRsScan: {}", parts.join(", "))
}

/// Read the next batch (sync or async depending on reader kind).
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_nextBatch(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
    schema_addr: jlong,
    array_addr: jlong,
) -> jboolean {
    env.with_env(|_env| -> JniResult<jboolean> {
        let state = unsafe { &mut *(handle as *mut ParquetReaderState) };

        if state.remaining == Some(0) {
            return Ok(JNI_FALSE);
        }

        let batch = match &mut state.kind {
            ReaderKind::Async(rx) => match rx.blocking_recv() {
                Some(r) => Some(r.map_err(err)),
                None => None,
            },
        };

        match batch {
            None => Ok(JNI_FALSE),
            Some(result) => {
                let batch = result?;

                let batch = if let Some(ref mut rem) = state.remaining {
                    if batch.num_rows() <= *rem {
                        *rem -= batch.num_rows();
                        batch
                    } else {
                        let take = *rem;
                        *rem = 0;
                        batch.slice(0, take)
                    }
                } else {
                    batch
                };

                let struct_array = StructArray::from(batch);
                let data = struct_array.to_data();

                let ffi_schema = ffi::FFI_ArrowSchema::try_from(data.data_type()).map_err(err)?;
                let ffi_array = ffi::FFI_ArrowArray::new(&data);

                unsafe {
                    ptr::write(schema_addr as *mut ffi::FFI_ArrowSchema, ffi_schema);
                    ptr::write(array_addr as *mut ffi::FFI_ArrowArray, ffi_array);
                }

                Ok(JNI_TRUE)
            }
        }
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Close the parquet-direct reader.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_closeReader(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
) {
    env.with_env(|_env| -> JniResult<()> {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut ParquetReaderState));
            }
        }
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getReaderPlan(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
) -> jni::sys::jobject {
    env.with_env(|env| -> JniResult<jni::sys::jobject> {
        let state = unsafe { &*(handle as *const ParquetReaderState) };
        let jstr = env.new_string(&state.plan)?;
        Ok(jstr.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
