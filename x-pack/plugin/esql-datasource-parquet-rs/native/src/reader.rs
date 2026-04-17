use std::ptr;
use std::sync::Arc;

use super::ASYNC_RUNTIME;
use super::filter::{self, FilterExpr};
use arrow::array::{Array, StructArray};
use arrow::ffi;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use jni::EnvUnowned;
use jni::errors::{Error as JniError, Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, RowFilter};
use parquet::file::metadata::PageIndexPolicy;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::errors::ParquetError;
use url::Url;

// ===== Parquet direct reader (no DataFusion) =====
// This file is mostly generated. See datafusion_reader.rs for detailed explanations.

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
    Sync(ParquetRecordBatchReader),
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
/// Local files use synchronous I/O; S3 URLs use async I/O via object_store.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_openReader(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    projected_columns: JObjectArray,
    batch_size: jint,
    limit: jlong,
    filter_handle: jlong,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let file_path_str = file_path.try_to_string(env)?;

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

        let kind = if file_path_str.starts_with("s3://") {
            open_s3(&file_path_str, projected_cols, batch_size, limit, filter, s3_concurrency())?
        } else {
            open_local_async(&file_path_str, projected_cols, batch_size, limit, filter, local_concurrency())?
        };

        let remaining = if limit > 0 { Some(limit as usize) } else { None };
        let state = Box::new(ParquetReaderState { kind, remaining, plan });
        Ok(Box::into_raw(state) as jlong)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Open multiple S3 Parquet files, fetching metadata for all files in parallel.
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
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let num_files = file_paths.len(env)?;
        let mut paths = Vec::with_capacity(num_files);
        for i in 0..num_files {
            let obj = file_paths.get_element(env, i)?;
            let jstr: JString = JString::cast_local(env, obj)?;
            paths.push(jstr.try_to_string(env)?);
        }

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

        let kind = open_s3_multi(&paths, projected_cols, batch_size, limit, filter, s3_concurrency())?;

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

/// Open a local Parquet file via the async path with multi-worker row group parallelism.
/// Uses `object_store::local::LocalFileSystem` which dispatches I/O to blocking threads
/// via `spawn_blocking` when called from a tokio context.
fn open_local_async(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
) -> JniResult<ReaderKind> {
    let local_fs = LocalFileSystem::new_with_prefix("/").map_err(err)?;
    let store: Arc<dyn ObjectStore> = Arc::new(local_fs);
    let normalized = file_path.strip_prefix('/').unwrap_or(file_path);
    let object_path = object_store::path::Path::from(normalized);
    open_async(store, object_path, projected_cols, batch_size, limit, filter, max_concurrency)
}

/// Open a local Parquet file (synchronous, single-threaded).
fn open_local(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
) -> JniResult<ReaderKind> {
    let file = std::fs::File::open(file_path).map_err(err)?;
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let mut builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .map_err(err)?;

    builder = setup_builder(builder, projected_cols, batch_size, limit, filter)?;

    let reader = builder.build().map_err(err)?;
    Ok(ReaderKind::Sync(reader))
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
    }

    Ok(surviving.len())
}

/// Async reader shared by both local and S3 paths.
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
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async move {
        let inner = async {
            let mut obj_reader = ParquetObjectReader::new(store, object_path);

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

/// Open a Parquet file on S3 (async via object_store).
///
/// Uses the default AWS credential chain: env vars (AWS_ACCESS_KEY_ID, etc.)
/// → ~/.aws/credentials → IMDS (EC2 instance profile).
/// Region: reads AWS_REGION / AWS_DEFAULT_REGION env vars.
fn open_s3(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
) -> JniResult<ReaderKind> {
    let url = Url::parse(file_path).map_err(err)?;
    let bucket = url
        .host_str()
        .ok_or_else(|| err("missing bucket in S3 URL"))?;
    let key = url.path().trim_start_matches('/');

    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(err)?;
    let store: Arc<dyn ObjectStore> = Arc::new(s3);
    let object_path = object_store::path::Path::from(key);

    open_async(store, object_path, projected_cols, batch_size, limit, filter, max_concurrency)
}

/// Open multiple S3 Parquet files with parallel metadata fetching.
/// All metadata is fetched concurrently, then workers are spawned across all files,
/// feeding a single mpsc channel.
fn open_s3_multi(
    file_paths: &[String],
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async {
        let mut file_infos = Vec::with_capacity(file_paths.len());
        for path in file_paths {
            let url = Url::parse(path).map_err(err)?;
            let bucket = url.host_str().ok_or_else(|| err("missing bucket"))?;
            let key = url.path().trim_start_matches('/');
            let s3 = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(err)?;
            let store: Arc<dyn ObjectStore> = Arc::new(s3);
            let object_path = object_store::path::Path::from(key);
            file_infos.push((store, object_path));
        }

        let meta_options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let meta_futs: Vec<_> = file_infos.iter().map(|(store, path)| {
            let mut reader = ParquetObjectReader::new(Arc::clone(store), path.clone());
            let opts = meta_options.clone();
            async move {
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


pub fn setup_builder<T>(
    mut builder: ArrowReaderBuilder<T>,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
) -> JniResult<ArrowReaderBuilder<T>> {
    builder = builder.with_batch_size(batch_size as usize);

    if limit > 0 {
        builder = builder.with_limit(limit as usize);
    }

    let arrow_schema = builder.schema().clone();

    // Column projection
    if let Some(ref cols) = projected_cols {
        let indices: Vec<usize> = cols
            .iter()
            .map(|name| arrow_schema.index_of(name).map_err(err))
            .collect::<std::result::Result<_, _>>()?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }

    // Row-group pruning and row-level filter via FilterExpr
    if let Some(ref expr) = filter {
        let metadata = builder.metadata().clone();
        let parquet_schema = builder.parquet_schema().clone();
        let selected: Vec<usize> = (0..metadata.num_row_groups())
            .filter(|&rg| filter::row_group_matches(expr, metadata.row_group(rg), &parquet_schema))
            .collect();
        builder = builder.with_row_groups(selected);

        let row_filter = filter::build_row_filter(expr, arrow_schema, &parquet_schema);
        builder = builder.with_row_filter(row_filter);
    }

    Ok(builder)
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
            ReaderKind::Sync(reader) => match reader.next() {
                None => None,
                Some(r) => Some(r.map_err(err)),
            },
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