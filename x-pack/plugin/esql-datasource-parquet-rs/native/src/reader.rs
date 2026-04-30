use std::collections::BTreeSet;
use std::ptr;
use std::sync::Arc;

use super::ASYNC_RUNTIME;
use super::filter::{self, FilterExpr};
use super::jni_utils::extract_storage_config;
use arrow::array::{Array, StructArray};
use arrow::ffi;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use jni::EnvUnowned;
use jni::errors::{Error as JniError, Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong};
use super::store::{StorageConfig, resolve_store, needs_file_size_hint};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Type as PhysicalType;
use parquet::column::page::{Page, PageReader};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::errors::ParquetError;
use parquet::schema::types::SchemaDescriptor;

// ===== Parquet direct reader (no DataFusion) =====

fn parquet_root_column_index(parquet_schema: &SchemaDescriptor, name: &str) -> Option<usize> {
    (0..parquet_schema.num_columns()).find(|&i| parquet_schema.column(i).name().eq_ignore_ascii_case(name))
}

/// Resolves parquet root column indices for `ProjectionMask::roots`.
///
/// When `projected_cols` is absent, callers read every column (`None`: skip `with_projection`).
///
/// Otherwise we merge columns touched by the pushed filter: the parquet async decoder intersects
/// predicate cache masks with `self.projection`, so omitting filter-only roots can prevent predicate
/// columns from being decoded while evaluation falls back to overly permissive boolean masks (wrong counts).
fn projection_root_indices(
    parquet_schema: &SchemaDescriptor,
    projected_cols: &Option<Vec<String>>,
    filter: &Option<Arc<FilterExpr>>,
) -> JniResult<Option<Vec<usize>>> {
    fn union_filter_roots(
        parquet_schema: &SchemaDescriptor,
        filter_inner: &FilterExpr,
        roots: &mut BTreeSet<usize>,
    ) -> JniResult<()> {
        for name in filter::collect_columns(filter_inner) {
            let idx = parquet_root_column_index(parquet_schema, name.as_str()).ok_or_else(|| {
                err(format!("unknown parquet column [{name}] referenced in pushed native filter"))
            })?;
            roots.insert(idx);
        }
        Ok(())
    }

    match projected_cols {
        None => Ok(None),
        Some(cols) if cols.is_empty() => match filter {
            None => Ok(None),
            Some(f) => {
                let mut roots = BTreeSet::new();
                union_filter_roots(parquet_schema, f.as_ref(), &mut roots)?;
                Ok(Some(roots.into_iter().collect()))
            }
        },
        Some(cols) => {
            let mut roots = BTreeSet::new();
            for name in cols {
                let idx = parquet_root_column_index(parquet_schema, name.as_str()).ok_or_else(|| {
                    err(format!("unknown parquet column [{name}] referenced in projection"))
                })?;
                roots.insert(idx);
            }
            if let Some(f) = filter {
                union_filter_roots(parquet_schema, f.as_ref(), &mut roots)?;
            }
            Ok(Some(roots.into_iter().collect()))
        }
    }
}

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

/// Load Parquet footer metadata for a file, backed by a process-wide LRU cache with a
/// 30-second access-based TTL (mirrors Java's `FooterByteCache`).
///
/// The head request is always issued to obtain the file size used as the cache key —
/// `(path, file_size)` — so that an in-place file replacement invalidates the cached
/// entry even if the path is unchanged. For local files the head call is a stat syscall.
///
/// Returns an opaque handle that must be freed via `freeArrowMetadata`.
/// Pass it to `openReaderForRange` as `metaHandle` to skip the per-split footer fetch.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_loadArrowMetadata(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    config_json: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let file_path_str = file_path.try_to_string(env)?;
        let storage_config = extract_storage_config(env, &config_json)?;

        let handle = ASYNC_RUNTIME.block_on(async move {
            let inner = async {
                // Process-wide cache hit requires no network calls at all.
                if let Some(cached) = super::cache::get(&file_path_str) {
                    return Ok::<jlong, JniError>(Box::into_raw(Box::new(cached)) as jlong);
                }

                let (store, object_path) = resolve_store(&file_path_str, &storage_config).map_err(err)?;
                let file_size = store.head(&object_path).await.map_err(err)?.size as u64;
                let mut obj_reader = ParquetObjectReader::new(store, object_path)
                    .with_file_size(file_size);
                let arrow_meta = ArrowReaderMetadata::load_async(
                    &mut obj_reader,
                    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
                ).await.map_err(err)?;

                super::cache::insert(file_path_str, arrow_meta.clone());
                Ok::<jlong, JniError>(Box::into_raw(Box::new(arrow_meta)) as jlong)
            };
            tokio::time::timeout(std::time::Duration::from_secs(300), inner)
                .await
                .map_err(|_| err("Parquet metadata load timed out after 300s"))?
        })?;

        Ok(handle)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Free a metadata handle previously returned by `loadArrowMetadata`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_freeArrowMetadata(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
) {
    env.with_env(|_env| -> JniResult<()> {
        if handle != 0 {
            unsafe { drop(Box::from_raw(handle as *mut ArrowReaderMetadata)); }
        }
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

/// JNI entry point: open a reader for a byte range within a file.
/// Only row groups whose `(starting_offset + compressed_size / 2)` falls within
/// `[range_start, range_end)` are read (same rule as Java `ParquetFormatReader#filterBlocksByRange`).
/// Used by the range-aware split framework to parallelize row group decoding across drivers.
///
/// `meta_handle`: opaque handle from `loadArrowMetadata`, or 0 to fetch the footer inline.
/// When non-zero, the footer fetch is skipped — only column data is read from storage.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_openReaderForRange(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    projected_columns: JObjectArray,
    batch_size: jint,
    limit: jlong,
    filter_handle: jlong,
    config_json: JString,
    range_start: jlong,
    range_end: jlong,
    meta_handle: jlong,
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

        // Clone ArrowReaderMetadata from the cached handle (cheap: only Arc refcount bumps).
        let pre_loaded_meta: Option<ArrowReaderMetadata> = if meta_handle != 0 {
            Some(unsafe { &*(meta_handle as *const ArrowReaderMetadata) }.clone())
        } else {
            None
        };

        let (store, object_path) = resolve_store(&file_path_str, &storage_config).map_err(err)?;
        let needs_head = pre_loaded_meta.is_none() && needs_file_size_hint(&file_path_str);
        let concurrency = if file_path_str.starts_with("s3://") { s3_concurrency() } else { local_concurrency() };
        let kind = open_async_for_range(
            store, object_path, projected_cols, batch_size, limit, filter,
            concurrency, needs_head, range_start, range_end, pre_loaded_meta,
        )?;

        let remaining = if limit > 0 { Some(limit as usize) } else { None };
        let state = Box::new(ParquetReaderState { kind, remaining, plan });
        Ok(Box::into_raw(state) as jlong)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Like [`open_async`] but restricts reading to row groups whose split-assignment midpoint
/// (`starting_offset + compressed_size / 2`) falls within `[range_start, range_end)`.
///
/// When `pre_loaded_meta` is provided the footer fetch is skipped; the object reader is
/// used only for column-data range requests which do not require the file-size hint.
fn open_async_for_range(
    store: Arc<dyn ObjectStore>,
    object_path: object_store::path::Path,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
    needs_head: bool,
    range_start: i64,
    range_end: i64,
    pre_loaded_meta: Option<ArrowReaderMetadata>,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async move {
        let inner = async {
            let mut obj_reader = ParquetObjectReader::new(store.clone(), object_path.clone());
            if needs_head {
                let meta = store.head(&object_path).await.map_err(err)?;
                obj_reader = obj_reader.with_file_size(meta.size as u64);
            }

            let arrow_meta = match pre_loaded_meta {
                Some(meta) => meta,
                None => ArrowReaderMetadata::load_async(
                    &mut obj_reader,
                    ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
                ).await.map_err(err)?,
            };

            let pre_selected: Vec<usize> = {
                let metadata = arrow_meta.metadata();
                (0..metadata.num_row_groups())
                    .filter(|&i| {
                        let midpoint = super::metadata::row_group_range_assignment_midpoint(metadata.row_group(i));
                        midpoint >= range_start && midpoint < range_end
                    })
                    .collect()
            };

            let pre_selected = if let Some(ref expr) = filter {
                prune_by_dictionary(&store, &object_path, arrow_meta.metadata(), arrow_meta.parquet_schema(), pre_selected, expr).await
            } else {
                pre_selected
            };

            let n_workers = max_concurrency;
            let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

            spawn_file_workers(
                obj_reader, arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx, Some(pre_selected),
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

// ---------------------------------------------------------------------------
// Dictionary-based row group pruning
// ---------------------------------------------------------------------------

/// An offset-adjusting `ChunkReader` backed by a `Bytes` slice fetched from remote storage.
///
/// `SerializedPageReader` calls `get_bytes(absolute_file_offset, len)`. When we pre-fetch
/// only the dictionary page range `[base, base+data.len())`, all offsets from parquet's
/// perspective start at `base`. `SlicedFile` subtracts `base` to convert to local indices.
struct SlicedFile {
    data: Bytes,
    base: u64,
}

impl Length for SlicedFile {
    fn len(&self) -> u64 {
        self.base + self.data.len() as u64
    }
}

impl ChunkReader for SlicedFile {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let local = start.saturating_sub(self.base) as usize;
        Ok(self.data.slice(local..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let local = start.saturating_sub(self.base) as usize;
        Ok(self.data.slice(local..local + length))
    }
}

/// Returns true if `target` is present in the PLAIN-encoded dictionary buffer.
fn dict_contains_value(buf: &Bytes, num_values: usize, phys_type: PhysicalType, target: &filter::StatValue) -> bool {
    use filter::StatValue;
    match (phys_type, target) {
        (PhysicalType::INT64, StatValue::Long(v)) => {
            buf.chunks_exact(8).any(|c: &[u8]| i64::from_le_bytes(c.try_into().unwrap()) == *v)
        }
        (PhysicalType::INT32, StatValue::Int(v)) => {
            buf.chunks_exact(4).any(|c: &[u8]| i32::from_le_bytes(c.try_into().unwrap()) == *v)
        }
        (PhysicalType::INT32, StatValue::Long(v)) => {
            if *v < i32::MIN as i64 || *v > i32::MAX as i64 { return false; }
            let v32 = *v as i32;
            buf.chunks_exact(4).any(|c: &[u8]| i32::from_le_bytes(c.try_into().unwrap()) == v32)
        }
        (PhysicalType::BYTE_ARRAY, StatValue::Str(s)) => {
            let target = s.as_bytes();
            let data = buf.as_ref();
            let mut pos = 0usize;
            for _ in 0..num_values {
                if pos + 4 > data.len() { break; }
                let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + len > data.len() { break; }
                if &data[pos..pos + len] == target { return true; }
                pos += len;
            }
            false
        }
        _ => true // unknown combo → conservative, don't prune
    }
}

/// Prunes `candidates` by checking each row group's column dictionary for equality predicates.
///
/// For each AND-connected `Eq(col, lit)` / `InList(col, lits)` in the filter, fetches the
/// dictionary page bytes for that column in each candidate row group and scans them. If none
/// of the target values appear in the dictionary, the row group is removed from the result.
///
/// On any I/O or parse error the row group is kept (conservative). Only called when the filter
/// has at least one equality predicate on a dictionary-encoded column.
async fn prune_by_dictionary(
    store: &Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
    parquet_schema: &SchemaDescriptor,
    candidates: Vec<usize>,
    filter: &FilterExpr,
) -> Vec<usize> {
    let eq_preds = filter::collect_eq_predicates(filter);
    if eq_preds.is_empty() {
        return candidates;
    }

    // Resolve column indices once; skip predicates referencing unknown columns.
    let col_preds: Vec<(usize, Vec<filter::StatValue>)> = eq_preds
        .into_iter()
        .filter_map(|(name, vals)| {
            let idx = (0..parquet_schema.num_columns())
                .find(|&i| parquet_schema.column(i).name().eq_ignore_ascii_case(&name))?;
            Some((idx, vals))
        })
        .collect();

    if col_preds.is_empty() {
        return candidates;
    }

    let mut surviving = Vec::with_capacity(candidates.len());

    for rg_idx in candidates {
        let rg = metadata.row_group(rg_idx);
        let mut prune = false;

        'predicates: for (col_idx, values) in &col_preds {
            let col_meta = rg.column(*col_idx);

            let dict_start = match col_meta.dictionary_page_offset() {
                Some(o) if o > 0 => o as u64,
                _ => continue, // no dictionary page — can't prune via this predicate
            };
            let data_start = col_meta.data_page_offset() as u64;
            if data_start <= dict_start {
                continue;
            }

            // Fetch only the dictionary page bytes (a ranged GET, not the full column chunk).
            let bytes = match store.get_range(path, dict_start..data_start).await {
                Ok(b) => b,
                Err(_) => continue,
            };

            let sliced = Arc::new(SlicedFile { data: bytes, base: dict_start });
            let mut page_reader = match SerializedPageReader::new(
                sliced,
                col_meta,
                col_meta.num_values() as usize,
                None,
            ) {
                Ok(r) => r,
                Err(_) => continue,
            };

            if let Ok(Some(Page::DictionaryPage { buf, num_values, .. })) = page_reader.get_next_page() {
                let phys_type = col_meta.column_descr().physical_type();
                let found = values.iter().any(|v| dict_contains_value(&buf, num_values as usize, phys_type, v));
                if !found {
                    prune = true;
                    break 'predicates;
                }
            }
        }

        if !prune {
            surviving.push(rg_idx);
        }
    }

    surviving
}

/// Spawn worker tasks for a single file, sending decoded batches into `tx`.
/// Returns the number of surviving row groups (0 means the file was fully pruned).
///
/// When `pre_selected` is provided (range-aware reads), filter-based pruning is applied
/// only within that subset rather than across all row groups.
async fn spawn_file_workers(
    obj_reader: ParquetObjectReader,
    arrow_meta: ArrowReaderMetadata,
    projected_cols: &Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: &Option<Arc<FilterExpr>>,
    max_concurrency: usize,
    tx: &mpsc::Sender<Result<RecordBatch, ParquetError>>,
    pre_selected: Option<Vec<usize>>,
) -> Result<usize, JniError> {
    let metadata = arrow_meta.metadata().clone();
    let arrow_schema = arrow_meta.schema().clone();
    let parquet_schema = arrow_meta.parquet_schema().clone();

    let candidates: Vec<usize> = pre_selected
        .unwrap_or_else(|| (0..metadata.num_row_groups()).collect());

    let surviving: Vec<usize> = candidates
        .into_iter()
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

        builder = builder.with_row_groups(chunk.clone());

        if let Some(expr) = filter {
            let row_filter = filter::build_row_filter(expr, arrow_schema.clone(), &parquet_schema);
            builder = builder.with_row_filter(row_filter);
        }

        if let Some(roots) = projection_root_indices(&parquet_schema, projected_cols, filter)? {
            let mask = ProjectionMask::roots(&parquet_schema, roots);
            builder = builder.with_projection(mask);
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

            let candidates: Vec<usize> = (0..arrow_meta.metadata().num_row_groups()).collect();
            let candidates = if let Some(ref expr) = filter {
                prune_by_dictionary(&store, &object_path, arrow_meta.metadata(), arrow_meta.parquet_schema(), candidates, expr).await
            } else {
                candidates
            };

            let n_workers = max_concurrency;
            let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

            spawn_file_workers(
                obj_reader, arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx, Some(candidates),
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
                &filter, max_concurrency, &tx, None,
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
