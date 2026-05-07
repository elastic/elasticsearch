use std::collections::{BTreeSet, HashSet};
use std::ptr;
use std::sync::Arc;

use super::ASYNC_RUNTIME;
use super::filter::{self, FilterExpr};
use super::jni_utils::extract_storage_config;
use arrow::array::{Array, BooleanArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use tokio::sync::mpsc;
use jni::EnvUnowned;
use jni::errors::{Error as JniError, Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JLongArray, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong};
use super::store::{StorageConfig, resolve_store, needs_file_size_hint};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector};
use parquet::basic::Encoding;
use parquet::basic::Type as PhysicalType;
use parquet::column::page::{Page, PageReader};
use parquet::file::metadata::PageIndexPolicy;

/// Build `ArrowReaderOptions` with page-index policy chosen based on whether a
/// filter will be pushed down. The page index parse adds a few hundred microseconds
/// to a few milliseconds per file (proportional to total page count); it's only
/// worth paying when we'll use it for column-index page pruning. With no filter
/// we'd parse it and never read it, which is pure overhead on full-scan queries.
fn meta_options_for(filter_present: bool) -> ArrowReaderOptions {
    let policy = if filter_present {
        // `Optional` rather than `Required`: pruning code already handles
        // `column_index() == None` gracefully, so we shouldn't error on legacy
        // files that lack a page index.
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    ArrowReaderOptions::new().with_page_index_policy(policy)
}

/// Returns the names of `BYTE_ARRAY` (Utf8/Binary) columns whose every chunk across every row
/// group is fully dictionary-encoded for its data pages.
///
/// We promote a column to `Dictionary(Int32, ...)` in [`dict_preserving_schema`] only when this
/// returns its name. Columns that include even one PLAIN-encoded data page are left unwrapped:
/// promoting them forces `parquet-rs` to synthesize a dictionary on the fly, which on
/// high-cardinality columns produces a near-N-entry dictionary plus an N-int indices vector and
/// then drags `OrdinalBytesRefBlock` indirection through every downstream operator. That is the
/// regression seen on ClickBench's `SearchPhrase`/`ClientIP`/`MobilePhoneModel` columns, where
/// `parquet-mr` falls back to PLAIN once its dictionary page overflows.
///
/// The chunk-level check follows the recipe documented on
/// [`ColumnChunkMetaData::page_encoding_stats_mask`]: a chunk is fully dict-encoded iff a
/// dictionary page is present **and** the data-page encodings mask contains only
/// `RLE_DICTIONARY` or only `PLAIN_DICTIONARY`. The plain `encodings_mask()` cannot be used
/// directly because the dictionary page itself is encoded as `PLAIN`, so a fallback chunk and a
/// pure-dict chunk both end up with `{PLAIN, RLE_DICTIONARY}` set.
///
/// Page encoding stats are written by modern parquet writers but may be absent in legacy files;
/// in that case we conservatively treat the chunk as not-fully-dict (no promotion) to avoid the
/// synthesis cost on something we cannot verify.
///
/// The decision is per-file (which is the granularity `ArrowReaderOptions::with_schema` allows)
/// and conservative. `parquet-mr`'s overflow pattern is effectively per-column-per-file — once
/// one chunk overflows, subsequent chunks for that column are written as PLAIN — so the
/// all-or-nothing signal mirrors what the Java reader achieves implicitly via its per-chunk
/// `dictionary != null` check at `PageColumnReader.java:945`.
fn dict_promotable_columns(metadata: &parquet::file::metadata::ParquetMetaData) -> HashSet<String> {
    let mut promotable = HashSet::new();
    let parquet_schema = metadata.file_metadata().schema_descr();
    let num_row_groups = metadata.num_row_groups();
    if num_row_groups == 0 {
        return promotable;
    }
    for col_idx in 0..parquet_schema.num_columns() {
        let col_desc = parquet_schema.column(col_idx);
        if col_desc.physical_type() != PhysicalType::BYTE_ARRAY {
            continue;
        }
        let mut all_dict = true;
        for rg_idx in 0..num_row_groups {
            let chunk = metadata.row_group(rg_idx).column(col_idx);
            if chunk_fully_dict_encoded(chunk) == false {
                all_dict = false;
                break;
            }
        }
        if all_dict {
            promotable.insert(col_desc.name().to_string());
        }
    }
    promotable
}

/// True iff the chunk has a dictionary page **and** every data page used a dictionary encoding
/// (`RLE_DICTIONARY` or `PLAIN_DICTIONARY`). Returns false when page encoding stats are missing
/// (legacy files), which is the conservative default — we don't promote what we can't verify.
fn chunk_fully_dict_encoded(chunk: &parquet::file::metadata::ColumnChunkMetaData) -> bool {
    if chunk.dictionary_page_offset().is_none() {
        return false;
    }
    match chunk.page_encoding_stats_mask() {
        Some(mask) => mask.is_only(Encoding::RLE_DICTIONARY) || mask.is_only(Encoding::PLAIN_DICTIONARY),
        None => false,
    }
}

/// Returns an Arrow schema where every `Utf8`/`Binary` column whose name is in `promotable`
/// is rewritten as `Dictionary(Int32, Utf8|Binary)`. Supplied to
/// `ArrowReaderOptions::with_schema` it instructs `parquet-rs` to keep dictionary-encoded
/// `BYTE_ARRAY` pages in their dictionary form (`make_byte_array_dictionary_reader`) instead of
/// materialising the values into a flat `VarChar`/`VarBinary` vector. Other columns (and
/// non-promotable string columns) are passed through unchanged.
fn dict_preserving_schema(arrow_schema: &Schema, promotable: &HashSet<String>) -> SchemaRef {
    let fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            if promotable.contains(f.name()) == false {
                return (**f).clone();
            }
            match f.data_type() {
                DataType::Utf8 => Field::new(
                    f.name(),
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    f.is_nullable(),
                )
                .with_metadata(f.metadata().clone()),
                DataType::Binary => Field::new(
                    f.name(),
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
                    f.is_nullable(),
                )
                .with_metadata(f.metadata().clone()),
                _ => (**f).clone(),
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, arrow_schema.metadata().clone()))
}

/// Loads `ArrowReaderMetadata` and, for columns whose chunks all used dictionary encoding,
/// rewrites the cached Arrow schema via [`dict_preserving_schema`] so the data reader emits
/// `DictionaryArray`s for those columns (preserving Parquet's dictionary encoding through to the
/// FFI boundary). When no column qualifies the second `try_new` is skipped entirely.
///
/// The page-index policy on `base_opts` is honoured by the initial `load_async`; the second
/// `try_new` only swaps the schema, the `ParquetMetaData` (including any loaded page indexes) is
/// reused as-is.
pub(crate) async fn load_arrow_meta_dict_promoted<R>(
    reader: &mut R,
    base_opts: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata, ParquetError>
where
    R: AsyncFileReader,
{
    let arrow_meta = ArrowReaderMetadata::load_async(reader, base_opts.clone()).await?;
    let promotable = dict_promotable_columns(arrow_meta.metadata());
    if promotable.is_empty() {
        return Ok(arrow_meta);
    }
    let dict_schema = dict_preserving_schema(arrow_meta.schema(), &promotable);
    ArrowReaderMetadata::try_new(arrow_meta.metadata().clone(), base_opts.with_schema(dict_schema))
}
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use super::coalescing::CoalescingReader;
use parquet::errors::ParquetError;
use parquet::schema::types::SchemaDescriptor;

/// How long a single worker is allowed to go without yielding a row group from
/// S3 / disk before we treat it as a hang and surface an error to the consumer.
/// 300 s matches the existing setup timeout and covers even very large row groups
/// on a degraded S3 link.
const WORKER_STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

// ===== Parquet direct reader (no DataFusion) =====

fn parquet_root_column_index(parquet_schema: &SchemaDescriptor, name: &str) -> Option<usize> {
    (0..parquet_schema.num_columns()).find(|&i| parquet_schema.column(i).name().eq_ignore_ascii_case(name))
}

/// Resolves parquet root column indices for `ProjectionMask::roots`.
///
/// When `projected_cols` is absent, callers read every column (`None`: skip `with_projection`).
///
/// When `projected_cols` is `Some(empty)` and a filter is present, only the filter columns are
/// projected so the `RowFilter` predicate can run (used by COUNT-only queries).
///
/// When `projected_cols` is `Some(cols)` with actual output columns, only those columns are
/// projected. Filter-only columns are intentionally excluded: parquet-rs (≥ 51.0.0) evaluates
/// `RowFilter` predicates against a separate column batch that is independent of the main
/// projection, so there is no need to include filter columns in the output projection.
/// Excluding them enables late materialisation: the filter predicate runs on its narrow column
/// set, the resulting row selection is applied before decoding the wider output columns, and
/// pages of output columns that contain no passing rows are skipped entirely.
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
                    err(format!("unknown parquet column [{name}] referenced in projection. Known columns {}", parquet_schema.columns().iter().map(|c| c.name()).collect::<Vec<_>>().join(", ")))
                })?;
                roots.insert(idx);
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
        let arrow_meta = ASYNC_RUNTIME.block_on(
            fetch_and_cache_metadata(&file_path_str, &storage_config)
        ).map_err(err)?;
        let kind = open_async(store, object_path, projected_cols, batch_size, limit, filter, concurrency, arrow_meta)?;

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
    split_offsets: JLongArray<'_>,
    split_lengths: JLongArray<'_>,
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

        let mut offsets = vec![0i64; num_files];
        let mut lengths = vec![0i64; num_files];
        split_offsets.get_region(env, 0, &mut offsets)?;
        split_lengths.get_region(env, 0, &mut lengths)?;

        let file_ranges: Vec<(String, i64, i64)> = paths
            .into_iter()
            .zip(offsets)
            .zip(lengths)
            .map(|((path, off), len)| (path, off, len))
            .collect();

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

        let plan_paths = file_ranges.iter().map(|(p, _, _)| p.as_str()).collect::<Vec<_>>().join(", ");
        let plan = build_plan_string(&plan_paths, &projected_cols, batch_size, limit, &filter);

        let kind = open_multi(&file_ranges, &storage_config, projected_cols, batch_size, limit, filter, s3_concurrency())?;

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
/// Fetches `ArrowReaderMetadata` for one file, inserts it into the process-wide cache,
/// and returns it. On a cache hit no network call is made.
///
/// Used by `loadArrowMetadata`.
async fn fetch_and_cache_metadata(
    file_path: &str,
    config: &StorageConfig,
) -> Result<ArrowReaderMetadata, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(cached) = super::cache::get(file_path) {
        return Ok(cached);
    }
    let (store, object_path) = resolve_store(file_path, config)?;
    let mut obj_reader = ParquetObjectReader::new(store.clone(), object_path.clone());
    if needs_file_size_hint(file_path) {
        let file_size = store.head(&object_path).await?.size as u64;
        obj_reader = obj_reader.with_file_size(file_size);
    }
    // Cached metadata is reused across queries with different (or no) filters.
    // Use `Optional`: parse the page index when present so filter-pushdown can use it,
    // without erroring on legacy files that lack a page index.
    let arrow_meta = load_arrow_meta_dict_promoted(
        &mut obj_reader,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
    ).await?;
    super::cache::insert(file_path.to_string(), arrow_meta.clone());
    Ok(arrow_meta)
}

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
                let arrow_meta = fetch_and_cache_metadata(&file_path_str, &storage_config)
                    .await
                    .map_err(err)?;
                Ok::<jlong, JniError>(Box::into_raw(Box::new(arrow_meta)) as jlong)
            };
            tokio::time::timeout(std::time::Duration::from_secs(300), inner)
                .await
                .map_err(|_| {
                    log::error!(target: "esql_parquet_rs::reader", "Parquet metadata load timed out after 300s");
                    err("Parquet metadata load timed out after 300s")
                })?
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
                None => load_arrow_meta_dict_promoted(
                    &mut obj_reader,
                    meta_options_for(filter.is_some()),
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
                prune_by_dictionary(&store, &object_path, arrow_meta.metadata(), arrow_meta.parquet_schema(), pre_selected, expr, max_concurrency).await
            } else {
                pre_selected
            };

            let n_workers = max_concurrency;
            let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

            spawn_file_workers(
                CoalescingReader::new(obj_reader), arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx, Some(pre_selected),
            ).await?;

            drop(tx);
            Ok::<_, JniError>(rx)
        };

        tokio::time::timeout(std::time::Duration::from_secs(300), inner)
            .await
            .map_err(|_| {
                log::error!(target: "esql_parquet_rs::reader", "Parquet async open (range) timed out after 300s");
                err("Parquet async open timed out after 300s")
            })?
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
        let local = start.checked_sub(self.base)
            .ok_or_else(|| ParquetError::General(format!(
                "SlicedFile: read at {start} is before base {}", self.base
            )))? as usize;
        Ok(self.data.slice(local..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let local = start.checked_sub(self.base)
            .ok_or_else(|| ParquetError::General(format!(
                "SlicedFile: read at {start} is before base {}", self.base
            )))? as usize;
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
        // Unknown (physical_type, literal_type) combination — conservative: keep the
        // row group rather than risk silently dropping matching rows. This branch
        // permanently disables dict pruning for that combo, so we emit a one-time
        // warning per unique combo so the gap surfaces in logs without spamming the
        // hot path (called once per RG × predicate column × value).
        _ => {
            warn_once_unhandled_dict_combo(phys_type, target);
            true
        }
    }
}

/// Emits a single `warn!` per unique (physical_type, StatValue variant) combination
/// encountered by `dict_contains_value`. Subsequent calls with the same combo are no-ops
/// (one HashSet lookup under a Mutex). Designed for use on a hot pruning path.
fn warn_once_unhandled_dict_combo(phys_type: PhysicalType, target: &filter::StatValue) {
    use std::collections::HashSet;
    use std::sync::{LazyLock, Mutex};

    static SEEN: LazyLock<Mutex<HashSet<(PhysicalType, &'static str)>>> =
        LazyLock::new(|| Mutex::new(HashSet::new()));

    let variant: &'static str = match target {
        filter::StatValue::Int(_)    => "Int",
        filter::StatValue::Long(_)   => "Long",
        filter::StatValue::Double(_) => "Double",
        filter::StatValue::Bool(_)   => "Bool",
        filter::StatValue::Str(_)    => "Str",
    };

    let mut guard = SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if guard.insert((phys_type, variant)) {
        log::warn!(
            target: "esql_parquet_rs::pruning",
            "dict_contains_value: no decoder for (physical_type={:?}, literal={}); \
             dictionary pruning permanently disabled for this combination — \
             row groups will be kept rather than pruned (correctness-safe). \
             Consider adding a decode branch in dict_contains_value.",
            phys_type, variant
        );
    }
}

/// Outcome of dictionary-based pruning for one row group.
struct RgPruneResult {
    rg_idx: usize,
    prune: bool,
    reason: Option<String>,
}

/// Prunes `candidates` by checking each row group's column dictionary for equality predicates.
///
/// For each AND-connected `Eq(col, lit)` / `InList(col, lits)` in the filter, fetches the
/// dictionary page bytes for that column in each candidate row group and scans them. If none
/// of the target values appear in the dictionary, the row group is removed from the result.
///
/// Per-row-group dictionary fetches run in parallel (bounded by `max_concurrency`)
/// so that for an 8-RG file we incur ~1 RTT of S3 latency instead of 8.
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
    max_concurrency: usize,
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

    let candidates_in = candidates.len();

    // Per-RG futures are independent: each fetches its own dict pages for the
    // relevant columns, then scans them. Within an RG predicates are still
    // evaluated sequentially so we can short-circuit on the first dict miss
    // (avoiding a useless second range fetch).
    let results: Vec<RgPruneResult> = futures::stream::iter(candidates.into_iter())
        .map(|rg_idx| {
            let store = Arc::clone(store);
            let path = path.clone();
            let col_preds = &col_preds;
            async move {
                evaluate_rg_dict_prune(&store, &path, metadata, rg_idx, col_preds).await
            }
        })
        .buffer_unordered(max_concurrency)
        .collect()
        .await;

    let mut surviving: Vec<usize> = results
        .into_iter()
        .filter_map(|r| {
            if r.prune {
                if let Some(reason) = r.reason {
                    log::debug!(
                        target: "esql_parquet_rs::pruning",
                        "rg={} PRUNED by dictionary: {}", r.rg_idx, reason
                    );
                }
                None
            } else {
                Some(r.rg_idx)
            }
        })
        .collect();
    // `buffer_unordered` returns futures in completion order; downstream
    // chunking and logging benefit from a deterministic ascending order.
    surviving.sort_unstable();

    if log::log_enabled!(log::Level::Debug) {
        log::debug!(
            target: "esql_parquet_rs::pruning",
            "prune_by_dictionary: candidates_in={} surviving={} concurrency={} preds={:?}",
            candidates_in, surviving.len(), max_concurrency,
            col_preds.iter().map(|(i, vs)| format!("col_idx={} n_vals={}", i, vs.len())).collect::<Vec<_>>()
        );
    }

    surviving
}

/// Evaluates dictionary pruning for a single row group across all configured predicates.
/// Sequential within the row group so we can short-circuit on the first dict miss.
async fn evaluate_rg_dict_prune(
    store: &Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
    rg_idx: usize,
    col_preds: &[(usize, Vec<filter::StatValue>)],
) -> RgPruneResult {
    let rg = metadata.row_group(rg_idx);

    for (col_idx, values) in col_preds {
        let col_meta = rg.column(*col_idx);
        let col_name = col_meta.column_descr().name().to_string();
        let phys_type = col_meta.column_descr().physical_type();
        let encodings_vec: Vec<parquet::basic::Encoding> = col_meta.encodings().collect();

        // Dictionary-only pruning is only sound when EVERY data page in the
        // column chunk is dictionary-encoded. parquet writers (parquet-mr,
        // parquet-rs) fall back to a raw-value encoding (DELTA_BINARY_PACKED,
        // DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, or
        // PLAIN for v1) once the dictionary grows past a size threshold; the
        // fallback values are NOT inserted into the dictionary, so a "dict
        // miss" would silently drop matching rows. Detect fallback by the
        // presence of any non-dictionary value encoding in the column chunk.
        //
        // Allowed encodings when dict-pruning is sound:
        //   - PLAIN, PLAIN_DICTIONARY: dictionary page format (legacy + v2 spelling differ)
        //   - RLE_DICTIONARY:           data pages encode dictionary indices
        //   - RLE, BIT_PACKED:          definition / repetition level encodings
        // Any other encoding implies dict fallback occurred.
        use parquet::basic::Encoding::*;
        // BIT_PACKED is deprecated in parquet-rs but legitimately appears as a
        // definition/repetition level encoding in legacy parquet v1 files; treat
        // it as safe (it never carries values).
        #[allow(deprecated)]
        let has_fallback = encodings_vec.iter().any(|e| !matches!(
            e,
            PLAIN | PLAIN_DICTIONARY | RLE_DICTIONARY | RLE | BIT_PACKED
        ));
        if has_fallback {
            if log::log_enabled!(log::Level::Trace) {
                log::trace!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} dict-prune SKIPPED: dict fallback detected (encodings={:?})",
                    rg_idx, col_name,
                    encodings_vec.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>()
                );
            }
            continue;
        }

        let dict_start = match col_meta.dictionary_page_offset() {
            Some(o) if o > 0 => o as u64,
            other => {
                log::trace!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} no dict_page_offset ({:?}), skipping dict prune",
                    rg_idx, col_name, other
                );
                continue;
            }
        };
        let data_start = col_meta.data_page_offset() as u64;
        if data_start <= dict_start {
            log::trace!(
                target: "esql_parquet_rs::pruning",
                "rg={} col={} data_start({}) <= dict_start({}), skipping",
                rg_idx, col_name, data_start, dict_start
            );
            continue;
        }

        let bytes = match store.get_range(path, dict_start..data_start).await {
            Ok(b) => b,
            Err(e) => {
                log::debug!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} dict get_range failed: {}", rg_idx, col_name, e
                );
                continue;
            }
        };
        let dict_bytes_len = bytes.len();

        let sliced = Arc::new(SlicedFile { data: bytes, base: dict_start });
        let mut page_reader = match SerializedPageReader::new(
            sliced,
            col_meta,
            col_meta.num_values() as usize,
            None,
        ) {
            Ok(r) => r,
            Err(e) => {
                log::debug!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} SerializedPageReader::new failed: {}", rg_idx, col_name, e
                );
                continue;
            }
        };

        match page_reader.get_next_page() {
            Ok(Some(Page::DictionaryPage { buf, num_values, .. })) => {
                let found = values.iter().any(|v| dict_contains_value(&buf, num_values as usize, phys_type, v));
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        target: "esql_parquet_rs::pruning",
                        "rg={} col={} dict page: phys={:?} num_values={} buf_len={} fetched_bytes={} target_count={} found={}",
                        rg_idx, col_name, phys_type, num_values, buf.len(), dict_bytes_len, values.len(), found
                    );
                }
                if !found {
                    return RgPruneResult {
                        rg_idx,
                        prune: true,
                        reason: Some(format!(
                            "dict miss col={} phys={:?} num_values={} buf_len={}",
                            col_name, phys_type, num_values, buf.len()
                        )),
                    };
                }
            }
            Ok(Some(other)) => {
                log::debug!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} first page is not a DictionaryPage: {:?} — skipping prune",
                    rg_idx, col_name, page_kind(&other)
                );
            }
            Ok(None) => {
                log::debug!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} no page returned by reader", rg_idx, col_name
                );
            }
            Err(e) => {
                log::debug!(
                    target: "esql_parquet_rs::pruning",
                    "rg={} col={} get_next_page error: {}", rg_idx, col_name, e
                );
            }
        }
    }

    RgPruneResult { rg_idx, prune: false, reason: None }
}

fn page_kind(p: &Page) -> &'static str {
    match p {
        Page::DataPage { .. } => "DataPage",
        Page::DataPageV2 { .. } => "DataPageV2",
        Page::DictionaryPage { .. } => "DictionaryPage",
    }
}

/// Converts a `BooleanArray` (predicate result) into `Vec<RowSelector>` for use with
/// `RowSelection`. Null values are treated as `false` (row does not pass).
/// Returns the selectors and whether any row passed (has a `true` in the mask).

fn bool_array_to_selectors(mask: &BooleanArray) -> (Vec<RowSelector>, bool) {
    let n = mask.len();
    if n == 0 {
        return (vec![], false);
    }
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut run_start = 0usize;
    let first = mask.is_valid(0) && mask.value(0);
    let mut run_val = first;
    let mut any_passing = first;
    for i in 1..n {
        let v = mask.is_valid(i) && mask.value(i);
        any_passing |= v;
        if v != run_val {
            let count = i - run_start;
            selectors.push(if run_val { RowSelector::select(count) } else { RowSelector::skip(count) });
            run_start = i;
            run_val = v;
        }
    }
    let count = n - run_start;
    selectors.push(if run_val { RowSelector::select(count) } else { RowSelector::skip(count) });
    (selectors, any_passing)
}

/// Composes two `RowSelector` lists into one absolute selection.
///
/// `outer` covers all rows in a chunk (mix of skip/select, e.g. from column-index stats).
/// `inner` covers only the rows *selected* by `outer` (phase-1 predicate results).
/// The result selects rows that pass both: outer-skipped rows remain skipped, and within
/// outer-selected rows the inner skip/select is applied.
fn compose_row_selections(outer: Vec<RowSelector>, inner: Vec<RowSelector>) -> Vec<RowSelector> {
    let mut result: Vec<RowSelector> = Vec::new();
    let mut inner_iter = inner.into_iter();
    let mut inner_rem: usize = 0;
    let mut inner_skip: bool = false;

    for outer_sel in outer {
        if outer_sel.skip {
            match result.last_mut() {
                Some(last) if last.skip => last.row_count += outer_sel.row_count,
                _ => result.push(RowSelector::skip(outer_sel.row_count)),
            }
        } else {
            let mut rows_left = outer_sel.row_count;
            while rows_left > 0 {
                if inner_rem == 0 {
                    match inner_iter.next() {
                        Some(s) => { inner_rem = s.row_count; inner_skip = s.skip; }
                        None => break,
                    }
                }
                let take = rows_left.min(inner_rem);
                match result.last_mut() {
                    Some(last) if last.skip == inner_skip => last.row_count += take,
                    _ => result.push(if inner_skip { RowSelector::skip(take) } else { RowSelector::select(take) }),
                }
                inner_rem -= take;
                rows_left -= take;
            }
        }
    }
    result
}

/// Spawn worker tasks for a single file, sending decoded batches into `tx`.
/// Returns the number of surviving row groups (0 means the file was fully pruned).
///
/// When `pre_selected` is provided (range-aware reads), filter-based pruning is applied
/// only within that subset rather than across all row groups.
async fn spawn_file_workers<R: AsyncFileReader + Clone + Send + Unpin + 'static>(
    obj_reader: R,
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
    let parquet_schema = arrow_meta.parquet_schema().clone();

    let candidates: Vec<usize> = pre_selected
        .unwrap_or_else(|| (0..metadata.num_row_groups()).collect());

    let candidates_in = candidates.len();
    let surviving: Vec<usize> = candidates
        .into_iter()
        .filter(|&rg_idx| {
            if let Some(expr) = filter {
                let keep = filter::row_group_matches(expr, metadata.row_group(rg_idx), &parquet_schema);
                if !keep {
                    log::trace!(
                        target: "esql_parquet_rs::pruning",
                        "rg={} pruned by row-group stats (filter={})",
                        rg_idx, expr
                    );
                }
                keep
            } else {
                true
            }
        })
        .collect();

    log::debug!(
        target: "esql_parquet_rs::reader",
        "spawn_file_workers: total_rgs={} candidates={} surviving={} batch_size={} limit={} max_concurrency={} filter={} projected={:?}",
        metadata.num_row_groups(), candidates_in, surviving.len(),
        batch_size, limit, max_concurrency,
        filter.as_ref().map(|f| f.to_string()).unwrap_or_else(|| "<none>".to_string()),
        projected_cols
    );

    if surviving.is_empty() {
        return Ok(0);
    }

    let n_workers = max_concurrency.min(surviving.len()).max(1);
    let chunks: Vec<Vec<usize>> = (0..n_workers)
        .map(|w| surviving.iter().skip(w).step_by(n_workers).copied().collect())
        .filter(|c: &Vec<usize>| !c.is_empty())
        .collect();

    for chunk in chunks {
        // When every row group in the chunk is guaranteed to satisfy the filter from
        // statistics alone, suppress the filter entirely so phase-1 is skipped.
        let chunk_trivially_passes = filter.as_ref().map_or(false, |expr| {
            chunk.iter().all(|&rg_idx| {
                filter::row_group_trivially_passes(expr, metadata.row_group(rg_idx), &parquet_schema)
            })
        });
        let effective_filter: Option<Arc<FilterExpr>> = if chunk_trivially_passes {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    target: "esql_parquet_rs::reader",
                    "chunk_rgs={:?} trivially_passes=true: skipping filter",
                    chunk
                );
            }
            None
        } else {
            filter.clone()
        };

        // Column-index page selection from statistics. Used in phase-1 to reduce the
        // number of filter-column pages fetched; the phase-1 predicate result then
        // supersedes this for phase-2 output column reads.
        let (col_index_selectors, any_col_index_skip) = {
            let mut selectors: Vec<RowSelector> = Vec::new();
            let mut any_skip = false;
            if let Some(ref expr) = effective_filter {
                let mut total_rows_in_chunk = 0usize;
                let mut total_rows_skipped = 0usize;
                for &rg_idx in &chunk {
                    let rg_rows = metadata.row_group(rg_idx).num_rows() as usize;
                    total_rows_in_chunk += rg_rows;
                    if let Some(rg_sel) = filter::row_group_page_selection(
                        expr, &metadata, &parquet_schema, rg_idx,
                    ) {
                        any_skip = true;
                        let rg_skipped: usize = rg_sel.iter()
                            .filter(|s| s.skip)
                            .map(|s| s.row_count)
                            .sum();
                        total_rows_skipped += rg_skipped;
                        selectors.extend(rg_sel);
                    } else {
                        selectors.push(RowSelector::select(rg_rows));
                    }
                }
                if any_skip {
                    log::debug!(
                        target: "esql_parquet_rs::reader",
                        "chunk_rgs={:?} batch_size={} limit={} filter={} page_pruning: rows_skipped={}/{} ({:.1}%)",
                        chunk, batch_size, limit, expr,
                        total_rows_skipped, total_rows_in_chunk,
                        if total_rows_in_chunk > 0 {
                            100.0 * total_rows_skipped as f64 / total_rows_in_chunk as f64
                        } else { 0.0 }
                    );
                } else {
                    log::debug!(
                        target: "esql_parquet_rs::reader",
                        "chunk_rgs={:?} batch_size={} limit={} filter={} page_pruning: no skips (rows={})",
                        chunk, batch_size, limit, expr, total_rows_in_chunk
                    );
                }
            }
            (selectors, any_skip)
        };

        // Decide whether to use two-phase reads or a single-pass with inline filtering.
        //
        // Two-phase is worthwhile when there are filter-only columns — columns referenced
        // by the predicate that are NOT needed in the output. Phase-1 reads those cheap
        // columns, builds an exact RowSelection, and phase-2 then skips output-column
        // pages whose rows were all filtered out. Classic example: CounterID (INT32,
        // tiny) filters rows so only a small fraction of URL/Title/Referer pages are read.
        //
        // When ALL filter columns are also output columns (e.g. WHERE URL != ""), reading
        // them in a separate phase would double the I/O for those large columns. Use
        // single-pass + inline row filtering instead.
        let has_filter_only_cols: bool = match (&effective_filter, projected_cols) {
            (None, _) | (_, None) => false,
            (Some(expr), Some(output_cols)) => {
                if output_cols.is_empty() {
                    // COUNT(*): no output columns at all — every filter column is filter-only.
                    !filter::collect_columns(expr).is_empty()
                } else {
                    let fc = filter::collect_columns(expr);
                    let out_set: std::collections::HashSet<&str> =
                        output_cols.iter().map(|s| s.as_str()).collect();
                    fc.iter().any(|c| !out_set.contains(c.as_str()))
                }
            }
        };

        // Phase-1 mask: filter columns only (only relevant for two-phase path).
        let filter_mask: Option<ProjectionMask> = if has_filter_only_cols {
            match &effective_filter {
                None => None,
                Some(expr) => {
                    projection_root_indices(&parquet_schema, &Some(vec![]), &Some(expr.clone()))?
                        .map(|roots| ProjectionMask::roots(&parquet_schema, roots))
                }
            }
        } else {
            None
        };

        // Output / single-pass projection mask.
        // COUNT(*) with a filter uses two-phase: phase-2 must return row counts without
        // fetching column data. An empty roots mask achieves this (0-field schema batches).
        let output_mask: Option<ProjectionMask> = match projected_cols {
            None => None,
            Some(cols) if cols.is_empty() => {
                Some(ProjectionMask::roots(&parquet_schema, vec![]))
            }
            Some(_) => projection_root_indices(&parquet_schema, projected_cols, &None)?
                .map(|roots| ProjectionMask::roots(&parquet_schema, roots)),
        };

        let obj_reader = obj_reader.clone();
        let arrow_meta = arrow_meta.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(async {
                if has_filter_only_cols {
                    let expr = effective_filter.as_ref().unwrap();
                    let fmask = filter_mask.unwrap();

                    // ── PHASE 1: read filter-only columns, evaluate predicate ─────────────
                    let mut p1_builder =
                        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                            obj_reader.clone(),
                            arrow_meta.clone(),
                        )
                        .with_batch_size(batch_size as usize)
                        .with_row_groups(chunk.clone())
                        .with_projection(fmask);
                    if any_col_index_skip {
                        p1_builder = p1_builder
                            .with_row_selection(RowSelection::from(col_index_selectors.clone()));
                    }
                    let mut p1_stream = match p1_builder.build() {
                        Ok(s) => s,
                        Err(e) => { let _ = tx.send(Err(e)).await; return; }
                    };

                    let mut p1_selectors: Vec<RowSelector> = Vec::new();
                    let mut any_passing = false;
                    loop {
                        let rg = match tokio::time::timeout(
                            WORKER_STALL_TIMEOUT,
                            p1_stream.next_row_group(),
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(_) => {
                                let msg = format!(
                                    "parquet worker stalled in phase-1 after {}s",
                                    WORKER_STALL_TIMEOUT.as_secs()
                                );
                                log::error!(target: "esql_parquet_rs::reader", "{}", msg);
                                let _ = tx.send(Err(ParquetError::General(msg))).await;
                                return;
                            }
                        };
                        let rg_reader = match rg {
                            Ok(None) => break,
                            Ok(Some(r)) => r,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        };
                        for batch_result in rg_reader {
                            let batch = match batch_result {
                                Ok(b) => b,
                                Err(e) => { let _ = tx.send(Err(ParquetError::from(e))).await; return; }
                            };
                            let mask = match filter::evaluate_filter(expr, &batch) {
                                Ok(m) => m,
                                Err(e) => {
                                    let _ = tx.send(Err(ParquetError::General(e.to_string()))).await;
                                    return;
                                }
                            };
                            let (sels, has_pass) = bool_array_to_selectors(&mask);
                            p1_selectors.extend(sels);
                            any_passing |= has_pass;
                        }
                    }

                    if !any_passing {
                        log::debug!(
                            target: "esql_parquet_rs::reader",
                            "chunk_rgs={:?} filter={} phase-1: 0 passing rows",
                            chunk, expr
                        );
                        return;
                    }

                    // Compose col-index selection (absolute positions) with phase-1 results
                    // (relative to col-index-selected rows) to get the exact row selection
                    // for phase-2. When no col-index skipping occurred, p1_selectors already
                    // covers all rows in absolute terms.
                    let final_selectors = if any_col_index_skip {
                        compose_row_selections(col_index_selectors, p1_selectors)
                    } else {
                        p1_selectors
                    };
                    let final_selection = RowSelection::from(final_selectors);

                    log::debug!(
                        target: "esql_parquet_rs::reader",
                        "chunk_rgs={:?} filter={} phase-1 done, starting phase-2 with row selection",
                        chunk, expr
                    );

                    // ── PHASE 2: read output columns using phase-1 row selection ──────────
                    // Pages of output columns where all rows were filtered out are skipped
                    // by parquet-rs before any S3 fetch is issued for those pages.
                    let mut p2_builder =
                        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                            obj_reader,
                            arrow_meta,
                        )
                        .with_batch_size(batch_size as usize)
                        .with_row_groups(chunk.clone())
                        .with_row_selection(final_selection);
                    if limit > 0 {
                        p2_builder = p2_builder.with_limit(limit as usize);
                    }
                    if let Some(ref mask) = output_mask {
                        p2_builder = p2_builder.with_projection(mask.clone());
                    }
                    let mut p2_stream = match p2_builder.build() {
                        Ok(s) => s,
                        Err(e) => { let _ = tx.send(Err(e)).await; return; }
                    };

                    loop {
                        let rg_result = match tokio::time::timeout(
                            WORKER_STALL_TIMEOUT,
                            p2_stream.next_row_group(),
                        )
                        .await
                        {
                            Ok(inner) => inner,
                            Err(_elapsed) => {
                                let msg = format!(
                                    "parquet worker stalled: no row-group progress for {}s",
                                    WORKER_STALL_TIMEOUT.as_secs()
                                );
                                log::error!(target: "esql_parquet_rs::reader", "{}", msg);
                                let _ = tx.send(Err(ParquetError::General(msg))).await;
                                return;
                            }
                        };
                        let reader = match rg_result {
                            Ok(None) => break,
                            Ok(Some(r)) => r,
                            Err(e) => { let _ = tx.send(Err(e)).await; return; }
                        };
                        for batch in reader {
                            // Phase-2 reads with `with_row_selection`, which leaves
                            // dictionaries sparse for the same reason `filter_record_batch`
                            // does — see [`filter::compact_record_batch_dicts`].
                            let to_send = match batch.map_err(ParquetError::from) {
                                Ok(b) => filter::compact_record_batch_dicts(b)
                                    .map_err(|e| ParquetError::General(e.to_string())),
                                Err(e) => Err(e),
                            };
                            if tx.send(to_send).await.is_err() {
                                return;
                            }
                        }
                    }
                } else if let Some(ref expr) = effective_filter {
                    // ── SINGLE-PASS WITH INLINE FILTERING ────────────────────────────────
                    // All filter columns are also output columns (e.g. WHERE URL != "").
                    // Reading them separately in phase-1 would double the I/O for those
                    // large columns. Read output cols once and filter rows inline instead.
                    let mut builder =
                        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                            obj_reader,
                            arrow_meta,
                        )
                        .with_batch_size(batch_size as usize);
                    if limit > 0 {
                        builder = builder.with_limit(limit as usize);
                    }
                    builder = builder.with_row_groups(chunk.clone());
                    if any_col_index_skip {
                        builder = builder.with_row_selection(RowSelection::from(col_index_selectors));
                    }
                    if let Some(ref mask) = output_mask {
                        builder = builder.with_projection(mask.clone());
                    }
                    let mut stream = match builder.build() {
                        Ok(s) => s,
                        Err(e) => { let _ = tx.send(Err(e)).await; return; }
                    };

                    loop {
                        let rg_result = match tokio::time::timeout(
                            WORKER_STALL_TIMEOUT,
                            stream.next_row_group(),
                        )
                        .await
                        {
                            Ok(inner) => inner,
                            Err(_elapsed) => {
                                let msg = format!(
                                    "parquet worker stalled: no row-group progress for {}s",
                                    WORKER_STALL_TIMEOUT.as_secs()
                                );
                                log::error!(target: "esql_parquet_rs::reader", "{}", msg);
                                let _ = tx.send(Err(ParquetError::General(msg))).await;
                                return;
                            }
                        };
                        let reader = match rg_result {
                            Ok(None) => break,
                            Ok(Some(r)) => r,
                            Err(e) => { let _ = tx.send(Err(e)).await; return; }
                        };
                        for batch_result in reader {
                            let batch = match batch_result {
                                Ok(b) => b,
                                Err(e) => { let _ = tx.send(Err(ParquetError::from(e))).await; return; }
                            };
                            let row_mask = match filter::evaluate_filter(expr, &batch) {
                                Ok(m) => m,
                                Err(e) => {
                                    let _ = tx.send(Err(ParquetError::General(e.to_string()))).await;
                                    return;
                                }
                            };
                            let filtered =
                                match arrow::compute::filter_record_batch(&batch, &row_mask) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        let _ = tx
                                            .send(Err(ParquetError::General(e.to_string())))
                                            .await;
                                        return;
                                    }
                                };
                            // filter_record_batch keeps the original (now sparse) dictionary
                            // for any DictionaryArray columns; compact it before handing the
                            // batch to Java to avoid phantom hash-aggregation groups.
                            let filtered = match filter::compact_record_batch_dicts(filtered) {
                                Ok(b) => b,
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(ParquetError::General(e.to_string())))
                                        .await;
                                    return;
                                }
                            };
                            if filtered.num_rows() > 0 && tx.send(Ok(filtered)).await.is_err() {
                                return;
                            }
                        }
                    }

                } else {
                    // ── SINGLE PASS: no filter, or filter trivially satisfied ─────────────
                    let mut builder =
                        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_metadata(
                            obj_reader,
                            arrow_meta,
                        )
                        .with_batch_size(batch_size as usize);
                    if limit > 0 {
                        builder = builder.with_limit(limit as usize);
                    }
                    builder = builder.with_row_groups(chunk.clone());
                    if let Some(ref mask) = output_mask {
                        builder = builder.with_projection(mask.clone());
                    }
                    let mut stream = match builder.build() {
                        Ok(s) => s,
                        Err(e) => { let _ = tx.send(Err(e)).await; return; }
                    };

                    loop {
                        let rg_result = match tokio::time::timeout(
                            WORKER_STALL_TIMEOUT,
                            stream.next_row_group(),
                        )
                        .await
                        {
                            Ok(inner) => inner,
                            Err(_elapsed) => {
                                let msg = format!(
                                    "parquet worker stalled: no row-group progress for {}s",
                                    WORKER_STALL_TIMEOUT.as_secs()
                                );
                                log::error!(target: "esql_parquet_rs::reader", "{}", msg);
                                let _ = tx.send(Err(ParquetError::General(msg))).await;
                                return;
                            }
                        };
                        let reader = match rg_result {
                            Ok(None) => break,
                            Ok(Some(r)) => r,
                            Err(e) => { let _ = tx.send(Err(e)).await; return; }
                        };
                        for batch in reader {
                            if tx.send(batch.map_err(|e| ParquetError::from(e))).await.is_err() {
                                return;
                            }
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
/// Uses pre-loaded (and LRU-cached) `ArrowReaderMetadata` to skip the per-open footer
/// parse. Prunes row groups with the predicate's column statistics, then splits surviving
/// row groups into N chunks (N = `max_concurrency`). Each chunk gets its own
/// `ParquetRecordBatchStream` running as a spawned tokio task, with decoded batches sent
/// through an mpsc channel. Within each task, `next_row_group()` pipelines I/O and decode.
fn open_async(
    store: Arc<dyn ObjectStore>,
    object_path: object_store::path::Path,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
    arrow_meta: ArrowReaderMetadata,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async move {
        let inner = async {
            // Metadata is already parsed and cached by the caller; the object reader is
            // used only for column-data range requests (new_with_metadata never re-fetches
            // the footer), so no file-size hint is needed here.
            let obj_reader = ParquetObjectReader::new(store.clone(), object_path.clone());

            let candidates: Vec<usize> = (0..arrow_meta.metadata().num_row_groups()).collect();
            let candidates = if let Some(ref expr) = filter {
                prune_by_dictionary(&store, &object_path, arrow_meta.metadata(), arrow_meta.parquet_schema(), candidates, expr, max_concurrency).await
            } else {
                candidates
            };

            let n_workers = max_concurrency;
            let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

            spawn_file_workers(
                CoalescingReader::new(obj_reader), arrow_meta, &projected_cols, batch_size, limit,
                &filter, max_concurrency, &tx, Some(candidates),
            ).await?;

            drop(tx);
            Ok::<_, JniError>(rx)
        };

        tokio::time::timeout(std::time::Duration::from_secs(300), inner)
            .await
            .map_err(|_| {
                log::error!(target: "esql_parquet_rs::reader", "Parquet async open timed out after 300s");
                err("Parquet async open timed out after 300s")
            })?
    })?;

    Ok(ReaderKind::Async(rx))
}

/// Open multiple Parquet files with parallel metadata fetching.
///
/// For each file the metadata is checked against the process-wide LRU cache before
/// any network call is issued; on a cache hit only column-data range requests are
/// made. After loading metadata, `prune_by_dictionary` eliminates files whose
/// equality-predicate column dictionaries cannot match the filter. Both phases run
/// concurrently across all files in a single `buffer_unordered` pass, feeding a
/// single mpsc channel.
fn open_multi(
    file_ranges: &[(String, i64, i64)],
    config: &StorageConfig,
    projected_cols: Option<Vec<String>>,
    batch_size: jint,
    limit: jlong,
    filter: Option<Arc<FilterExpr>>,
    max_concurrency: usize,
) -> JniResult<ReaderKind> {
    let rx = ASYNC_RUNTIME.block_on(async {
        // Group by path so each unique file fetches its footer exactly once,
        // even when multiple splits reference the same file.
        // Use a Vec to preserve a stable per-file processing order.
        let mut unique_files: Vec<(String, Vec<(i64, i64)>)> = Vec::new();
        for (path, offset, length) in file_ranges {
            if let Some(entry) = unique_files.iter_mut().find(|(p, _)| p == path) {
                entry.1.push((*offset, *offset + *length));
            } else {
                unique_files.push((path.clone(), vec![(*offset, *offset + *length)]));
            }
        }

        let mut file_infos: Vec<(String, Vec<(i64, i64)>, Arc<dyn ObjectStore>, object_store::path::Path)> =
            Vec::with_capacity(unique_files.len());
        for (path_str, ranges) in unique_files {
            let (store, object_path) = resolve_store(&path_str, config).map_err(err)?;
            file_infos.push((path_str, ranges, store, object_path));
        }

        let needs_head = file_ranges.first().map_or(false, |(p, _, _)| needs_file_size_hint(p));
        let meta_options = meta_options_for(filter.is_some());

        // Per-file: cache check → metadata fetch on miss → dictionary pruning.
        // All files processed concurrently within max_concurrency.
        let file_stream = futures::stream::iter(file_infos.into_iter().map(|(path_str, ranges, store, path)| {
            let filter_clone = filter.clone();
            let opts = meta_options.clone();
            async move {
                let (reader, meta) = if let Some(cached) = super::cache::get(&path_str) {
                    // Cache hit: skip metadata fetch entirely, no network call needed.
                    (ParquetObjectReader::new(Arc::clone(&store), path.clone()), cached)
                } else {
                    let mut reader = ParquetObjectReader::new(Arc::clone(&store), path.clone());
                    if needs_head {
                        let head = store.head(&path).await.map_err(err)?;
                        reader = reader.with_file_size(head.size as u64);
                    }
                    let meta = load_arrow_meta_dict_promoted(&mut reader, opts)
                        .await
                        .map_err(err)?;
                    super::cache::insert(path_str, meta.clone());
                    (reader, meta)
                };

                // Select row groups whose midpoint falls within any of the claimed ranges.
                // Matches the Java-side coalescing assignment logic (same as open_async_for_range).
                let candidates: Vec<usize> = (0..meta.metadata().num_row_groups())
                    .filter(|&i| {
                        let mid = super::metadata::row_group_range_assignment_midpoint(
                            meta.metadata().row_group(i),
                        );
                        ranges.iter().any(|&(start, end)| mid >= start && mid < end)
                    })
                    .collect();

                let candidates = if let Some(ref expr) = filter_clone {
                    prune_by_dictionary(
                        &store, &path,
                        meta.metadata(), meta.parquet_schema(),
                        candidates, expr, max_concurrency,
                    )
                    .await
                } else {
                    candidates
                };

                Ok::<_, JniError>((reader, meta, candidates))
            }
        }))
        .buffer_unordered(max_concurrency);
        let all_files: Vec<_> = file_stream.try_collect().await?;

        let n_workers = max_concurrency;
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);

        // Spawn a background task that reads files one at a time in the data phase to guard
        // against over-concurrency. The metadata+pruning phase above is still fully parallel.
        // Processing sequentially here caps concurrent S3 data streams to max_concurrency,
        // the same ceiling as the single-file path.
        //
        // IMPORTANT: this must be a background task, NOT run inside block_on. The block_on
        // must return rx to the caller (Java), which then drains the channel via nextBatch().
        // Running the loop here would deadlock once tx fills up (Java never sees rx).
        ASYNC_RUNTIME.spawn(async move {
            for (obj_reader, arrow_meta, candidates) in all_files {
                if candidates.is_empty() {
                    continue; // fully pruned by dictionary
                }
                let (file_tx, mut file_rx) =
                    mpsc::channel::<Result<RecordBatch, ParquetError>>(n_workers * 2);
                match spawn_file_workers(
                    CoalescingReader::new(obj_reader), arrow_meta, &projected_cols, batch_size,
                    limit, &filter, max_concurrency, &file_tx, Some(candidates),
                )
                .await
                {
                    Err(e) => {
                        let _ = tx
                            .send(Err(ParquetError::External(Box::new(
                                std::io::Error::other(e.to_string()),
                            ))))
                            .await;
                        return;
                    }
                    Ok(_) => {}
                }
                drop(file_tx);
                while let Some(batch) = file_rx.recv().await {
                    if tx.send(batch).await.is_err() {
                        return; // consumer closed early (e.g. LIMIT reached)
                    }
                }
            }
            // tx dropped here, signalling end-of-stream to the Java consumer
        });

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
