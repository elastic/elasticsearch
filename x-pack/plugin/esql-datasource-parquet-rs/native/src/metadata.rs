use std::ptr;

use super::ASYNC_RUNTIME;
use super::filter::StatValue;
use super::jni_utils::{jni_err, extract_storage_config};
use super::store::{resolve_store, needs_file_size_hint};
use object_store::ObjectStoreExt;
use arrow::ffi;
use jni::{EnvUnowned, jni_str};
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;

use super::store::StorageConfig;

type MetadataError = Box<dyn std::error::Error + Send + Sync>;

fn load_metadata(
    file_path: &str,
    config: &StorageConfig,
) -> Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>, MetadataError> {
    let (store, object_path) = resolve_store(file_path, config)?;
    ASYNC_RUNTIME.block_on(async {
        let mut reader = ParquetObjectReader::new(store.clone(), object_path.clone());
        if needs_file_size_hint(file_path) {
            let meta = store.head(&object_path).await?;
            reader = reader.with_file_size(meta.size as u64);
        }
        let arrow_meta = ArrowReaderMetadata::load_async(
            &mut reader,
            ArrowReaderOptions::new(),
        )
        .await?;
        Ok(arrow_meta.metadata().clone())
    })
}

fn export_schema_ffi(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema_addr: jlong,
) -> Result<(), MetadataError> {
    let file_metadata = metadata.file_metadata();
    let arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let ffi_schema = ffi::FFI_ArrowSchema::try_from(&arrow_schema)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    unsafe {
        ptr::write(schema_addr as *mut ffi::FFI_ArrowSchema, ffi_schema);
    }
    Ok(())
}

struct FileStats {
    total_rows: i64,
    total_bytes: i64,
}

fn read_statistics_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> FileStats {
    let mut total_rows: i64 = 0;
    let mut total_bytes: i64 = 0;
    for rg in metadata.row_groups() {
        total_rows += rg.num_rows();
        total_bytes += rg.total_byte_size();
    }
    FileStats { total_rows, total_bytes }
}

struct ColumnStats {
    name: String,
    null_count: i64,
    min_value: Option<StatValue>,
    max_value: Option<StatValue>,
}

fn read_column_statistics_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<Vec<ColumnStats>, MetadataError> {
    let file_metadata = metadata.file_metadata();
    let schema = file_metadata.schema_descr();
    let num_columns = schema.num_columns();
    let num_row_groups = metadata.num_row_groups();

    let mut columns: Vec<ColumnStats> = Vec::with_capacity(num_columns);
    for col_idx in 0..num_columns {
        let col_desc = schema.column(col_idx);
        let col_name = col_desc.name().to_string();
        let mut null_count: i64 = 0;
        let mut global_min: Option<StatValue> = None;
        let mut global_max: Option<StatValue> = None;

        for rg_idx in 0..num_row_groups {
            let rg = metadata.row_group(rg_idx);
            let col_meta = rg.column(col_idx);

            if let Some(stats) = col_meta.statistics() {
                if let Some(nc) = stats.null_count_opt() {
                    null_count += nc as i64;
                }
                let (min_val, max_val) = extract_stats(stats);
                if let Some(v) = min_val {
                    global_min = Some(match global_min {
                        None => v,
                        Some(existing) => if v < existing { v } else { existing },
                    });
                }
                if let Some(v) = max_val {
                    global_max = Some(match global_max {
                        None => v,
                        Some(existing) => if v > existing { v } else { existing },
                    });
                }
            }
        }

        columns.push(ColumnStats { name: col_name, null_count, min_value: global_min, max_value: global_max });
    }

    Ok(columns)
}

fn extract_stats(stats: &parquet::file::statistics::Statistics) -> (Option<StatValue>, Option<StatValue>) {
    use parquet::file::statistics::Statistics::*;
    match stats {
        Int32(s) => (
            s.min_opt().map(|v| StatValue::Int(*v)),
            s.max_opt().map(|v| StatValue::Int(*v)),
        ),
        Int64(s) => (
            s.min_opt().map(|v| StatValue::Long(*v)),
            s.max_opt().map(|v| StatValue::Long(*v)),
        ),
        Float(s) => (
            s.min_opt().map(|v| StatValue::Double(*v as f64)),
            s.max_opt().map(|v| StatValue::Double(*v as f64)),
        ),
        Double(s) => (
            s.min_opt().map(|v| StatValue::Double(*v)),
            s.max_opt().map(|v| StatValue::Double(*v)),
        ),
        Boolean(s) => (
            s.min_opt().map(|v| StatValue::Bool(*v)),
            s.max_opt().map(|v| StatValue::Bool(*v)),
        ),
        ByteArray(s) => (
            s.min_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
            s.max_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
        ),
        FixedLenByteArray(s) => (
            s.min_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
            s.max_opt().map(|v| StatValue::Str(String::from_utf8_lossy(v.data()).into())),
        ),
        Int96(_) => (None, None),
    }
}

// ---------------------------------------------------------------------------
// JNI entry points
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getSchemaFFI(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    config_json: JString,
    schema_addr: jlong,
) {
    env.with_env(|env| -> JniResult<()> {
        let path = file_path.try_to_string(env)?;
        let config = extract_storage_config(env, &config_json)?;
        let metadata = load_metadata(&path, &config).map_err(jni_err)?;
        export_schema_ffi(&metadata, schema_addr).map_err(jni_err)?;
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getColumnStatistics<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    file_path: JString<'local>,
    config_json: JString<'local>,
) -> jni::objects::JObjectArray<'local> {
    env.with_env(|env| -> JniResult<jni::objects::JObjectArray<'local>> {
        let path = file_path.try_to_string(env)?;
        let config = extract_storage_config(env, &config_json)?;
        let metadata = load_metadata(&path, &config).map_err(jni_err)?;
        let columns = read_column_statistics_from_metadata(&metadata).map_err(jni_err)?;

        let string_class = env.find_class(jni_str!("java/lang/String"))?;
        let arr_len = (columns.len() * 4) as i32;
        let arr = env.new_object_array(arr_len, &string_class, &JObject::null())?;

        for (i, col) in columns.iter().enumerate() {
            let base = i * 4;
            let name = env.new_string(&col.name)?;
            let null_count = env.new_string(col.null_count.to_string())?;
            let min_str = col.min_value.as_ref().map(|v| v.to_string()).unwrap_or_default();
            let max_str = col.max_value.as_ref().map(|v| v.to_string()).unwrap_or_default();
            let min = env.new_string(&min_str)?;
            let max = env.new_string(&max_str)?;
            arr.set_element(env, base, &name)?;
            arr.set_element(env, base + 1, &null_count)?;
            arr.set_element(env, base + 2, &min)?;
            arr.set_element(env, base + 3, &max)?;
        }

        Ok(arr)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getStatistics<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    file_path: JString<'local>,
    config_json: JString<'local>,
) -> jni::sys::jlongArray {
    env.with_env(|env| -> JniResult<jni::sys::jlongArray> {
        let path = file_path.try_to_string(env)?;
        let config = extract_storage_config(env, &config_json)?;
        let metadata = load_metadata(&path, &config).map_err(jni_err)?;
        let stats = read_statistics_from_metadata(&metadata);

        let arr = env.new_long_array(2)?;
        let buf: [i64; 2] = [stats.total_rows, stats.total_bytes];
        arr.set_region(env, 0, &buf)?;
        Ok(arr.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
