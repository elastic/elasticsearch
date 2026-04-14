use super::jni_utils::*;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use jni::{EnvUnowned, jni_str};
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};

type MetadataError = Box<dyn std::error::Error + Send + Sync>;

pub const TYPE_BOOLEAN: jint = 1;
pub const TYPE_INT32: jint = 2;
pub const TYPE_INT64: jint = 3;
pub const TYPE_FLOAT32: jint = 4;
pub const TYPE_FLOAT64: jint = 5;
pub const TYPE_UTF8: jint = 6;
pub const TYPE_BINARY: jint = 7;
pub const TYPE_DATE32: jint = 8;
pub const TYPE_TIMESTAMP_MILLIS: jint = 9;
pub const TYPE_TIMESTAMP_MICROS: jint = 10;
pub const TYPE_TIMESTAMP_NANOS: jint = 11;
pub const TYPE_DECIMAL128: jint = 12;
pub const TYPE_LIST: jint = 13;
pub const TYPE_UNSUPPORTED: jint = -1;

fn arrow_type_to_id(dt: &ArrowDataType) -> jint {
    match dt {
        ArrowDataType::Boolean => TYPE_BOOLEAN,
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32
        | ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 => TYPE_INT32,
        ArrowDataType::Int64 | ArrowDataType::UInt64 => TYPE_INT64,
        ArrowDataType::Float16 | ArrowDataType::Float32 => TYPE_FLOAT32,
        ArrowDataType::Float64 => TYPE_FLOAT64,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => TYPE_UTF8,
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::FixedSizeBinary(_) => TYPE_BINARY,
        ArrowDataType::Date32 | ArrowDataType::Date64 => TYPE_DATE32,
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => TYPE_TIMESTAMP_MILLIS,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => TYPE_TIMESTAMP_MICROS,
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => TYPE_TIMESTAMP_NANOS,
        ArrowDataType::Timestamp(TimeUnit::Second, _) => TYPE_TIMESTAMP_MILLIS,
        ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => TYPE_DECIMAL128,
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element_type = arrow_type_to_id(field.data_type());
            if element_type == TYPE_UNSUPPORTED { TYPE_UNSUPPORTED } else { TYPE_LIST }
        }
        _ => TYPE_UNSUPPORTED,
    }
}

fn arrow_list_element_type_id(dt: &ArrowDataType) -> jint {
    match dt {
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            arrow_type_to_id(field.data_type())
        }
        _ => arrow_type_to_id(dt),
    }
}

struct SchemaEntry {
    name: String,
    type_id: jint,
    element_type_id: jint,
}

fn read_parquet_metadata(
    file_path: &str,
) -> Result<parquet::file::metadata::ParquetMetaData, MetadataError> {
    let file = std::fs::File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader.metadata().clone())
}

fn read_schema_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<Vec<SchemaEntry>, MetadataError> {
    let file_metadata = metadata.file_metadata();
    let arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let mut entries = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        entries.push(SchemaEntry {
            name: field.name().clone(),
            type_id: arrow_type_to_id(field.data_type()),
            element_type_id: arrow_list_element_type_id(field.data_type()),
        });
    }
    Ok(entries)
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
    min_value: Option<String>,
    max_value: Option<String>,
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
        let mut global_min: Option<String> = None;
        let mut global_max: Option<String> = None;

        for rg_idx in 0..num_row_groups {
            let rg = metadata.row_group(rg_idx);
            let col_meta = rg.column(col_idx);

            if let Some(stats) = col_meta.statistics() {
                if let Some(nc) = stats.null_count_opt() {
                    null_count += nc as i64;
                }
                let (min_str, max_str) = format_stats(stats);
                if let Some(min_s) = min_str {
                    global_min = match global_min {
                        None => Some(min_s),
                        Some(existing) => Some(pick_min(&existing, &min_s)),
                    };
                }
                if let Some(max_s) = max_str {
                    global_max = match global_max {
                        None => Some(max_s),
                        Some(existing) => Some(pick_max(&existing, &max_s)),
                    };
                }
            }
        }

        columns.push(ColumnStats { name: col_name, null_count, min_value: global_min, max_value: global_max });
    }

    Ok(columns)
}

fn format_stats(stats: &parquet::file::statistics::Statistics) -> (Option<String>, Option<String>) {
    use parquet::file::statistics::Statistics::*;
    match stats {
        Int32(s) => (s.min_opt().map(|v| v.to_string()), s.max_opt().map(|v| v.to_string())),
        Int64(s) => (s.min_opt().map(|v| v.to_string()), s.max_opt().map(|v| v.to_string())),
        Float(s) => (s.min_opt().map(|v| v.to_string()), s.max_opt().map(|v| v.to_string())),
        Double(s) => (s.min_opt().map(|v| v.to_string()), s.max_opt().map(|v| v.to_string())),
        Boolean(s) => (s.min_opt().map(|v| v.to_string()), s.max_opt().map(|v| v.to_string())),
        ByteArray(s) => (
            s.min_opt().map(|v| String::from_utf8_lossy(v.data()).to_string()),
            s.max_opt().map(|v| String::from_utf8_lossy(v.data()).to_string()),
        ),
        FixedLenByteArray(s) => (
            s.min_opt().map(|v| String::from_utf8_lossy(v.data()).to_string()),
            s.max_opt().map(|v| String::from_utf8_lossy(v.data()).to_string()),
        ),
        Int96(_) => (None, None),
    }
}

fn pick_min(a: &str, b: &str) -> String {
    if a <= b { a.to_string() } else { b.to_string() }
}

fn pick_max(a: &str, b: &str) -> String {
    if a >= b { a.to_string() } else { b.to_string() }
}

// ---------------------------------------------------------------------------
// JNI entry points
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getSchema<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    file_path: JString<'local>,
) -> jni::objects::JObjectArray<'local> {
    env.with_env(|env| -> JniResult<jni::objects::JObjectArray<'local>> {
        let path = file_path.try_to_string(env)?;
        let metadata = read_parquet_metadata(&path).map_err(jni_err)?;
        let entries = read_schema_from_metadata(&metadata).map_err(jni_err)?;

        let string_class = env.find_class(jni_str!("java/lang/String"))?;
        let arr_len = (entries.len() * 3) as i32;
        let arr = env.new_object_array(arr_len, &string_class, &JObject::null())?;

        for (i, entry) in entries.iter().enumerate() {
            let base = i * 3;
            let name = env.new_string(&entry.name)?;
            let type_str = env.new_string(entry.type_id.to_string())?;
            let elem_str = env.new_string(entry.element_type_id.to_string())?;
            arr.set_element(env, base, &name)?;
            arr.set_element(env, base + 1, &type_str)?;
            arr.set_element(env, base + 2, &elem_str)?;
        }

        Ok(arr)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_getColumnStatistics<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    file_path: JString<'local>,
) -> jni::objects::JObjectArray<'local> {
    env.with_env(|env| -> JniResult<jni::objects::JObjectArray<'local>> {
        let path = file_path.try_to_string(env)?;
        let metadata = read_parquet_metadata(&path).map_err(jni_err)?;
        let columns = read_column_statistics_from_metadata(&metadata).map_err(jni_err)?;

        let string_class = env.find_class(jni_str!("java/lang/String"))?;
        let arr_len = (columns.len() * 4) as i32;
        let arr = env.new_object_array(arr_len, &string_class, &JObject::null())?;

        for (i, col) in columns.iter().enumerate() {
            let base = i * 4;
            let name = env.new_string(&col.name)?;
            let null_count = env.new_string(col.null_count.to_string())?;
            let min = env.new_string(col.min_value.as_deref().unwrap_or(""))?;
            let max = env.new_string(col.max_value.as_deref().unwrap_or(""))?;
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
) -> jni::sys::jlongArray {
    env.with_env(|env| -> JniResult<jni::sys::jlongArray> {
        let path = file_path.try_to_string(env)?;
        let metadata = read_parquet_metadata(&path).map_err(jni_err)?;
        let stats = read_statistics_from_metadata(&metadata);

        let arr = env.new_long_array(2)?;
        let buf: [i64; 2] = [stats.total_rows, stats.total_bytes];
        arr.set_region(env, 0, &buf)?;
        Ok(arr.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
