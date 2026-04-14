use super::filter::{self, FilterExpr};
use super::jni_utils::*;
use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

pub struct ReaderState {
    reader: ParquetRecordBatchReader,
    rows_remaining: Option<usize>,
    plan: String,
}

type ReaderError = Box<dyn std::error::Error + Send + Sync>;

fn open_reader(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: usize,
    limit: Option<usize>,
    filter: Option<FilterExpr>,
) -> Result<ReaderState, ReaderError> {
    let file = std::fs::File::open(file_path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    let parquet_schema = builder.metadata().file_metadata().schema_descr_ptr();
    let arrow_schema = builder.schema().clone();
    let total_row_groups = builder.metadata().num_row_groups();
    let mut selected_row_groups = total_row_groups;

    // Row group pruning via statistics
    if let Some(ref expr) = filter {
        let metadata = builder.metadata().clone();
        let mut selected = Vec::new();
        for rg_idx in 0..total_row_groups {
            let rg_meta = metadata.row_group(rg_idx);
            if filter::row_group_matches(expr, rg_meta, &parquet_schema) {
                selected.push(rg_idx);
            }
        }
        selected_row_groups = selected.len();
        if selected_row_groups < total_row_groups {
            builder = builder.with_row_groups(selected);
        }
    }

    // Projection pushdown: match against Arrow schema field names (top-level)
    // and use root-level indices so that group/list columns are included correctly.
    if let Some(cols) = &projected_cols {
        let root_indices: Vec<usize> = cols.iter()
            .filter_map(|name| {
                let lower = name.to_ascii_lowercase();
                arrow_schema.fields().iter().enumerate()
                    .find(|(_, f)| f.name().to_ascii_lowercase() == lower)
                    .map(|(i, _)| i)
            })
            .collect();
        if root_indices.is_empty() == false {
            let projection = ProjectionMask::roots(&parquet_schema, root_indices);
            builder = builder.with_projection(projection);
        }
    }

    builder = builder.with_batch_size(batch_size);

    if let Some(limit) = limit {
        builder = builder.with_limit(limit);
    }

    // Row-level filter pushdown
    if let Some(ref expr) = filter {
        let row_filter = filter::build_row_filter(expr, arrow_schema, &parquet_schema);
        builder = builder.with_row_filter(row_filter);
    }

    let reader = builder.build()?;

    let plan = build_plan_string(
        file_path,
        &projected_cols,
        batch_size,
        limit,
        &filter,
        selected_row_groups,
        total_row_groups,
    );

    Ok(ReaderState {
        reader,
        rows_remaining: limit,
        plan,
    })
}

fn build_plan_string(
    file_path: &str,
    projected_cols: &Option<Vec<String>>,
    batch_size: usize,
    limit: Option<usize>,
    filter: &Option<FilterExpr>,
    selected_row_groups: usize,
    total_row_groups: usize,
) -> String {
    let mut parts = Vec::new();

    // File name (just the basename for readability)
    let basename = file_path.rsplit('/').next().unwrap_or(file_path);
    parts.push(format!("file=[{basename}]"));

    // Projection
    if let Some(cols) = projected_cols {
        parts.push(format!("projection=[{}]", cols.join(", ")));
    }

    // Filter
    if let Some(expr) = filter {
        parts.push(format!("filter=[{expr}]"));
    }

    // Row group pruning
    if selected_row_groups < total_row_groups {
        parts.push(format!("row_groups={selected_row_groups}/{total_row_groups}"));
    } else {
        parts.push(format!("row_groups={total_row_groups}"));
    }

    // Limit
    if let Some(limit) = limit {
        parts.push(format!("limit={limit}"));
    }

    parts.push(format!("batch_size={batch_size}"));

    format!("ParquetRsScan: {}", parts.join(", "))
}

// ---------------------------------------------------------------------------
// JNI entry points
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_openReader(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    projected_columns: JObjectArray,
    batch_size: jni::sys::jint,
    limit: jlong,
    filter_handle: jlong,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let filter = if filter_handle != 0 {
            Some(unsafe { &*(filter_handle as *const FilterExpr) }.clone())
        } else {
            None
        };

        let state = open_reader(
            &file_path.try_to_string(env)?,
            string_array_to_vec(&projected_columns, env)?,
            batch_size as usize,
            jlong_to_opt_usize(limit),
            filter,
        )
        .map_err(jni_err)?;

        let handle = Box::into_raw(Box::new(state)) as jlong;
        Ok(handle)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_nextBatch(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
    schema_addr: jlong,
    array_addr: jlong,
) -> jboolean {
    env.with_env(|_env| -> JniResult<jboolean> {
        let state = unsafe { &mut *(handle as *mut ReaderState) };

        if let Some(remaining) = state.rows_remaining {
            if remaining == 0 {
                return Ok(JNI_FALSE);
            }
        }

        let next = state.reader.next().transpose().map_err(jni_err)?;

        match next {
            None => Ok(JNI_FALSE),
            Some(batch) => {
                let rows = batch.num_rows();
                if let Some(ref mut remaining) = state.rows_remaining {
                    *remaining = remaining.saturating_sub(rows);
                }

                let struct_array = StructArray::from(batch);
                let data = struct_array.to_data();

                let ffi_schema = FFI_ArrowSchema::try_from(data.data_type()).map_err(jni_err)?;
                let ffi_array = FFI_ArrowArray::new(&data);

                unsafe {
                    let schema_ptr = schema_addr as *mut FFI_ArrowSchema;
                    let array_ptr = array_addr as *mut FFI_ArrowArray;
                    schema_ptr.write(ffi_schema);
                    array_ptr.write(ffi_array);
                }

                Ok(JNI_TRUE)
            }
        }
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_parquetrs_ParquetRsBridge_closeReader(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
) {
    env.with_env(|_env| -> JniResult<()> {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut ReaderState));
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
        let state = unsafe { &*(handle as *const ReaderState) };
        let jstr = env.new_string(&state.plan)?;
        Ok(jstr.into_raw())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
