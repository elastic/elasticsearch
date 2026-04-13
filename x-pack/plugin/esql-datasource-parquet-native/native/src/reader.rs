use super::ASYNC_RUNTIME;
use super::jni_utils::*;
use super::objstore;
use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::StreamExt;
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};
use std::collections::HashMap;

pub struct ReaderState {
    _ctx: SessionContext,
    stream: SendableRecordBatchStream,
    execution_plan: String,
}

type ReaderError = Box<dyn std::error::Error + Send + Sync>;

/// Builds a lookup from lowercase field name to the actual schema field name.
fn build_case_map(schema: &datafusion::common::DFSchema) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for field in schema.fields() {
        map.insert(field.name().to_ascii_lowercase(), field.name().clone());
    }
    map
}

/// Rewrites column references in an Expr tree to match the actual schema case.
fn remap_columns(expr: Expr, case_map: &HashMap<String, String>) -> Expr {
    expr.transform(|e| {
        match e {
            Expr::Column(ref c) => {
                let lookup = c.name().to_ascii_lowercase();
                if let Some(actual) = case_map.get(&lookup) {
                    if actual != c.name() {
                        return Ok(Transformed::yes(
                            Expr::Column(Column::new(Some("data"), actual.as_str()))
                        ));
                    }
                }
                Ok(Transformed::no(e))
            }
            _ => Ok(Transformed::no(e)),
        }
    })
    .unwrap()
    .data
}

async fn open_stream(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: usize,
    limit: Option<usize>,
    filter: Option<Expr>,
    config: &HashMap<String, String>,
) -> Result<ReaderState, ReaderError> {
    let ctx = objstore::create_session(file_path, batch_size, config)?;

    let parquet_opts = ParquetReadOptions::default();
    ctx.register_parquet("data", file_path, parquet_opts).await?;

    let mut df = ctx.table("data").await?;

    let case_map = build_case_map(df.schema());

    if let Some(expr) = filter {
        let remapped = remap_columns(expr, &case_map);
        df = df.filter(remapped)?;
    }

    if let Some(cols) = projected_cols {
        let exprs: Vec<Expr> = cols.iter().map(|c| {
            let lookup = c.to_ascii_lowercase();
            match case_map.get(&lookup) {
                Some(actual) => col(Column::new(Some("data"), actual.as_str())),
                None => col(c.as_str()),
            }
        }).collect();
        df = df.select(exprs)?;
    }

    if let Some(limit) = limit {
        df = df.limit(0, Some(limit))?;
    }

    let plan = df.clone().create_physical_plan().await?;
    let execution_plan = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();

    let stream = df.execute_stream().await?;

    Ok(ReaderState {
        _ctx: ctx,
        stream,
        execution_plan,
    })
}

// ---------------------------------------------------------------------------
// JNI entry points
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_openReader(
    mut env: EnvUnowned,
    _class: JClass,
    file_path: JString,
    projected_columns: JObjectArray,
    batch_size: jni::sys::jint,
    limit: jlong,
    filter_handle: jlong,
    config_keys: JObjectArray,
    config_values: JObjectArray,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let filter = if filter_handle != 0 {
            Some(unsafe { &*(filter_handle as *const Expr) }.clone())
        } else {
            None
        };

        let config = string_arrays_to_map(&config_keys, &config_values, env)?;

        let state = ASYNC_RUNTIME
            .block_on(open_stream(
                &file_path.try_to_string(env)?,
                string_array_to_vec(&projected_columns, env)?,
                batch_size as usize,
                jlong_to_opt_usize(limit),
                filter,
                &config,
            ))
            .map_err(jni_err)?;

        let handle = Box::into_raw(Box::new(state)) as jlong;
        Ok(handle)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

/// Reads the next batch and writes the Arrow C Data Interface structs to the provided addresses.
/// Returns true if a batch was produced, false if end-of-stream.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_nextBatch(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
    schema_addr: jlong,
    array_addr: jlong,
) -> jboolean {
    env.with_env(|_env| -> JniResult<jboolean> {
        let state = unsafe { &mut *(handle as *mut ReaderState) };

        let next = ASYNC_RUNTIME
            .block_on(state.stream.next())
            .transpose()
            .map_err(jni_err)?;

        match next {
            None => Ok(JNI_FALSE),
            Some(batch) => {
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
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_getExecutionPlan<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::objects::JString<'local> {
    env.with_env(|env| -> JniResult<jni::objects::JString<'local>> {
        let state = unsafe { &*(handle as *const ReaderState) };
        let plan_str = env.new_string(&state.execution_plan)?;
        Ok(plan_str)
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_closeReader(
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
