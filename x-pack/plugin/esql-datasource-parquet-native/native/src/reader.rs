use super::ASYNC_RUNTIME;
use super::jni_utils::*;
use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::StreamExt;
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{jboolean, jlong, JNI_FALSE, JNI_TRUE};

pub struct ReaderState {
    _ctx: SessionContext,
    stream: SendableRecordBatchStream,
    execution_plan: String,
}

type ReaderError = Box<dyn std::error::Error + Send + Sync>;

async fn open_stream(
    file_path: &str,
    projected_cols: Option<Vec<String>>,
    batch_size: usize,
    limit: Option<usize>,
    filter: Option<Expr>,
) -> Result<ReaderState, ReaderError> {
    let mut config = SessionConfig::new().with_batch_size(batch_size);
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;

    let ctx = SessionContext::new_with_config(config);

    let parquet_opts = ParquetReadOptions::default();
    ctx.register_parquet("data", file_path, parquet_opts).await?;

    let mut df = ctx.table("data").await?;

    if let Some(expr) = filter {
        df = df.filter(expr)?;
    }

    if let Some(cols) = projected_cols {
        let exprs: Vec<Expr> = cols.into_iter().map(|c| col(c)).collect();
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
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let filter = if filter_handle != 0 {
            Some(unsafe { *Box::from_raw(filter_handle as *mut Expr) })
        } else {
            None
        };

        let state = ASYNC_RUNTIME
            .block_on(open_stream(
                &file_path.try_to_string(env)?,
                string_array_to_vec(&projected_columns, env)?,
                batch_size as usize,
                jlong_to_opt_usize(limit),
                filter,
            ))
            .map_err(jni_err)?;

        Ok(Box::into_raw(Box::new(state)) as jlong)
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
