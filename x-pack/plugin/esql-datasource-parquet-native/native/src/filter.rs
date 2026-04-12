use super::jni_utils::*;
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use jni::EnvUnowned;
use jni::errors::{Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{JClass, JLongArray, JString};
use jni::sys::jlong;

fn box_expr(expr: Expr) -> jlong {
    Box::into_raw(Box::new(expr)) as jlong
}

unsafe fn unbox_expr(handle: jlong) -> Box<Expr> {
    unsafe { Box::from_raw(handle as *mut Expr) }
}

unsafe fn ref_expr(handle: jlong) -> &'static Expr {
    unsafe { &*(handle as *const Expr) }
}

// ---------------------------------------------------------------------------
// Column reference
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createColumn(
    mut env: EnvUnowned,
    _class: JClass,
    name: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let col_name = name.try_to_string(env)?;
        Ok(box_expr(col(col_name)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralInt(
    mut env: EnvUnowned,
    _class: JClass,
    value: jni::sys::jint,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        Ok(box_expr(lit(value)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralLong(
    mut env: EnvUnowned,
    _class: JClass,
    value: jni::sys::jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        Ok(box_expr(lit(value)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralTimestampMillis(
    mut env: EnvUnowned,
    _class: JClass,
    value: jni::sys::jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        let scalar = ScalarValue::TimestampMillisecond(Some(value), None);
        Ok(box_expr(Expr::Literal(scalar, None)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralDouble(
    mut env: EnvUnowned,
    _class: JClass,
    value: jni::sys::jdouble,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        Ok(box_expr(lit(value)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralBool(
    mut env: EnvUnowned,
    _class: JClass,
    value: jni::sys::jboolean,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        Ok(box_expr(lit(value)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLiteralString(
    mut env: EnvUnowned,
    _class: JClass,
    value: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let s = value.try_to_string(env)?;
        Ok(box_expr(lit(s)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Binary comparisons — each takes ownership of left and right handles
// ---------------------------------------------------------------------------

macro_rules! binary_op {
    ($jni_name:ident, $op:expr) => {
        #[unsafe(no_mangle)]
        pub extern "system" fn $jni_name(
            mut env: EnvUnowned,
            _class: JClass,
            left: jlong,
            right: jlong,
        ) -> jlong {
            env.with_env(|_env| -> JniResult<jlong> {
                let l = unsafe { *unbox_expr(left) };
                let r = unsafe { *unbox_expr(right) };
                Ok(box_expr($op(l, r)))
            })
            .resolve::<ThrowRuntimeExAndDefault>()
        }
    };
}

binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createEquals,
    |l: Expr, r: Expr| l.eq(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createNotEquals,
    |l: Expr, r: Expr| l.not_eq(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createGreaterThan,
    |l: Expr, r: Expr| l.gt(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createGreaterThanOrEqual,
    |l: Expr, r: Expr| l.gt_eq(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLessThan,
    |l: Expr, r: Expr| l.lt(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLessThanOrEqual,
    |l: Expr, r: Expr| l.lt_eq(r)
);

// ---------------------------------------------------------------------------
// Logical operators
// ---------------------------------------------------------------------------

binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createAnd,
    |l: Expr, r: Expr| l.and(r)
);
binary_op!(
    Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createOr,
    |l: Expr, r: Expr| l.or(r)
);

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createNot(
    mut env: EnvUnowned,
    _class: JClass,
    child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(Expr::Not(Box::new(c))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Null checks
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createIsNull(
    mut env: EnvUnowned,
    _class: JClass,
    child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(c.is_null()))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createIsNotNull(
    mut env: EnvUnowned,
    _class: JClass,
    child: jlong,
) -> jlong {
    env.with_env(|_env| -> JniResult<jlong> {
        let c = unsafe { *unbox_expr(child) };
        Ok(box_expr(c.is_not_null()))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// IN list: createInList(column_handle, [literal_handles])
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createInList(
    mut env: EnvUnowned,
    _class: JClass,
    expr_handle: jlong,
    list_handles: JLongArray,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let e = unsafe { *unbox_expr(expr_handle) };
        let len = list_handles.len(env)?;
        let mut handles = vec![0i64; len];
        list_handles.get_region(env, 0, &mut handles)?;

        let mut items = Vec::with_capacity(len);
        for h in handles {
            items.push(unsafe { *unbox_expr(h) });
        }

        Ok(box_expr(in_list(e, items, false)))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// StartsWith: col >= prefix AND col < nextPrefix
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createStartsWith(
    mut env: EnvUnowned,
    _class: JClass,
    col_handle: jlong,
    prefix: JString,
    upper_bound: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let c = unsafe { ref_expr(col_handle) }.clone();
        let prefix_str = prefix.try_to_string(env)?;

        let lower = c.clone().gt_eq(lit(prefix_str));

        let upper_opt = jstring_to_opt_string(&upper_bound, env)?;
        let result = match upper_opt {
            Some(ub) => lower.and(c.lt(lit(ub))),
            None => lower,
        };

        unsafe { unbox_expr(col_handle) };

        Ok(box_expr(result))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// LIKE: col LIKE pattern (SQL-style % and _ wildcards)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createLike(
    mut env: EnvUnowned,
    _class: JClass,
    col_handle: jlong,
    pattern: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let c = unsafe { *unbox_expr(col_handle) };
        let pat = pattern.try_to_string(env)?;
        Ok(box_expr(c.like(lit(pat))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_createNotLike(
    mut env: EnvUnowned,
    _class: JClass,
    col_handle: jlong,
    pattern: JString,
) -> jlong {
    env.with_env(|env| -> JniResult<jlong> {
        let c = unsafe { *unbox_expr(col_handle) };
        let pat = pattern.try_to_string(env)?;
        Ok(box_expr(c.not_like(lit(pat))))
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_elasticsearch_xpack_esql_datasource_parquet_datafusion_DataFusionBridge_freeExpr(
    mut env: EnvUnowned,
    _class: JClass,
    handle: jlong,
) {
    env.with_env(|_env| -> JniResult<()> {
        if handle != 0 {
            unsafe { unbox_expr(handle) };
        }
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}
