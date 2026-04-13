use jni::Env;
use jni::errors::{Error as JniError, Result as JniResult};
use jni::objects::{JObjectArray, JString};
use jni::sys::{jdouble, jlong};
use std::collections::HashMap;

pub fn jlong_to_opt_usize(jlong: jlong) -> Option<usize> {
    if jlong < 0 { None } else { Some(jlong as usize) }
}

pub fn jdouble_to_opt_f64(jdouble: jdouble) -> Option<f64> {
    if jdouble.is_nan() { None } else { Some(jdouble) }
}

pub fn string_array_to_vec(arr: &JObjectArray, env: &mut Env) -> JniResult<Option<Vec<String>>> {
    if arr.is_null() {
        return Ok(None);
    }
    let len = arr.len(env)?;
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let obj = arr.get_element(env, i)?;
        let jstr: JString = JString::cast_local(env, obj)?;
        result.push(jstr.try_to_string(env)?);
    }
    Ok(Some(result))
}

/// Converts two parallel JNI string arrays (keys, values) into a HashMap.
/// Returns an empty map if either array is null.
pub fn string_arrays_to_map(
    keys: &JObjectArray,
    values: &JObjectArray,
    env: &mut Env,
) -> JniResult<HashMap<String, String>> {
    if keys.is_null() || values.is_null() {
        return Ok(HashMap::new());
    }
    let len = keys.len(env)?;
    let mut map = HashMap::with_capacity(len);
    for i in 0..len {
        let k_obj = keys.get_element(env, i)?;
        let v_obj = values.get_element(env, i)?;
        let k: JString = JString::cast_local(env, k_obj)?;
        let v: JString = JString::cast_local(env, v_obj)?;
        map.insert(k.try_to_string(env)?, v.try_to_string(env)?);
    }
    Ok(map)
}

pub fn jstring_to_opt_string(jstr: &JString, env: &mut Env) -> JniResult<Option<String>> {
    if jstr.is_null() {
        return Ok(None);
    }
    Ok(Some(jstr.try_to_string(env)?))
}

pub fn jni_err(e: impl std::fmt::Display) -> JniError {
    JniError::ParseFailed(format!("{e}"))
}
