use jni::Env;
use jni::errors::{Error as JniError, Result as JniResult};
use jni::objects::{JObjectArray, JString};
use jni::sys::jlong;

pub fn jlong_to_opt_usize(jlong: jlong) -> Option<usize> {
    if jlong < 0 { None } else { Some(jlong as usize) }
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

pub fn jstring_to_opt_string(jstr: &JString, env: &mut Env) -> JniResult<Option<String>> {
    if jstr.is_null() {
        return Ok(None);
    }
    Ok(Some(jstr.try_to_string(env)?))
}

pub fn jni_err(e: impl std::fmt::Display) -> JniError {
    JniError::ParseFailed(format!("{e}"))
}
