use crate::store::StorageConfig;
use jni::Env;
use jni::errors::{Error as JniError, Result as JniResult};
use jni::objects::JString;

pub fn jstring_to_opt_string(jstr: &JString, env: &mut Env) -> JniResult<Option<String>> {
    if jstr.is_null() {
        return Ok(None);
    }
    Ok(Some(jstr.try_to_string(env)?))
}

pub fn jni_err(e: impl std::fmt::Display) -> JniError {
    JniError::ParseFailed(format!("{e}"))
}

pub fn extract_storage_config(
    env: &mut Env,
    config_json: &JString,
) -> JniResult<StorageConfig> {
    match jstring_to_opt_string(config_json, env)? {
        Some(json) => StorageConfig::from_json(&json).map_err(jni_err),
        None => Ok(StorageConfig::empty()),
    }
}

