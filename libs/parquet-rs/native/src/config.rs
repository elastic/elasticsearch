// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the "Elastic License
// 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
// Public License v 1"; you may not use this file except in compliance with, at
// your election, the "Elastic License 2.0", the "GNU Affero General Public
// License v3.0 only", or the "Server Side Public License, v 1".

use std::collections::HashMap;
use std::ffi::c_char;

use crate::ffi::cstr_to_str;

/// Parses the optional config JSON into a key-value map.
/// Returns an empty map if the pointer is null.
pub unsafe fn parse_config(
    config_ptr: *const c_char,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    if config_ptr.is_null() {
        return Ok(HashMap::new());
    }
    let json = unsafe { cstr_to_str(config_ptr) }?;
    Ok(serde_json::from_str(json)?)
}
