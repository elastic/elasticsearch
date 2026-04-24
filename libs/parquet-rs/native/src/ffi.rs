// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the "Elastic License
// 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
// Public License v 1"; you may not use this file except in compliance with, at
// your election, the "Elastic License 2.0", the "GNU Affero General Public
// License v3.0 only", or the "Server Side Public License, v 1".

use std::cell::RefCell;
use std::ffi::{c_char, CStr};
use std::ptr;

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub fn set_last_error(msg: String) {
    LAST_ERROR.set(Some(msg));
}

/// Evaluates an expression that returns `Result`. On `Ok`, yields the value.
/// On `Err`, stores the error message via `set_last_error` and returns `-1`
/// from the enclosing function.
macro_rules! ffi_try {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                $crate::ffi::set_last_error(format!("{e}"));
                return -1;
            }
        }
    };
}

/// Calls the provided function, catching panics and converting them to errors.
/// Returns 0 on success, -1 on error or panic.
pub fn ffi_call<F, T, E>(f: F) -> i64
where
    F: FnOnce() -> Result<(), E> + std::panic::UnwindSafe,
    E: std::fmt::Display,
{
    match std::panic::catch_unwind(|| f()) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            crate::ffi::set_last_error(e.to_string());
            -1
        }
        Err(e) => {
            let x = format!("Panic: {:?}", e);
            crate::ffi::set_last_error(x);
            -1
        }
    }
}

/// Converts a null-terminated C string pointer to a Rust `&str`.
pub unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    unsafe { CStr::from_ptr(ptr) }.to_str()
}

/// Copies the last error message into the provided buffer.
/// Returns the number of bytes written (excluding null terminator), or 0 if no error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pqrs_last_error(buf: *mut c_char, buf_len: i32) -> i32 {
    if buf.is_null() || buf_len <= 0 {
        return 0;
    }
    LAST_ERROR.with(|e| {
        let mut err = e.borrow_mut();
        match err.take() {
            Some(msg) => {
                // Truncate to a valid UTF-8 char boundary
                let buf_len = buf_len as usize;
                let mut copy_len: usize;
                if (msg.len() < buf_len) {
                    copy_len = msg.len();
                } else {
                    copy_len = buf_len - 1;
                    while !msg.is_char_boundary(copy_len) {
                        copy_len -= 1;
                    }
                }
                unsafe {
                    ptr::copy_nonoverlapping(msg.as_ptr(), buf as *mut u8, copy_len);
                    *buf.add(copy_len+1) = 0;
                }
                copy_len as i32
            }
            None => {
                unsafe { *buf = 0 };
                0
            }
        }
    })
}
