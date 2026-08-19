// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the "Elastic License
// 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
// Public License v 1"; you may not use this file except in compliance with, at
// your election, the "Elastic License 2.0", the "GNU Affero General Public
// License v3.0 only", or the "Server Side Public License, v 1".

use std::ffi::c_char;
use std::fs::File;
use std::ptr;

use arrow::ffi::FFI_ArrowSchema;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

use crate::config::parse_config;
use crate::ffi::{cstr_to_str, ffi_call};

/// Reads Parquet file statistics: total_rows and total_bytes.
/// Writes results to the provided output pointers.
/// `config` is an optional null-terminated JSON config string (NULL to omit).
/// Returns 0 on success, -1 on error (check pqrs_last_error).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pqrs_get_statistics(
    path: *const c_char,
    config: *const c_char,
    out_rows: *mut i64,
    out_bytes: *mut i64,
) -> i32 {
    ffi_call(|| {
        let path = unsafe { cstr_to_str(path)? };
        let _config = unsafe { parse_config(config)? };

        // TODO: use _config to resolve remote storage (object_store) when URI scheme is non-local
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let (total_rows, total_bytes) = metadata.row_groups().iter().fold(
            (0i64, 0i64),
            |(rows, bytes), rg| (rows + rg.num_rows(), bytes + rg.total_byte_size()),
        );

        unsafe {
            *out_rows = total_rows;
            *out_bytes = total_bytes;
        }
        Ok(())
    })
}

/// Exports the Parquet file's Arrow schema via the Arrow C Data Interface.
/// The caller must provide the memory address of a pre-allocated FFI_ArrowSchema struct.
/// `config` is an optional null-terminated JSON config string (NULL to omit).
/// Returns 0 on success, -1 on error (check pqrs_last_error).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pqrs_get_schema_ffi(
    path: *const c_char,
    config: *const c_char,
    schema_addr: i64,
) -> i32 {
    ffi_call(|| {
        let path = unsafe { cstr_to_str(path)? };
        let _config = unsafe { parse_config(config)? };

        // TODO: use _config to resolve remote storage (object_store) when URI scheme is non-local
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata().file_metadata();

        let arrow_schema = parquet_to_arrow_schema(
            metadata.schema_descr(),
            metadata.key_value_metadata(),
        )?;

        let ffi_schema = FFI_ArrowSchema::try_from(&arrow_schema)?;
        unsafe {
            ptr::write(schema_addr as *mut FFI_ArrowSchema, ffi_schema);
        }
        Ok(())
    })
}
