/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.ParquetRsLibrary;

import java.io.IOException;

/**
 * Public-facing Java API for Parquet operations backed by the Rust {@code es_parquet_rs}
 * native library, accessed via Panama FFI.
 *
 * <p> All methods delegate to the low-level {@link ParquetRsLibrary} bindings and translate
 * native error codes into {@link IOException}, including the native error message when
 * available.
 *
 * <p> An optional JSON configuration string ({@code configJson}) can be supplied to configure
 * access to remote object stores (S3, Azure, GCS). For local file paths, pass {@code null}.
 */
public final class ParquetRsFunctions {

    private final ParquetRsLibrary lib;

    ParquetRsFunctions(ParquetRsLibrary lib) {
        this.lib = lib;
    }

    /**
     * Reads file-level statistics from a Parquet file.
     *
     * <p> Aggregates row counts and byte sizes across all row groups in the file.
     *
     * @param path       the path to the Parquet file (local filesystem path or remote URI)
     * @param configJson optional JSON string containing storage configuration for remote
     *                   object stores (e.g. S3 credentials, Azure connection info), or
     *                   {@code null} for local files
     * @return a two-element array: {@code [totalRows, totalBytes]}
     * @throws IOException if the file cannot be opened, is not a valid Parquet file,
     *                     or if {@code configJson} is malformed JSON
     */
    public long[] getStatistics(String path, String configJson) throws IOException {
        long[] result = lib.getStatistics(path, configJson);
        if (result == null) {
            throw newIOException("getStatistics", path);
        }
        return result;
    }

    /**
     * Exports the Parquet file's Arrow schema via the Arrow C Data Interface.
     *
     * <p> The caller must allocate an {@code ArrowSchema} struct (e.g. via
     * {@code org.apache.arrow.c.ArrowSchema.allocateNew}) and pass its memory address.
     * On success the struct is populated with the schema exported from the Parquet file's
     * metadata; ownership of any heap-allocated children is transferred to the caller.
     *
     * @param path       the path to the Parquet file (local filesystem path or remote URI)
     * @param configJson optional JSON string containing storage configuration for remote
     *                   object stores (e.g. S3 credentials, Azure connection info), or
     *                   {@code null} for local files
     * @param schemaAddr the native memory address of a pre-allocated {@code FFI_ArrowSchema}
     *                   struct where the exported schema will be written
     * @throws IOException if the file cannot be opened, is not a valid Parquet file,
     *                     or if {@code configJson} is malformed JSON
     */
    public void getSchemaFFI(String path, String configJson, long schemaAddr) throws IOException {
        int rc = lib.getSchemaFFI(path, configJson, schemaAddr);
        if (rc != 0) {
            throw newIOException("getSchemaFFI", path);
        }
    }

    private IOException newIOException(String operation, String path) {
        String nativeError = lib.lastError();
        String message = nativeError != null
            ? operation + " failed for [" + path + "]: " + nativeError
            : operation + " failed for [" + path + "]";
        return new IOException(message);
    }
}
