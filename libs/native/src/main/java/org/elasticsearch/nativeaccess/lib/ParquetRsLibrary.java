/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

/**
 * Low-level FFI interface to the Rust es_parquet_rs shared library for Parquet operations.
 */
public non-sealed interface ParquetRsLibrary extends NativeLibrary {

    /** Returns the last error message from the native library, or null if none. */
    String lastError();

    /**
     * Reads Parquet file statistics.
     * @param path the path to the Parquet file
     * @param configJson optional JSON storage configuration, or null
     * @return a two-element array: [totalRows, totalBytes], or null on error
     */
    long[] getStatistics(String path, String configJson);

    /**
     * Exports the Parquet file's Arrow schema via the Arrow C Data Interface.
     * Writes an FFI_ArrowSchema to the given memory address.
     * @param path the path to the Parquet file
     * @param configJson optional JSON storage configuration, or null
     * @param schemaAddr memory address of a pre-allocated ArrowSchema FFI struct
     * @return 0 on success, -1 on error (check {@link #lastError()})
     */
    int getSchemaFFI(String path, String configJson, long schemaAddr);
}
