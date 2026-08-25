/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.adapter.MemorySegmentAdapter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Low-level FFI interface to the Rust es_parquet_rs shared library for Parquet operations.
 */
@LibrarySpecification(name = "es_parquet_rs", unavailableOn = { Platform.WINDOWS_X64, Platform.DARWIN_X64 })
public abstract class ParquetRsLibrary {

    private static final int ERROR_BUF_SIZE = 4096;

    /** Raw binding for pqrs_last_error; use {@link #lastError()} instead. */
    @Function("pqrs_last_error")
    protected abstract int lastError(MemorySegment buf, int size);

    /** Returns the last error message from the native library, or null if none. */
    public String lastError() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(ERROR_BUF_SIZE);
            int len = lastError(buf, ERROR_BUF_SIZE);
            if (len <= 0) {
                return null;
            }
            return MemorySegmentAdapter.getString(buf, 0);
        }
    }

    /** Raw binding for pqrs_get_statistics; use {@link #getStatistics(String, String)} instead. */
    @Function("pqrs_get_statistics")
    protected abstract int getStatistics(String path, String configJson, MemorySegment outRows, MemorySegment outBytes);

    /**
     * Reads Parquet file statistics.
     * @param path the path to the Parquet file
     * @param configJson optional JSON storage configuration, or null
     * @return a two-element array: [totalRows, totalBytes], or null on error
     */
    public long[] getStatistics(String path, String configJson) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outRows = arena.allocate(JAVA_LONG);
            MemorySegment outBytes = arena.allocate(JAVA_LONG);
            int rc = getStatistics(path, configJson, outRows, outBytes);
            if (rc != 0) {
                return null;
            }
            return new long[] { outRows.get(JAVA_LONG, 0), outBytes.get(JAVA_LONG, 0) };
        }
    }

    /**
     * Exports the Parquet file's Arrow schema via the Arrow C Data Interface.
     * Writes an FFI_ArrowSchema to the given memory address.
     * @param path the path to the Parquet file
     * @param configJson optional JSON storage configuration, or null
     * @param schemaAddr memory address of a pre-allocated ArrowSchema FFI struct
     * @return 0 on success, -1 on error (check {@link #lastError()})
     */
    @Function("pqrs_get_schema_ffi")
    public abstract int getSchemaFFI(String path, String configJson, long schemaAddr);
}
