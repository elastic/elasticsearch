/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

/**
 * JNI bridge to the Rust DataFusion-based Parquet reader.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy batch transfer from Rust to Java.
 * The schema and array FFI structs are written to caller-provided memory addresses,
 * then imported into Java Arrow vectors via {@code Data.importVectorSchemaRoot}.
 */
final class DataFusionBridge {

    static {
        NativeLibLoader.load();
    }

    private DataFusionBridge() {}

    /** Opens a DataFusion reader. Returns an opaque native handle. */
    static native long openReader(String filePath, String[] projectedColumns, int batchSize, long limit);

    /**
     * Reads the next batch via the Arrow C Data Interface.
     * Writes FFI_ArrowSchema and FFI_ArrowArray to the provided memory addresses.
     *
     * @param schemaAddr memory address of an allocated ArrowSchema FFI struct
     * @param arrayAddr memory address of an allocated ArrowArray FFI struct
     * @return true if a batch was produced, false if end-of-stream
     */
    static native boolean nextBatch(long handle, long schemaAddr, long arrayAddr);

    /** Closes the native reader and releases all resources. */
    static native void closeReader(long handle);

    /** Returns schema as [name0, typeId0, elemTypeId0, name1, ...]. */
    static native String[] getSchema(String filePath);

    /** Returns [totalRows, totalBytes]. */
    static native long[] getStatistics(String filePath);
}
