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
 * Filter expressions are built incrementally as a DataFusion {@code Expr} tree in Rust
 * via the {@code create*} methods, then passed to {@link #openReader} as an opaque handle.
 */
final class DataFusionBridge {

    static {
        NativeLibLoader.load();
    }

    private DataFusionBridge() {}

    // ---- Reader lifecycle ----

    /**
     * Opens a DataFusion reader with optional filter.
     *
     * @param filterHandle opaque handle to a DataFusion Expr built via create* methods, or 0 for no filter.
     *                     Ownership is transferred: the native side frees the Expr when the reader opens.
     */
    static native long openReader(String filePath, String[] projectedColumns, int batchSize, long limit, long filterHandle);

    static native boolean nextBatch(long handle, long schemaAddr, long arrayAddr);

    static native void closeReader(long handle);

    /** Returns the DataFusion execution plan string for an open reader. */
    static native String getExecutionPlan(long handle);

    // ---- Metadata ----

    static native String[] getSchema(String filePath);

    static native long[] getStatistics(String filePath);

    /** Returns column statistics as [name0, nullCount0, min0, max0, name1, ...]. Empty string = absent. */
    static native String[] getColumnStatistics(String filePath);

    // ---- Filter expression building ----
    // Each create* method returns an opaque handle (boxed Expr on Rust heap).
    // Binary operations consume their input handles (ownership transfer).

    static native long createColumn(String name);

    static native long createLiteralInt(int value);

    static native long createLiteralLong(long value);

    static native long createLiteralTimestampMillis(long value);

    static native long createLiteralDouble(double value);

    static native long createLiteralBool(boolean value);

    static native long createLiteralString(String value);

    static native long createEquals(long left, long right);

    static native long createNotEquals(long left, long right);

    static native long createGreaterThan(long left, long right);

    static native long createGreaterThanOrEqual(long left, long right);

    static native long createLessThan(long left, long right);

    static native long createLessThanOrEqual(long left, long right);

    static native long createAnd(long left, long right);

    static native long createOr(long left, long right);

    static native long createNot(long child);

    static native long createIsNull(long child);

    static native long createIsNotNull(long child);

    static native long createInList(long exprHandle, long[] listHandles);

    /**
     * Builds a StartsWith filter as {@code col >= prefix AND col < upperBound}.
     *
     * @param colHandle handle to a column Expr (consumed)
     * @param prefix the prefix string
     * @param upperBound exclusive upper bound string, or null if prefix is all-max codepoints
     */
    static native long createStartsWith(long colHandle, String prefix, String upperBound);

    /** Builds {@code col LIKE pattern} using SQL LIKE syntax (% and _ wildcards). */
    static native long createLike(long colHandle, String pattern);

    /** Builds {@code col NOT LIKE pattern} using SQL LIKE syntax (% and _ wildcards). */
    static native long createNotLike(long colHandle, String pattern);

    static native void freeExpr(long handle);
}
