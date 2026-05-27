/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

/**
 * JNI bridge to the Rust parquet-rs based Parquet reader.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy batch transfer from Rust to Java.
 * Filter expressions are built incrementally as a FilterExpr tree in Rust
 * via the {@code create*} methods, then passed to {@link #openReader} as an opaque handle.
 */
final class ParquetRsBridge {

    private ParquetRsBridge() {}

    // ---- Reader lifecycle ----

    /**
     * Opens a parquet-rs reader with optional filter.
     *
     * @param filterHandle opaque handle to a FilterExpr built via create* methods, or 0 for no filter.
     *                     The native side clones the expr; the handle remains valid for reuse across files.
     * @param configJson JSON-serialized storage configuration from the ESQL WITH clause, or null.
     */
    static native long openReader(
        String filePath,
        String[] projectedColumns,
        int batchSize,
        long limit,
        long filterHandle,
        String configJson
    );

    static native long openReaderMulti(
        String[] filePaths,
        long[] splitOffsets,
        long[] splitLengths,
        String[] projectedColumns,
        int batchSize,
        long limit,
        long filterHandle,
        String configJson
    );

    static native boolean nextBatch(long handle, long schemaAddr, long arrayAddr);

    static native void closeReader(long handle);

    /** Returns a human-readable description of the reader's scan plan (pushed filter, projection, row groups, etc.). */
    static native String getReaderPlan(long handle);

    /**
     * Opens a reader that only reads row groups assigned to {@code [rangeStart, rangeEnd)} by parquet-mr-style
     * split logic: inclusion iff {@code startingPos + compressedSize / 2} lies in that half-open interval
     * (matches Java {@code ParquetFormatReader} / {@code filterBlocksByRange}).
     *
     * @param metaHandle opaque handle returned by {@link #loadArrowMetadata}, or {@code 0} to fetch the
     *                   footer inline. When non-zero the per-split footer round-trip is skipped.
     */
    static native long openReaderForRange(
        String filePath,
        String[] projectedColumns,
        int batchSize,
        long limit,
        long filterHandle,
        String configJson,
        long rangeStart,
        long rangeEnd,
        long metaHandle
    );

    /**
     * Fetches and caches the Parquet footer (Arrow metadata) for a file.
     * <p>
     * Returns an opaque handle that must be freed via {@link #freeArrowMetadata} when no longer needed.
     * Pass it to {@link #openReaderForRange} as {@code metaHandle} to avoid re-fetching the footer on
     * every split — especially important for remote storage (S3 / GCS / Azure).
     */
    static native long loadArrowMetadata(String filePath, String configJson);

    /** Frees a metadata handle previously returned by {@link #loadArrowMetadata}. */
    static native void freeArrowMetadata(long handle);

    // ---- Metadata ----

    /**
     * Exports the Parquet file's Arrow schema via the C Data Interface.
     * The caller must allocate an {@code ArrowSchema} and pass its memory address.
     *
     * @param schemaAddr memory address of a pre-allocated {@code ArrowSchema} FFI struct
     */
    static native void getSchemaFFI(String filePath, String configJson, long schemaAddr);

    static native long[] getStatistics(String filePath, String configJson);

    /** Returns column statistics as [name0, nullCount0, min0, max0, name1, ...]. Empty string = absent. */
    static native String[] getColumnStatistics(String filePath, String configJson);

    /**
     * Footprint split discovery after a single Parquet footer read from the native reader.
     * <p>
     * Returns {@code Object[2]}:
     * <ul>
     *   <li>Index 0: {@code long[]} with {@code offset, compressedLength, rowCount, rowGroupUncompressedBytes}
     *       per row group (four values repeated).</li>
     *   <li>Index 1: {@code String[][]} — one inner array per row group: a flat sequence
     *       {@code name, chunkUncompressedSize, nullCount, min, max} per logical column chunk
     *       (when Parquet exposes no typed statistics on the JNI side, {@code min}/{@code max} are empty).
     *       </li>
     * </ul>
     *
     */
    /**
     * @param metaHandle opaque handle returned by {@link #loadArrowMetadata}, or {@code 0} to fetch the
     *                   footer inline via the shared LRU cache.
     */
    static native Object[] discoverRowGroupSplits(String filePath, String configJson, long metaHandle);

    // ---- Filter expression building ----
    //
    // Native handle ownership contract for every {@code create*} method below:
    // * On success the method returns a fresh native handle (a {@code jlong}). The
    // Java caller owns it and must eventually pass it to {@link #freeExpr} or to
    // another {@code create*} method as an input.
    // * On failure the method throws a Java exception (typically RuntimeException)
    // and returns 0.
    // * Every input handle passed in is consumed by the call regardless of whether
    // it succeeds or throws. Java callers MUST NOT call {@link #freeExpr} on an
    // input handle after passing it to a {@code create*} method, even on failure.
    // * Each handle is single-use: pass it to exactly one downstream call (a
    // {@code create*} or {@link #freeExpr}).
    //
    // The recommended Java-side pattern is to wrap fresh handles in {@link ExprHandle}
    // and call {@link ExprHandle#release} immediately before passing them to a
    // {@code create*} method, so try-with-resources cleanup is a no-op on the success
    // path and a no-op (rather than a double-free) if the {@code create*} call throws.

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

    static native long createStartsWith(long colHandle, String prefix, String upperBound);

    static native long createLike(long colHandle, String pattern, boolean caseInsensitive);

    static native long createNotLike(long colHandle, String pattern, boolean caseInsensitive);

    static native void freeExpr(long handle);

    /** Returns a human-readable string representation of a FilterExpr. */
    static native String describeExpr(long handle);
}
