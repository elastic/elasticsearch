/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

/**
 * Opaque wrapper around a native parquet-rs {@code FilterExpr} handle representing a pushed filter.
 * <p>
 * The handle is a pointer to a heap-allocated {@code Box<FilterExpr>} on the Rust side.
 * The native side clones the FilterExpr on each {@link ParquetRsBridge#openReader} call so
 * the handle remains valid across multiple files. The caller must free the handle via
 * {@link ParquetRsBridge#freeExpr} when the reader is closed.
 */
record ParquetRsPushedFilter(long exprHandle) {
    private static String describe(long handle) {
        if (handle == 0) {
            return "none";
        }
        return ParquetRsBridge.describeExpr(handle);
    }

    @Override
    public String toString() {
        return describe(exprHandle);
    }
}
