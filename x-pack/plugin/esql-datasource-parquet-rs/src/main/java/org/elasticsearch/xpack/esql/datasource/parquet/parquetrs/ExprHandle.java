/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * Owning wrapper around a native FilterExpr handle returned by {@link ParquetRsBridge}
 * {@code create*} methods. Exists to make leak-free composition of native handles
 * straightforward via try-with-resources.
 *
 * <p>Usage pattern:
 * <pre>{@code
 * try (ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(name));
 *      ExprHandle lit = ExprHandle.of(createLiteral(...))) {
 *     if (lit.get() == 0) return 0;                   // try-with-resources frees col
 *     return ParquetRsBridge.createEquals(col.release(), lit.release());
 * }
 * }</pre>
 *
 * <p>{@code release()} <strong>must</strong> be called immediately before passing
 * the underlying handle to another {@code create*} method, because per the
 * {@link ParquetRsBridge} contract those methods take ownership of their inputs
 * (success <em>and</em> failure). Releasing first means {@link #close()} becomes a
 * no-op even if the {@code create*} call throws, avoiding a double-free.
 *
 * <p>If the wrapper is closed without being released (e.g. an early return or an
 * exception thrown before the {@code create*} call), {@link ParquetRsBridge#freeExpr}
 * is invoked to free the still-owned handle.
 */
final class ExprHandle implements Releasable {

    private long handle;

    static ExprHandle of(long handle) {
        return new ExprHandle(handle);
    }

    private ExprHandle(long handle) {
        this.handle = handle;
    }

    /** The raw native handle, or 0 if it has been released or never set. */
    long get() {
        return handle;
    }

    /**
     * Hands ownership of the native handle to the caller and zeroes this wrapper,
     * making subsequent {@link #close()} calls a no-op. Used immediately before
     * passing the handle to a {@link ParquetRsBridge} {@code create*} method (which
     * consumes its inputs whether it succeeds or throws).
     */
    long release() {
        long h = handle;
        handle = 0;
        return h;
    }

    @Override
    public void close() {
        if (handle != 0) {
            ParquetRsBridge.freeExpr(handle);
            handle = 0;
        }
    }

    /**
     * Releases every wrapper in {@code list} and returns the underlying handles
     * as a primitive array, ready to be passed to a {@code create*} method that
     * accepts {@code long[]}.
     */
    static long[] releaseAll(List<ExprHandle> list) {
        long[] out = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            out[i] = list.get(i).release();
        }
        return out;
    }
}
