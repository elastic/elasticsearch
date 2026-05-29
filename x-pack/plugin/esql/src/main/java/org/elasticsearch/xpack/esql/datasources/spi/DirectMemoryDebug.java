/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Assertions;

import java.nio.ByteBuffer;

/**
 * Assertions-only helpers for catching use-after-free of off-heap (direct) read buffers.
 *
 * <p>Direct read buffers are exposed to decoders as detached {@link ByteBuffer} views over
 * {@code ArrowBuf} native memory. Reads through those views bypass Arrow's reference-count
 * tracking, so neither the Arrow debug allocator nor a {@code Releasable} can observe a read that
 * aliases a buffer after it has been freed. That is the failure mode behind the nondeterministic
 * zstd corruption ({@code "Src size is incorrect"} / {@code "Destination buffer is too small"}): a
 * page slice handed out before free is read after the backing region was returned to the allocator
 * and recycled — and it only manifests when the region happens to have been overwritten already.
 *
 * <p>{@link #poison(ByteBuffer)} removes that timing dependency: called immediately before a direct
 * buffer is freed, it overwrites the region with a recognizable sentinel so any surviving alias
 * reads deterministically-corrupt bytes on every run instead of occasionally seeing intact data.
 * It is a no-op when assertions are disabled and on heap-backed buffers (test stubs), so the
 * production read path is unchanged.
 */
public final class DirectMemoryDebug {

    /** Spells "BAD" in hex ({@code BA DB AD BA …}) — unmistakable in a dump and fails any zstd frame check. */
    private static final int POISON = 0xBADBADBA;

    private DirectMemoryDebug() {}

    /**
     * Overwrite the direct memory backing {@code buffer} with the poison sentinel, just before the
     * buffer is released. No-op unless assertions are enabled and {@code buffer} is direct. Does not
     * mutate the caller's {@code position}/{@code limit}.
     */
    public static void poison(ByteBuffer buffer) {
        if (Assertions.ENABLED == false || buffer == null || buffer.isDirect() == false) {
            return;
        }
        ByteBuffer view = buffer.duplicate();
        view.clear();
        while (view.remaining() >= Integer.BYTES) {
            view.putInt(POISON);
        }
        while (view.hasRemaining()) {
            view.put((byte) 0xDE);
        }
    }
}
