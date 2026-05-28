/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Result of {@link StorageObject#readBytesAsync(long, long, DirectBufferFactory, java.util.concurrent.Executor,
 * org.elasticsearch.action.ActionListener)}: the bytes plus a {@link Releasable} that frees the
 * native memory backing them.
 *
 * <p>The {@code buffer} is a direct {@link ByteBuffer} view of an {@link ArrowBuf} allocated from
 * the caller-supplied {@link BufferAllocator}. The caller must invoke {@link #close()} once the
 * bytes have been consumed: closing decrements the {@code ArrowBuf}'s reference count, which is
 * what actually returns the memory to the allocator. Closing the allocator alone is not enough —
 * Arrow's {@code BaseAllocator.close()} treats outstanding {@code ArrowBuf}s as a leak (see
 * esql-planning#851).
 *
 * <p>Using {@link #buffer()} after {@link #close()} reads dangling memory; the underlying chunk
 * may have been recycled into another allocation.
 */
public record DirectReadBuffer(ByteBuffer buffer, Releasable release) implements Releasable {

    /**
     * Allocates an {@link ArrowBuf} of {@code length} bytes from {@code allocator} and wraps it
     * as a {@link DirectReadBuffer}. The returned buffer's contents are uninitialized; the caller
     * is responsible for filling {@link #buffer()} before delivering it downstream and for
     * calling {@link #close()} once consumption is complete (or on the failure path).
     *
     * <p>Translates allocator failures (breaker trip, {@link OutOfMemoryError}, any other Arrow
     * runtime exception) into a single {@link IOException} so backend implementations can use a
     * uniform error path. The intermediate {@link ArrowBuf} is released on every failure path.
     */
    public static DirectReadBuffer allocate(BufferAllocator allocator, int length) throws IOException {
        ArrowBuf buf = null;
        try {
            buf = allocator.buffer(length);
            return new DirectReadBuffer(buf.nioBuffer(0, length), buf::close);
        } catch (OutOfMemoryError | RuntimeException e) {
            if (buf != null) {
                buf.close();
            }
            throw new IOException("failed to allocate " + length + " bytes from allocator " + allocator.getName(), e);
        }
    }

    @Override
    public void close() {
        release.close();
    }
}
