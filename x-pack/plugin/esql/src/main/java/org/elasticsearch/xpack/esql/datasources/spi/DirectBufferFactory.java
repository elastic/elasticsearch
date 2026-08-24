/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.LocalCircuitBreaker;

import java.io.IOException;

/**
 * Producer of {@link DirectReadBuffer}s for {@link StorageObject#readBytesAsync}.
 *
 * <p>A factory bundles together "where the bytes go" (a destination buffer) and "what gets
 * charged for them". Backends only see this one-method abstraction, so they need not depend
 * on any specific buffer implementation. Production code uses {@link #forBreaker(CircuitBreaker)}
 * (heap {@code byte[]}, charged to the breaker, released on {@link DirectReadBuffer#close()}).
 * {@link #forAllocator(BufferAllocator)} is deprecated: Arrow-backed I/O is no longer the
 * production path.
 *
 * <p>{@link #allocate(int)} may throw {@link IOException} when the underlying allocator refuses
 * the allocation (circuit-breaker trip, OOM, etc.). On success the returned buffer is uninitialized
 * and the caller is responsible for closing it once consumption is complete or on the failure path.
 */
@FunctionalInterface
public interface DirectBufferFactory {
    DirectReadBuffer allocate(int length) throws IOException;

    /**
     * Breaker that is safe to charge from I/O / generic threads.
     * {@link LocalCircuitBreaker} is pinned to the driver thread; HTTP/S3 completion callbacks
     * must charge its parent request breaker instead. Identity for any other breaker.
     */
    static CircuitBreaker forAsyncIo(CircuitBreaker breaker) {
        if (breaker instanceof LocalCircuitBreaker local) {
            return local.parentBreaker();
        }
        return breaker;
    }

    /**
     * Returns a factory that allocates a heap {@code byte[]} of {@code length} bytes and charges
     * {@code breaker}. {@link DirectReadBuffer#close()} releases the charge. This is the production
     * bridge from a {@link CircuitBreaker} to {@link DirectBufferFactory}.
     *
     * <p>When {@code breaker} is a {@link LocalCircuitBreaker}, charges go to
     * {@link #forAsyncIo(CircuitBreaker)} so {@link StorageObject#readBytesAsync} completions
     * on generic threads do not trip the driver's single-thread assertion.
     */
    static DirectBufferFactory forBreaker(CircuitBreaker breaker) {
        return len -> DirectReadBuffer.allocate(breaker, len);
    }

    /**
     * Returns a factory that allocates from the given Arrow {@link BufferAllocator}.
     *
     * @deprecated production I/O uses {@link #forBreaker(CircuitBreaker)}. Kept for tests and
     *             Arrow-backed callers that still allocate off-heap.
     */
    @Deprecated
    static DirectBufferFactory forAllocator(BufferAllocator allocator) {
        return len -> DirectReadBuffer.allocate(allocator, len);
    }
}
