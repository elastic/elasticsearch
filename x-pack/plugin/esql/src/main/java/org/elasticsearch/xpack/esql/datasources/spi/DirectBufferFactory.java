/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;

/**
 * Producer of {@link DirectReadBuffer}s for {@link StorageObject#readBytesAsync}.
 *
 * <p>A factory bundles together "where the bytes go" (a destination off-heap buffer) and "what
 * gets charged for them" (the underlying allocator / circuit breaker). Backends only see this
 * one-method abstraction, so they need not depend on any specific direct-memory implementation
 * (Arrow today, anything else later). The bridge from a concrete allocator to this interface is
 * confined to {@link #forAllocator(BufferAllocator)} and to call sites that build a factory
 * differently (e.g. tests that fake the allocation).
 *
 * <p>{@link #allocate(int)} may throw {@link IOException} when the underlying allocator refuses
 * the allocation (circuit-breaker trip, OOM, etc.). On success the returned buffer is uninitialized
 * and the caller is responsible for closing it once consumption is complete or on the failure path.
 */
@FunctionalInterface
public interface DirectBufferFactory {
    DirectReadBuffer allocate(int length) throws IOException;

    /**
     * Returns a factory that allocates from the given Arrow {@link BufferAllocator}. This is the
     * one supported bridge from a concrete allocator to {@link DirectBufferFactory}; production
     * code calls it once at the boundary that owns the allocator (currently
     * {@code CoalescedRangeReader}).
     */
    static DirectBufferFactory forAllocator(BufferAllocator allocator) {
        return len -> DirectReadBuffer.allocate(allocator, len);
    }
}
