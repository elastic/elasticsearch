/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Parquet {@code ByteBufferAllocator} that charges allocations to a circuit breaker.
 * Callers that may allocate off the driver thread must pass
 * {@link org.elasticsearch.compute.data.LocalCircuitBreaker#forAsyncIo(CircuitBreaker)} themselves;
 * this class does not rewrite the breaker.
 *
 * <p>{@link #release} uncharges only the outstanding checkout for this buffer identity, then
 * invokes the delegate. A stale second {@code release} of a previous checkout does not steal the
 * reservation of a later allocate. Uncharge still runs before {@code delegate.release} so a
 * pooling delegate cannot hand the backing array to another thread before this reservation drops.
 */
public class CircuitBreakerByteBufferAllocator implements ByteBufferAllocator {
    private final ByteBufferAllocator delegate;
    private final CircuitBreaker breaker;
    /**
     * Charged capacity per buffer identity handed out by {@link #allocate}. Reference identity
     * because {@link ByteBuffer#equals} compares contents. The keys are held strongly, but unlike
     * the node-wide {@link PoolingHeapByteBufferAllocator} (which tracks its checkouts weakly for
     * this reason), this allocator is created per read-options build — see
     * {@code ParquetFormatReader#readOptionsBuilder} — so a checkout parquet-mr never releases is
     * pinned at most for that file open's lifetime, along with its breaker charge (which a missed
     * release leaked before this map existed too).
     */
    private final ConcurrentHashMap<Identity, Integer> outstanding = new ConcurrentHashMap<>();

    public CircuitBreakerByteBufferAllocator(ByteBufferAllocator delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    ByteBufferAllocator delegate() {
        return delegate;
    }

    @Override
    public ByteBuffer allocate(int capacity) {
        ByteBuffer buffer = null;
        breaker.addEstimateBytesAndMaybeBreak(capacity, "parquet reader");
        try {
            buffer = delegate.allocate(capacity);
        } finally {
            if (buffer == null) {
                // Failed to allocate, but we reserved that space.
                breaker.addWithoutBreaking(-capacity);
            }
        }

        // Capacity may have been rounded up.
        boolean success = false;
        var difference = buffer.capacity() - capacity;
        if (difference != 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(difference, "parquet reader");
                success = true;
            } finally {
                if (success == false) {
                    // Couldn't charge the extra capacity. Uncharge the original reservation
                    // before release: a pooling delegate may reuse the backing array immediately.
                    breaker.addWithoutBreaking(-capacity);
                    delegate.release(buffer);
                }
            }
        }
        Integer previous = outstanding.put(new Identity(buffer), buffer.capacity());
        if (previous != null) {
            throw new IllegalStateException("checked out a buffer that is already charged");
        }
        return buffer;
    }

    @Override
    public void release(ByteBuffer byteBuffer) {
        Integer charged = outstanding.remove(new Identity(byteBuffer));
        if (charged != null) {
            // Uncharge before the delegate may reuse the backing array.
            breaker.addWithoutBreaking(-charged);
        }
        delegate.release(byteBuffer);
    }

    @Override
    public boolean isDirect() {
        return delegate.isDirect();
    }

    private static final class Identity {
        private final ByteBuffer buffer;

        Identity(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Identity other && other.buffer == buffer;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(buffer);
        }
    }
}
