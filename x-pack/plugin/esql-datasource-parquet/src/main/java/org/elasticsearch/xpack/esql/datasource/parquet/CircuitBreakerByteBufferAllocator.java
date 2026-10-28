/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;

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
 *
 * <p>For heap delegates the charge is the backing array's {@linkplain HeapFootprint#byteArrayBytes(long)
 * heap footprint} rather than its capacity, so a humongous slab is charged the whole G1 regions it occupies.
 */
public class CircuitBreakerByteBufferAllocator implements ByteBufferAllocator {
    private final ByteBufferAllocator delegate;
    private final CircuitBreaker breaker;
    /**
     * Charged bytes per buffer identity handed out by {@link #allocate}. Reference identity
     * because {@link ByteBuffer#equals} compares contents. The keys are held strongly, but unlike
     * the node-wide {@link PoolingHeapByteBufferAllocator} (which tracks its checkouts weakly for
     * this reason), this allocator is created per read-options build — see
     * {@code ParquetFormatReader#readOptionsBuilder} — so a checkout parquet-mr never releases is
     * pinned at most for that file open's lifetime, along with its breaker charge (which a missed
     * release leaked before this map existed too).
     */
    private final ConcurrentHashMap<Identity, Long> outstanding = new ConcurrentHashMap<>();

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
        long requestedCharge = charge(capacity);
        breaker.addEstimateBytesAndMaybeBreak(requestedCharge, "parquet reader");
        try {
            buffer = delegate.allocate(capacity);
        } finally {
            if (buffer == null) {
                // Failed to allocate, but we reserved that space.
                breaker.addWithoutBreaking(-requestedCharge);
            }
        }

        // Capacity may have been rounded up.
        boolean success = false;
        long charged = charge(buffer.capacity());
        var difference = charged - requestedCharge;
        if (difference != 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(difference, "parquet reader");
                success = true;
            } finally {
                if (success == false) {
                    // Couldn't charge the extra capacity. Uncharge the original reservation
                    // before release: a pooling delegate may reuse the backing array immediately.
                    breaker.addWithoutBreaking(-requestedCharge);
                    delegate.release(buffer);
                }
            }
        }
        Long previous = outstanding.put(new Identity(buffer), charged);
        if (previous != null) {
            // Unreachable with the current delegates (both hand out fresh instances); if a delegate
            // ever recycles a live identity, don't let the invariant failure also strand this
            // allocation's charge and checkout. The previous charge is unrecoverable by design.
            outstanding.remove(new Identity(buffer));
            breaker.addWithoutBreaking(-charged);
            delegate.release(buffer);
            throw new IllegalStateException("checked out a buffer that is already charged");
        }
        return buffer;
    }

    @Override
    public void release(ByteBuffer byteBuffer) {
        Long charged = outstanding.remove(new Identity(byteBuffer));
        if (charged != null) {
            // Uncharge before the delegate may reuse the backing array.
            breaker.addWithoutBreaking(-charged);
        }
        delegate.release(byteBuffer);
    }

    /**
     * Bytes to charge for a buffer of {@code capacity}: the backing array's heap footprint for heap delegates,
     * the capacity itself for direct ones (off-heap memory has no array header or G1 regions).
     */
    private long charge(int capacity) {
        return delegate.isDirect() ? capacity : HeapFootprint.byteArrayBytes(capacity);
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
