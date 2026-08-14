/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.util.MemoryUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Factory for creating Arrow {@link RootAllocator}s that integrate with an Elasticsearch
 * {@link CircuitBreaker} for native-memory accounting.
 */

// Note: it would more naturally fit in the :libs:arrow module, but would require a dependency
// on :server: which could cause circular dependencies issues. A solution would be to move
// the CircuitBreaker interface to :libs:core
public final class CircuitBreakingArrowAllocator {

    private CircuitBreakingArrowAllocator() {}

    /**
     * Creates a {@link RootAllocator} with the default Arrow rounding policy.
     */
    public static RootAllocator create(CircuitBreaker circuitBreaker) {
        return create(circuitBreaker, DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY, null);
    }

    /**
     * Creates a {@link RootAllocator} with the given rounding policy.
     */
    public static RootAllocator create(CircuitBreaker circuitBreaker, RoundingPolicy roundingPolicy) {
        return create(circuitBreaker, roundingPolicy, null);
    }

    /**
     * Creates a {@link RootAllocator}, optionally overriding the base {@link AllocationManager.Factory}.
     * Pass {@code null} for {@code factory} to use the default unsafe allocator.
     * This overload exists for tests that need to inject a controlled factory.
     */
    static RootAllocator create(CircuitBreaker circuitBreaker, RoundingPolicy roundingPolicy, AllocationManager.Factory factory) {
        var base = factory != null ? factory : defaultFactory();
        return new RootAllocator(
            RootAllocator.configBuilder()
                .listener(breakingListener(circuitBreaker))
                .allocationManagerFactory(oomCorrecting(circuitBreaker, base))
                .maxAllocation(Long.MAX_VALUE)
                .roundingPolicy(roundingPolicy)
                .build()
        );
    }

    /**
     * Returns an {@link AllocationManager.Factory} that is functionally equivalent to Arrow's
     * {@code UnsafeAllocationManager.FACTORY} but performs the native allocation <em>before</em>
     * calling {@code super(accountingAllocator)}, so an {@link OutOfMemoryError} from
     * {@link MemoryUtil#allocateMemory} escapes {@link AllocationManager.Factory#create} without
     * any half-constructed {@link AllocationManager} being registered with the allocator.
     */
    static AllocationManager.Factory defaultFactory() {
        return new AllocationManager.Factory() {
            @Override
            public AllocationManager create(BufferAllocator accountingAllocator, long size) {
                final long address = MemoryUtil.allocateMemory(size);
                return new AllocationManager(accountingAllocator) {
                    @Override
                    public long getSize() {
                        return size;
                    }

                    @Override
                    protected long memoryAddress() {
                        return address;
                    }

                    @Override
                    protected void release0() {
                        MemoryUtil.freeMemory(address);
                    }
                };
            }

            @Override
            public ArrowBuf empty() {
                return EMPTY;
            }
        };
    }

    private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP, null, 0, MemoryUtil.allocateMemory(0));

    /**
     * Wraps {@code base} so that any {@code Throwable} thrown from
     * {@link AllocationManager.Factory#create} is caught and the pre-allocation charge is refunded
     * to {@code breaker} before the error is re-thrown.
     */
    static AllocationManager.Factory oomCorrecting(CircuitBreaker breaker, AllocationManager.Factory base) {
        return new AllocationManager.Factory() {
            @Override
            public AllocationManager create(BufferAllocator accountingAllocator, long size) {
                var success = false;
                try {
                    var result = base.create(accountingAllocator, size);
                    success = true;
                    return result;
                } finally {
                    if (!success) {
                        breaker.addWithoutBreaking(-size);
                    }
                }
            }

            @Override
            public ArrowBuf empty() {
                return base.empty();
            }
        };
    }

    static AllocationListener breakingListener(CircuitBreaker circuitBreaker) {
        return new AllocationListener() {
            @Override
            public void onPreAllocation(long size) {
                circuitBreaker.addEstimateBytesAndMaybeBreak(size, "Arrow allocator");
            }

            @Override
            public void onRelease(long size) {
                circuitBreaker.addWithoutBreaking(-size);
            }
        };
    }
}
