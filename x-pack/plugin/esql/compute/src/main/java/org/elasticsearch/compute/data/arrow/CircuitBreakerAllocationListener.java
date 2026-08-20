/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Arrow allocation listener that uses a circuit breaker to track memory usage.
 * <p>
 * Arrow's {@code BaseAllocator} charges {@link #onPreAllocation} before checking
 * {@code maxAllocation}. When that cap fails, {@link #onFailedAllocation} undoes
 * the breaker charge and throws {@link org.elasticsearch.common.breaker.CircuitBreakingException}
 * so the query returns 429 rather than Arrow's {@code OutOfMemoryException} (HTTP 500).
 */

// Note: it would more naturally fit in the :libs:arrow module, but would require a dependency
// on :server: which could cause circular dependencies issues. A solution would be to move
// the CircuitBreaker interface to :libs:core
public record CircuitBreakerAllocationListener(CircuitBreaker circuitBreaker) implements AllocationListener {

    @Override
    public void onPreAllocation(long size) {
        circuitBreaker.addEstimateBytesAndMaybeBreak(size, "Arrow allocator");
    }

    @Override
    public void onRelease(long size) {
        circuitBreaker.addWithoutBreaking(-size);
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
        circuitBreaker.addWithoutBreaking(-size);
        throw DirectBufferAllocationManager.circuitBreakingException(size, failedAllocatorLimit(outcome), null);
    }

    private static long failedAllocatorLimit(AllocationOutcome outcome) {
        if (outcome == null) {
            return 0L;
        }
        var details = outcome.getDetails();
        if (details.isEmpty()) {
            return 0L;
        }
        var failed = details.get().getFailedAllocator();
        return failed == null ? 0L : failed.getLimit();
    }
}
