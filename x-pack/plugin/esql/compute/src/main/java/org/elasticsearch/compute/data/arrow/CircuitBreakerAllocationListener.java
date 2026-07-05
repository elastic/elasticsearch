/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Arrow allocation listener that uses a circuit breaker to track memory usage.
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
}
