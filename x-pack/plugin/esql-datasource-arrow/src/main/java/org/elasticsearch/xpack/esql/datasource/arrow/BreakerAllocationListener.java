/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * Arrow {@link AllocationListener} that delegates to a {@link CircuitBreaker}
 * so that Arrow buffer memory is tracked by Elasticsearch's circuit breaker infrastructure.
 *
 * <p>{@link #onPreAllocation(long)} reserves space on the breaker before the allocation happens,
 * which may throw a {@code CircuitBreakingException} to abort the allocation.
 * {@link #onRelease(long)} releases the corresponding amount when Arrow frees the buffer.
 */
final class BreakerAllocationListener implements AllocationListener {
    private final CircuitBreaker breaker;

    BreakerAllocationListener(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    @Override
    public void onPreAllocation(long size) {
        breaker.addEstimateBytesAndMaybeBreak(size, "<arrow_buffer>");
    }

    @Override
    public void onRelease(long size) {
        breaker.addWithoutBreaking(-size);
    }
}
