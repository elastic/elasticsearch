/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A CircuitBreaker that counts how often memory was requested.
 * Delegates to another circuit breaker
 */
public class CountingCircuitBreaker implements CircuitBreaker {
    CircuitBreaker delegate;
    AtomicLong memoryRequestCount;

    public CountingCircuitBreaker(CircuitBreaker delegate) {
        this.delegate = delegate;
        memoryRequestCount = new AtomicLong(0);
    }

    public long getMemoryRequestCount() {
        return memoryRequestCount.get();
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        delegate.circuitBreak(fieldName, bytesNeeded);
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        incMemoryRequestCountIfPositive(bytes);
        delegate.addEstimateBytesAndMaybeBreak(bytes, label);
    }

    @Override
    public void checkRealMemoryUsage(String label) throws CircuitBreakingException {
        delegate.checkRealMemoryUsage(label);
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        incMemoryRequestCountIfPositive(bytes);
        delegate.addWithoutBreaking(bytes);
    }

    @Override
    public long getUsed() {
        return delegate.getUsed();
    }

    @Override
    public long getLimit() {
        return delegate.getLimit();
    }

    @Override
    public double getOverhead() {
        return delegate.getOverhead();
    }

    @Override
    public long getTrippedCount() {
        return delegate.getTrippedCount();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public Durability getDurability() {
        return delegate.getDurability();
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        delegate.setLimitAndOverhead(limit, overhead);
    }

    private void incMemoryRequestCountIfPositive(long bytes) {
        if (bytes > 0) {
            memoryRequestCount.incrementAndGet();
        }
    }
}
