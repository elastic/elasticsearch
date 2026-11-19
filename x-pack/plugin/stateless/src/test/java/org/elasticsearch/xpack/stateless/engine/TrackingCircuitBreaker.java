/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only {@link CircuitBreaker} that tracks reservations and trips on a configurable limit. Used by tests
 * that want to inspect reserved bytes or exercise the deferral path without spinning up a full
 * {@link org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService}.
 *
 * <p>The default limit is {@code -1} (no enforcement). Tests can call {@link #setLimit} to engage the breakable
 * path.
 */
public final class TrackingCircuitBreaker implements CircuitBreaker {

    private final String name;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong tripped = new AtomicLong();
    private volatile long limit;

    public TrackingCircuitBreaker(String name, long limit) {
        this.name = name;
        this.limit = limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        tripped.incrementAndGet();
        throw new CircuitBreakingException(
            "tracking breaker tripped on [" + fieldName + "] for [" + bytesNeeded + "] bytes",
            bytesNeeded,
            limit,
            Durability.TRANSIENT
        );
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        long newUsed = used.addAndGet(bytes);
        if (limit != -1 && newUsed > limit) {
            used.addAndGet(-bytes);
            circuitBreak(label, bytes);
        }
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        used.addAndGet(bytes);
    }

    @Override
    public long getUsed() {
        return used.get();
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public double getOverhead() {
        return 1.0;
    }

    @Override
    public long getTrippedCount() {
        return tripped.get();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Durability getDurability() {
        return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        this.limit = limit;
    }
}
