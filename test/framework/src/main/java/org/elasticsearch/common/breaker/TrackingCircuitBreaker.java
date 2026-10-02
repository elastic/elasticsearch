/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link NoopCircuitBreaker} that records how it was used. Use {@link #wasCalled()} to verify that a code path
 * consulted the breaker, and {@link #getUsed()} / {@link #peak()} to verify that reservations were charged and released.
 *
 * <p>With the default constructor nothing ever trips; pass a non-negative limit to trip once {@link #getUsed()} exceeds it.
 */
public class TrackingCircuitBreaker extends NoopCircuitBreaker {

    private final AtomicLong used = new AtomicLong();
    private final AtomicLong peak = new AtomicLong();
    private final long limit;

    public TrackingCircuitBreaker() {
        this(-1L);
    }

    /** @param limit the number of bytes above which reservations trip, or a negative value for no limit */
    public TrackingCircuitBreaker(long limit) {
        super("tracking");
        this.limit = limit;
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        long current = used.addAndGet(bytes);
        if (limit >= 0 && current > limit) {
            used.addAndGet(-bytes);
            throw new CircuitBreakingException("test breaker tripped", bytes, limit, Durability.TRANSIENT);
        }
        peak.accumulateAndGet(current, Math::max);
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

    /** The highest value {@link #getUsed()} reached through {@link #addEstimateBytesAndMaybeBreak}. */
    public long peak() {
        return peak.get();
    }

    /** Returns {@code true} if {@link #addEstimateBytesAndMaybeBreak} was called at least once. */
    public boolean wasCalled() {
        return peak.get() > 0;
    }

    /** Resets the tracking state so the instance can be reused across sub-tests. */
    public void reset() {
        used.set(0);
        peak.set(0);
    }
}
