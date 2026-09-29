/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link NoopCircuitBreaker} that records whether {@link #addEstimateBytesAndMaybeBreak}
 * was ever called. Use {@link #wasCalled()} in assertions to verify that a code path
 * consulted the circuit breaker.
 */
public class TrackingCircuitBreaker extends NoopCircuitBreaker {

    private final AtomicBoolean called = new AtomicBoolean();

    public TrackingCircuitBreaker() {
        super("tracking");
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        called.set(true);
    }

    /** Returns {@code true} if {@link #addEstimateBytesAndMaybeBreak} was called at least once. */
    public boolean wasCalled() {
        return called.get();
    }

    /** Resets the tracking state so the instance can be reused across sub-tests. */
    public void reset() {
        called.set(false);
    }
}
