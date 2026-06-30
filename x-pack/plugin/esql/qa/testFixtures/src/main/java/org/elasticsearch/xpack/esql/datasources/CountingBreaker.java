/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

/**
 * A {@link CircuitBreaker} that never trips but counts reservations, so tests can assert how many
 * times byte storage was reserved during a build. Positive deltas are reservations; negative deltas
 * are releases. Call {@link #reset()} between the setup phase and the code under test so only
 * production-path reservations are counted.
 */
public final class CountingBreaker implements CircuitBreaker {
    private long used;
    private int positiveReservations;

    public void reset() {
        used = 0;
        positiveReservations = 0;
    }

    public long used() {
        return used;
    }

    public int positiveReservations() {
        return positiveReservations;
    }

    private void record(long bytes) {
        used += bytes;
        if (bytes > 0) {
            positiveReservations++;
        }
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {}

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        record(bytes);
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        record(bytes);
    }

    @Override
    public long getUsed() {
        return used;
    }

    @Override
    public long getLimit() {
        return Long.MAX_VALUE;
    }

    @Override
    public double getOverhead() {
        return 1.0;
    }

    @Override
    public long getTrippedCount() {
        return 0;
    }

    @Override
    public String getName() {
        return CircuitBreaker.REQUEST;
    }

    @Override
    public Durability getDurability() {
        return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {}
}
