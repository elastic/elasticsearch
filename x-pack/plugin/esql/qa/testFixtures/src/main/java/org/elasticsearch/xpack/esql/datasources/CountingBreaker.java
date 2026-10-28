/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.Arrays;

/**
 * A {@link CircuitBreaker} that never trips but counts reservations, so tests can assert how many
 * times byte storage was reserved during a build, and how big those reservations were. Positive
 * deltas are reservations; negative deltas are releases. Call {@link #reset()} between the setup
 * phase and the code under test so only production-path reservations are counted.
 */
public final class CountingBreaker implements CircuitBreaker {
    private long used;
    private int positiveReservations;
    private long[] reservations = new long[16];

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

    /**
     * How many single reservations were at least {@code minBytes}. Lets a test assert that a builder was
     * sized for what it actually holds: a reader that sizes a per-record buffer for a whole page makes one
     * page-sized reservation per record instead of one per page, which {@link #positiveReservations()}
     * alone cannot distinguish from a reader that simply makes many small ones.
     */
    public int reservationsOfAtLeast(long minBytes) {
        int count = 0;
        for (int i = 0; i < positiveReservations; i++) {
            if (reservations[i] >= minBytes) {
                count++;
            }
        }
        return count;
    }

    /**
     * A {@link CircuitBreakerService} handing out this breaker, for wiring into a {@code MockBigArrays} so that
     * big-array byte storage — which a {@code BlockFactory}'s own breaker never sees — is counted too.
     */
    public CircuitBreakerService service() {
        return new CircuitBreakerService() {
            @Override
            public CircuitBreaker getBreaker(String name) {
                return CountingBreaker.this;
            }

            @Override
            public AllCircuitBreakerStats stats() {
                throw new UnsupportedOperationException();
            }

            @Override
            public CircuitBreakerStats stats(String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void record(long bytes) {
        used += bytes;
        if (bytes > 0) {
            if (positiveReservations == reservations.length) {
                reservations = Arrays.copyOf(reservations, positiveReservations * 2);
            }
            reservations[positiveReservations] = bytes;
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
