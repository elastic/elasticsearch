/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

/**
 * A {@link CircuitBreakerService} whose breaker refuses every n-th allocation request and accounts for all the
 * others. It exists to exercise callers that treat a trip as a per-item rejection and keep using the same data
 * structure, which is the path on which a structure left half-updated by a refused allocation does its damage.
 * <p>
 * {@link LimitedBreaker} cannot stand in for it: its limit is fixed for the lifetime of the breaker, so once it
 * is full every later request is refused too and the structure never gets the chance to recover.
 * <p>
 * Refusals are off until {@link #startRefusing} is called, so a structure can be built without the test having
 * to know how many allocations construction makes.
 */
public class RefusingCircuitBreakerService extends CircuitBreakerService {

    private final RefusingCircuitBreaker breaker;

    /**
     * @param refuseEveryNth refuse allocation requests whose one-based index is a multiple of this, counting
     *                       from the {@link #startRefusing} call
     */
    public RefusingCircuitBreakerService(int refuseEveryNth) {
        this.breaker = new RefusingCircuitBreaker(refuseEveryNth);
    }

    public void startRefusing() {
        breaker.refusing = true;
        breaker.calls = 0;
    }

    public void stopRefusing() {
        breaker.refusing = false;
    }

    /**
     * How many allocation requests have been refused. Assert on this: a structure that grows geometrically may
     * make too few allocations for the refusal to ever fire, leaving the test passing without covering anything.
     */
    public int refusals() {
        return breaker.refusals;
    }

    /**
     * Asserts that every byte reserved against this breaker has been handed back, which after a run that closed
     * its structures means nothing leaked. Worth calling from any test that provokes refusals: a refused
     * allocation that keeps its reservation costs a caller which treats the trip as a per-item rejection a little
     * headroom every time, until the breaker trips for good.
     * <p>
     * {@link MockBigArrays} cannot check this. Its leak tracking only records arrays it wrapped itself, so
     * anything reserving through {@link BigArrays#adjustBreaker} directly is invisible to it.
     */
    public void assertNoResidualReservation() {
        MatcherAssert.assertThat("circuit breaker reservation was not fully released", breaker.getUsed(), Matchers.equalTo(0L));
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.REQUEST) });
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        return new CircuitBreakerStats(CircuitBreaker.REQUEST, -1, breaker.getUsed(), 0, 0);
    }

    private static class RefusingCircuitBreaker extends NoopCircuitBreaker {

        private final int refuseEveryNth;
        private boolean refusing;
        private long used;
        private int calls;
        private int refusals;

        RefusingCircuitBreaker(int refuseEveryNth) {
            super(CircuitBreaker.REQUEST);
            this.refuseEveryNth = refuseEveryNth;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (refusing && ++calls % refuseEveryNth == 0) {
                ++refusals;
                throw new CircuitBreakingException("refused allocation [" + calls + "]", bytes, used, Durability.TRANSIENT);
            }
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }
    }
}
