/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.test.ESTestCase;

/**
 * {@link CircuitBreakerService} that fails one twentieth of the time when you
 * add bytes. This is useful to make sure code responds sensibly to circuit
 * breaks at unpredictable times.
 */
public class CrankyCircuitBreakerService extends CircuitBreakerService {
    /**
     * Error message thrown when the breaker randomly trips.
     */
    public static final String ERROR_MESSAGE = "cranky breaker";

    private final CircuitBreaker breaker = new CircuitBreaker() {
        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {

        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (ESTestCase.random().nextInt(20) == 0) {
                throw new CircuitBreakingException(ERROR_MESSAGE, Durability.PERMANENT);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {

        }

        @Override
        public long getUsed() {
            return 0;
        }

        @Override
        public long getLimit() {
            return 0;
        }

        @Override
        public double getOverhead() {
            return 0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return CircuitBreaker.FIELDDATA;
        }

        @Override
        public Durability getDurability() {
            return null;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {

        }
    };

    @Override
    public CircuitBreaker getBreaker(String name) {
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.FIELDDATA) });
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
    }
}
