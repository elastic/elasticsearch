/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.function.Consumer;

/**
 * Special purpose circuit breaker, don't reuse it.
 *
 * This breaker service and breaker wraps another breaker that has a shorter lifetime than the
 * objects using this breaker. Therefore it is possible to disconnect the wrapped breaker.
 *
 * This class is only used to workaround a problem in search/aggs and should eventually be
 * removed once search/aggs fixes problems.
 */
public class DelegatingCircuitBreakerService extends CircuitBreakerService {

    private static class DelegatingCircuitBreaker implements CircuitBreaker {

        private CircuitBreaker wrappedBreaker;
        private final Consumer<Long> addToBreakerOverwrite;
        private final String name;

        DelegatingCircuitBreaker(CircuitBreaker wrappedBreaker, Consumer<Long> addToBreakerOverwrite) {
            this.wrappedBreaker = wrappedBreaker;
            this.addToBreakerOverwrite = addToBreakerOverwrite;
            this.name = wrappedBreaker.getName();
        }

        void disconnect() {
            wrappedBreaker = null;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            if (wrappedBreaker != null) {
                wrappedBreaker.circuitBreak(fieldName, bytesNeeded);
            }
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (wrappedBreaker != null) {
                addToBreakerOverwrite.accept(bytes);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            if (wrappedBreaker != null) {
                addToBreakerOverwrite.accept(bytes);
            }
        }

        @Override
        public long getUsed() {
            if (wrappedBreaker != null) {
                return wrappedBreaker.getUsed();
            }
            return 0;
        }

        @Override
        public long getLimit() {
            if (wrappedBreaker != null) {
                return wrappedBreaker.getLimit();
            }
            return 0;
        }

        @Override
        public double getOverhead() {
            if (wrappedBreaker != null) {
                return wrappedBreaker.getOverhead();
            }
            return 0.0;
        }

        @Override
        public long getTrippedCount() {
            if (wrappedBreaker != null) {
                return wrappedBreaker.getTrippedCount();
            }
            return 0;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Durability getDurability() {
            if (wrappedBreaker != null) {
                return wrappedBreaker.getDurability();
            }
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            if (wrappedBreaker != null) {
                wrappedBreaker.setLimitAndOverhead(limit, overhead);
            }
        }
    }

    private final DelegatingCircuitBreaker breaker;

    DelegatingCircuitBreakerService(CircuitBreaker wrappedBreaker, Consumer<Long> addToBreakerOverwrite) {
        this.breaker = new DelegatingCircuitBreaker(wrappedBreaker, addToBreakerOverwrite);
    }

    void disconnect() {
        breaker.disconnect();
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        // ignore the name
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(breaker.getName()) });
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        return new CircuitBreakerStats(name, breaker.getLimit(), 0, breaker.getOverhead(), breaker.getTrippedCount());
    }

}
