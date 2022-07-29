/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregator;

import java.util.function.Consumer;

/**
 * Special purpose circuit breaker, don't reuse it.
 *
 * This breaker service is written for aggregators which use big arrays that have a lifespan beyond the mapping phase.
 *
 * The breaker service and breaker wraps another breaker - namely the one from the aggregation context - which has a shorter
 * lifetime than the objects using this breaker. Using the breaker from the aggregation context ensures that we actually
 * break if the agg is running out of memory.
 *
 * TL/DR
 *
 * (Using the non-recycling big array instance does not trip the circuit breaker, it is not only non-recycling, but also
 * non-accounting.)
 *
 * After the mapping phase, when the aggregation context gets destructed, see {@link ItemSetMapReduceAggregator#doClose()}, all objects
 * are normally freed. "a lifespan beyond the mapping phase" - however means, we still have to keep internal big array objects
 * _after_ `doClose()` got called. This adapter ensures objects aren't freed. In order to not double-account freed memory
 * the agg context gets disconnected in `doClose()`.
 *
 * (Circuit breaker additions must always be in sync, you absolutely must deduct exactly the same number of allocated bytes
 * you added before, including exceptional cases like request cancellation or exceptions. gh#67476 might tackle this issue.
 * At the time of writing circuit breakers are a global gauge.)
 *
 * After the map phase and before reduce, the {@link ItemSetMapReduceAggregator} creates instances of
 * {@link InternalItemSetMapReduceAggregation}, see {@link ItemSetMapReduceAggregator#buildAggregations(long[])}.
 *
 * (Note 1: Instead of keeping the existing instance, it would have been possible to deep-copy the object like
 * {@link CardinalityAggregator#buildAggregations(long[])}. I decided against this approach mainly because the deep-copy isn't
 * secured by circuit breakers, meaning the node could run out of memory during the deep-copy.)
 * (Note 2: Between {@link ItemSetMapReduceAggregator#doClose()} and serializing {@link InternalItemSetMapReduceAggregation}
 * memory accounting is broken, meaning the agg context gets closed and bytes get returned to the circuit breaker before memory is
 * actually freed. An incoming expensive request could potentially arrive during that window of time. However, this scenario is less
 * likely than the out of memory during deep-copy)
 *
 * Summing up, this class workarounds problems in search/aggs. It should not exist and eventually it should be removed.
 *
 * Further follow up is tracked in gh#88128
 */
final class DelegatingCircuitBreakerService extends CircuitBreakerService {

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
