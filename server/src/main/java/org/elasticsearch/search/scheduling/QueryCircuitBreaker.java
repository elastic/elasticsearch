/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The memory of one query on a node. Every byte it counts is also on the {@link CircuitBreaker#REQUEST} breaker, which
 * decides whether a charge breaks. Callers see the request breaker: {@link #getUsed()} and {@link #getLimit()} are its.
 * The query's own bytes are {@link #usedBytes()}.
 */
final class QueryCircuitBreaker implements CircuitBreaker {
    private final CircuitBreaker parent;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong peak = new AtomicLong();
    private final Runnable onReleased;

    /**
     * @param onReleased called when a release leaves the query with no bytes
     */
    QueryCircuitBreaker(CircuitBreaker parent, Runnable onReleased) {
        this.parent = parent;
        this.onReleased = onReleased;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        parent.circuitBreak(fieldName, bytesNeeded);
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        parent.addEstimateBytesAndMaybeBreak(bytes, label);
        add(bytes);
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        parent.addWithoutBreaking(bytes);
        add(bytes);
    }

    @Override
    public void addWithoutBreaking(long bytes, String label) {
        parent.addWithoutBreaking(bytes, label);
        add(bytes);
    }

    @Override
    public void moveHeld(long bytes, String fromLabel, String toLabel) {
        parent.moveHeld(bytes, fromLabel, toLabel);
    }

    private void add(long bytes) {
        long current = used.addAndGet(bytes);
        assert current >= 0 : "the query released more than it charged: " + current;
        if (bytes > 0) {
            updatePeak(current);
        } else if (bytes < 0 && current == 0) {
            onReleased.run();
        }
    }

    private void updatePeak(long current) {
        long max = peak.get();
        while (current > max && peak.compareAndSet(max, current) == false) {
            max = peak.get();
        }
    }

    /**
     * The bytes the query holds on this node.
     */
    long usedBytes() {
        return used.get();
    }

    /**
     * The most bytes the query held at once on this node.
     */
    long peakBytes() {
        return peak.get();
    }

    @Override
    public long getUsed() {
        return parent.getUsed();
    }

    @Override
    public long getLimit() {
        return parent.getLimit();
    }

    @Override
    public double getOverhead() {
        return parent.getOverhead();
    }

    @Override
    public long getTrippedCount() {
        return parent.getTrippedCount();
    }

    @Override
    public String getName() {
        return parent.getName();
    }

    @Override
    public Durability getDurability() {
        return parent.getDurability();
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        throw new UnsupportedOperationException("the limits of a query breaker are its parent's");
    }

    /**
     * Returns the query's breaker as the request breaker, and the node's breakers otherwise.
     */
    static final class Service extends CircuitBreakerService {
        private final CircuitBreakerService next;
        private final QueryCircuitBreaker queryBreaker;

        Service(CircuitBreakerService next, QueryCircuitBreaker queryBreaker) {
            this.next = next;
            this.queryBreaker = queryBreaker;
        }

        @Override
        public CircuitBreaker getBreaker(String name) {
            return CircuitBreaker.REQUEST.equals(name) ? queryBreaker : next.getBreaker(name);
        }

        @Override
        public AllCircuitBreakerStats stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CircuitBreakerStats stats(String name) {
            throw new UnsupportedOperationException();
        }
    }
}
