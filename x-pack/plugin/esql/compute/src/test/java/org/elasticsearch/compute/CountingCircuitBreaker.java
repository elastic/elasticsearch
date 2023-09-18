/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.common.breaker.CircuitBreaker;

public class CountingCircuitBreaker implements CircuitBreaker {

    private final String name;

    private long total;

    public CountingCircuitBreaker(String name) {
        this.name = name;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        // noop
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
        total += bytes;
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        total += bytes;
    }

    @Override
    public long getUsed() {
        return total;
    }

    @Override
    public long getLimit() {
        return -1;
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
        return this.name;
    }

    @Override
    public Durability getDurability() {
        return Durability.PERMANENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {}
}
