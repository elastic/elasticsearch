/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.breaker;

/**
 * A CircuitBreaker that doesn't increment or adjust, and all operations are
 * basically noops
 */
public class NoopCircuitBreaker implements CircuitBreaker {
    public static final int LIMIT = -1;

    private final String name;

    public NoopCircuitBreaker(String name) {
        this.name = name;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        // noop
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {}

    @Override
    public void addWithoutBreaking(long bytes) {}

    @Override
    public long getUsed() {
        return 0;
    }

    @Override
    public long getLimit() {
        return LIMIT;
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
