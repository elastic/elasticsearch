/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.MemorySizeValue;

/**
 * Bounds one pattern compilation to a fixed share of the heap. The optimizer has no request breaker to charge: it runs
 * before any search context exists, and this module cannot see the ES|QL fold context that plays this role there. So
 * each build gets its own budget, held only while the automaton is built, and a pattern that exceeds it is the user's
 * problem: a client error, not a breaker trip.
 */
final class AutomatonBudget implements CircuitBreaker {

    /** Five percent of the heap, the share ES|QL grants a constant fold; many concurrent builds still cannot each exceed it. */
    static final long LIMIT = MemorySizeValue.parseBytesSizeValueOrHeapRatio("5%", "automaton_budget").getBytes();

    private long used;

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        if (used + bytes > LIMIT) {
            throw new IllegalArgumentException(
                "Pattern is too large to compile: it needs [" + (used + bytes) + "] bytes, the limit is [" + LIMIT + "]"
            );
        }
        used += bytes;
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        used += bytes;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getUsed() {
        return used;
    }

    @Override
    public long getLimit() {
        return LIMIT;
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
        return "automaton_budget";
    }

    @Override
    public Durability getDurability() {
        return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        throw new UnsupportedOperationException();
    }
}
