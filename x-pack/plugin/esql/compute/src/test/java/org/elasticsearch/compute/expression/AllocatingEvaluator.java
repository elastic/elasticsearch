/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

/**
 * {@link ExpressionEvaluator} that allocates a single byte for every {@link Page}
 * it receives and releases it when {@link #close closed}. Use it to test that
 * things that use {@link ExpressionEvaluator}s properly close them.
 */
public class AllocatingEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(AllocatingEvaluator.class);

    private final CircuitBreaker breaker;
    private final ExpressionEvaluator next;
    private long allocated;

    public AllocatingEvaluator(CircuitBreaker breaker, ExpressionEvaluator next) {
        this.breaker = breaker;
        this.next = next;
    }

    @Override
    public Block eval(Page page) {
        breaker.addEstimateBytesAndMaybeBreak(1, "test");
        allocated++;
        return next.eval(page);
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-allocated);
    }
}
