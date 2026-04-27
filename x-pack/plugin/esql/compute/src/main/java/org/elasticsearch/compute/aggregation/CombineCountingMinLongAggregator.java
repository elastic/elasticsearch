/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only aggregator that behaves like {@link MinLongAggregator} but counts
 * how many times {@link #combine} is called. Used to verify that the
 * {@code combineOnceForConstant} optimisation short-circuits constant vectors.
 */
@Aggregator(
    value = { @IntermediateState(name = "min", type = "LONG"), @IntermediateState(name = "seen", type = "BOOLEAN") },
    combineOnceForConstant = true
)
@GroupingAggregator(combineOnceForConstant = true)
class CombineCountingMinLongAggregator {

    static final AtomicInteger COMBINE_CALL_COUNT = new AtomicInteger();

    public static long init() {
        return Long.MAX_VALUE;
    }

    public static long combine(long current, long v) {
        COMBINE_CALL_COUNT.incrementAndGet();
        return Math.min(current, v);
    }
}
