/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.compute.ann.RuntimeAggregator;
import org.elasticsearch.compute.ann.RuntimeIntermediateState;

/**
 * Runtime aggregator for Sum2 function.
 * <p>
 * This class is the runtime equivalent of {@code SumIntAggregator}, using
 * {@link RuntimeAggregator} annotation instead of compile-time {@code @Aggregator}.
 * </p>
 * <p>
 * The aggregator computes the sum of integer values, storing the result as a long
 * to avoid overflow for reasonable input sizes.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "sum", type = "LONG"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN")
    }
)
public class Sum2Aggregator {

    /**
     * Initialize the aggregation state to zero.
     */
    public static long init() {
        return 0;
    }

    /**
     * Combine the current state with a new integer value.
     * Uses Math.addExact to detect overflow.
     */
    public static long combine(long current, int v) {
        return Math.addExact(current, v);
    }

    /**
     * Combine two intermediate states (for combining partial aggregations).
     */
    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
