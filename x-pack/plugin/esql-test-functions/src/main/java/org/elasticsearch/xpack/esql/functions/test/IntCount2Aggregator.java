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
 * Runtime aggregator for IntSum2 function.
 * <p>
 * This aggregator sums integer values, storing the result as an int.
 * It tests the runtime aggregator generation with INT state type.
 * </p>
 * <p>
 * Note: For int state with int input, we can only have one combine method
 * since both raw input and intermediate state combination have the same
 * signature (int, int). This is fine for sum since adding values works
 * the same way for both cases.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "sum", type = "INT"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
)
public class IntCount2Aggregator {

    /**
     * Initialize the aggregation state to zero.
     */
    public static int init() {
        return 0;
    }

    /**
     * Combine the current state with a new integer value by adding it.
     */
    public static int combine(int current, int v) {
        return current + v;
    }
}
