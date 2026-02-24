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
 * Runtime aggregator for DoubleSum2 function.
 * <p>
 * This aggregator computes the sum of double values, storing the result as a double.
 * It tests the runtime aggregator generation with DOUBLE state type.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "sum", type = "DOUBLE"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") }
)
public class DoubleSum2Aggregator {

    /**
     * Initialize the aggregation state to zero.
     */
    public static double init() {
        return 0.0;
    }

    /**
     * Combine the current state with a new double value.
     */
    public static double combine(double current, double v) {
        return current + v;
    }
}
