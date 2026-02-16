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
 * Runtime aggregator for SafeSum2 function with overflow protection.
 * <p>
 * This class demonstrates the use of {@code warnExceptions} in the
 * {@link RuntimeAggregator} annotation. When an {@link ArithmeticException}
 * occurs during the combine operation (e.g., due to overflow), the exception
 * is caught, a warning is registered, and the aggregation is marked as failed.
 * </p>
 * <p>
 * The aggregator computes the sum of integer values using {@code Math.addExact},
 * which throws {@link ArithmeticException} on overflow. With {@code warnExceptions},
 * the overflow is handled gracefully instead of failing the entire query.
 * </p>
 * <p>
 * When warnExceptions is non-empty, the intermediate state includes a "failed" flag
 * in addition to the value and "seen" flag.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "sum", type = "LONG"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN"),
        @RuntimeIntermediateState(name = "failed", type = "BOOLEAN")
    },
    warnExceptions = { ArithmeticException.class }
)
public class SafeSum2Aggregator {

    /**
     * Initialize the aggregation state to zero.
     */
    public static long init() {
        return 0;
    }

    /**
     * Combine the current state with a new integer value.
     * Uses Math.addExact to detect overflow, which throws ArithmeticException.
     * When warnExceptions includes ArithmeticException, the exception is caught
     * and the state is marked as failed instead of propagating the error.
     */
    public static long combine(long current, int v) {
        return Math.addExact(current, v);
    }

    /**
     * Combine two intermediate states (for combining partial aggregations).
     * Also uses Math.addExact for overflow detection.
     */
    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
