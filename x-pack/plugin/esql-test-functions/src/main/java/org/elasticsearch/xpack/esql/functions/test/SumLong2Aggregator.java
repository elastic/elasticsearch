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
 * Runtime aggregator for summing long values.
 * <p>
 * This is the runtime equivalent of {@code SumLongAggregator}, used for
 * benchmarking runtime-generated aggregators against compile-time ones.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "sum", type = "LONG"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN")
    }
)
public class SumLong2Aggregator {

    public static long init() {
        return 0;
    }

    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
