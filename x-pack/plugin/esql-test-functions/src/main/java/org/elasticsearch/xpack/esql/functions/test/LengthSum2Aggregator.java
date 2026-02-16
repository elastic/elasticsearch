/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.RuntimeAggregator;
import org.elasticsearch.compute.ann.RuntimeIntermediateState;

/**
 * Runtime aggregator that sums the lengths of BytesRef (string) values.
 * This tests BytesRef input handling with primitive (long) state.
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "lengthSum", type = "LONG"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN")
    }
)
public class LengthSum2Aggregator {

    /**
     * Initialize the aggregation state to zero.
     */
    public static long init() {
        return 0;
    }

    /**
     * Combine the current state with a new BytesRef value by adding its length.
     */
    public static long combine(long current, BytesRef v) {
        return Math.addExact(current, v.length);
    }

    /**
     * Combine two intermediate states (for combining partial aggregations).
     */
    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
