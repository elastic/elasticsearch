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

/**
 * Sum long aggregator for compatibility with old versions.
 * <p>
 *     Replaced by {@link org.elasticsearch.compute.aggregation.SumLongAggregator} since {@code EsqlFeatures#FN_SUM_OVERFLOW_HANDLING}.
 * </p>
 * <p>
 *     Should be kept for as long as we need compatibility with the version this was added on, as the new aggregator's layout is different.
 * </p>
 */
@Aggregator(value = { @IntermediateState(name = "sum", type = "LONG"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class OverflowingSumLongAggregator {

    public static long init() {
        return 0;
    }

    public static long combine(long current, long v) {
        return Math.addExact(current, v);
    }
}
