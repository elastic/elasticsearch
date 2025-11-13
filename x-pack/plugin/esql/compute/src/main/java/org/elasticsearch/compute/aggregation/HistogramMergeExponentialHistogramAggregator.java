/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

@Aggregator({ @IntermediateState(name = "value", type = "EXPONENTIAL_HISTOGRAM"), })
@GroupingAggregator
public class HistogramMergeExponentialHistogramAggregator {

    public static ExponentialHistogramStates.SingleState initSingle(DriverContext driverContext) {
        return new ExponentialHistogramStates.SingleState(driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.SingleState state, ExponentialHistogram value) {
        state.add(value, true);
    }

    public static void combineIntermediate(ExponentialHistogramStates.SingleState state, ExponentialHistogram value) {
        state.add(value, false);
    }

    public static Block evaluateFinal(ExponentialHistogramStates.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static ExponentialHistogramStates.GroupingState initGrouping(BigArrays bigArrays, DriverContext driverContext) {
        return new ExponentialHistogramStates.GroupingState(bigArrays, driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.GroupingState current, int groupId, ExponentialHistogram value) {
        current.add(groupId, value, true);
    }

    public static void combineIntermediate(ExponentialHistogramStates.GroupingState state, int groupId, ExponentialHistogram value) {
        state.add(groupId, value, false);
    }

    public static Block evaluateFinal(
        ExponentialHistogramStates.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluateFinal(selected, ctx.driverContext());
    }
}
