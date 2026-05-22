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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * Returns the first non-null exponential histogram value encountered. Used by FUSE
 * and by {@code FIRST(field, NULL)} when the sort field is null or constant.
 */
@Aggregator({ @IntermediateState(name = "values", type = "EXPONENTIAL_HISTOGRAM"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
public class AnyExponentialHistogramAggregator {
    public static String describe() {
        return "any_ExponentialHistogram";
    }

    public static ExponentialHistogramStates.SeenSingleState initSingle(DriverContext driverContext) {
        return new ExponentialHistogramStates.SeenSingleState(driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.SeenSingleState current, ExponentialHistogram value) {
        if (current.isSeen() == false) {
            current.set(value);
        }
    }

    public static void combineIntermediate(ExponentialHistogramStates.SeenSingleState current, ExponentialHistogram values, boolean seen) {
        if (seen && current.isSeen() == false) {
            current.set(values);
        }
    }

    public static Block evaluateFinal(ExponentialHistogramStates.SeenSingleState current, DriverContext ctx) {
        return current.evaluateFinalHistogram(ctx);
    }

    public static ExponentialHistogramStates.SeenGroupingState initGrouping(DriverContext driverContext) {
        return new ExponentialHistogramStates.SeenGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.SeenGroupingState current, int groupId, ExponentialHistogram value) {
        if (current.seen(groupId) == false) {
            current.set(groupId, value);
        }
    }

    public static void combineIntermediate(
        ExponentialHistogramStates.SeenGroupingState current,
        int groupId,
        ExponentialHistogram values,
        boolean seen
    ) {
        if (seen) {
            combine(current, groupId, values);
        }
    }

    public static Block evaluateFinal(
        ExponentialHistogramStates.SeenGroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluateFinalHistograms(selected, ctx.driverContext());
    }
}
