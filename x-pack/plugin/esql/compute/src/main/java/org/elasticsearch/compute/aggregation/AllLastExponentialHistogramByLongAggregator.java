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
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * Picks the exponential histogram with the maximum long sort key.
 */
@Aggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "sortKeys", type = "LONG"),
        @IntermediateState(name = "values", type = "EXPONENTIAL_HISTOGRAM_BLOCK"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator()
public class AllLastExponentialHistogramByLongAggregator {
    public static String describe() {
        return "all_last_ExponentialHistogram_by_long";
    }

    public static ExponentialHistogramStates.WithLongSingleState initSingle(DriverContext driverContext) {
        return new ExponentialHistogramStates.WithLongSingleState(driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.WithLongSingleState current, ExponentialHistogram value, long sortKey) {
        if (current.isSeen() == false || sortKey > current.longValue()) {
            current.set(sortKey, value);
        }
    }

    public static void combineIntermediate(
        ExponentialHistogramStates.WithLongSingleState current,
        long sortKey,
        ExponentialHistogramBlock values,
        boolean seen
    ) {
        if (seen) {
            ExponentialHistogram value = values.getExponentialHistogram(values.getFirstValueIndex(0), new ExponentialHistogramScratch());
            if (current.isSeen()) {
                combine(current, value, sortKey);
            } else {
                current.set(sortKey, value);
            }
        }
    }

    public static Block evaluateFinal(ExponentialHistogramStates.WithLongSingleState current, DriverContext ctx) {
        return current.evaluateFinalHistogram(ctx);
    }

    public static ExponentialHistogramStates.WithLongGroupingState initGrouping(DriverContext driverContext) {
        return new ExponentialHistogramStates.WithLongGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(
        ExponentialHistogramStates.WithLongGroupingState current,
        int groupId,
        ExponentialHistogram value,
        long sortKey
    ) {
        if (current.seen(groupId) == false || sortKey > current.longValue(groupId)) {
            current.set(groupId, sortKey, value);
        }
    }

    public static void combineIntermediate(
        ExponentialHistogramStates.WithLongGroupingState current,
        int groupId,
        long sortKey,
        ExponentialHistogramBlock values,
        boolean seen,
        int otherPosition
    ) {
        if (seen) {
            ExponentialHistogram value = values.getExponentialHistogram(
                values.getFirstValueIndex(otherPosition),
                new ExponentialHistogramScratch()
            );
            combine(current, groupId, value, sortKey);
        }
    }

    public static Block evaluateFinal(
        ExponentialHistogramStates.WithLongGroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluateFinalHistograms(selected, ctx.driverContext());
    }
}
