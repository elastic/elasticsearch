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
 * A time-series aggregation function that collects the Last occurrence exponential histogram of a time series in a specified interval.
 *
 */
@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "EXPONENTIAL_HISTOGRAM"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator()
public class LastExponentialHistogramByTimestampAggregator {
    public static String describe() {
        return "last_ExponentialHistogram_by_timestamp";
    }

    public static ExponentialHistogramStates.WithLongSingleState initSingle(DriverContext driverContext) {
        return new ExponentialHistogramStates.WithLongSingleState(driverContext.breaker());
    }

    public static void combine(ExponentialHistogramStates.WithLongSingleState current, ExponentialHistogram value, long timestamp) {
        if (timestamp > current.longValue()) {
            current.set(timestamp, value);
        }
    }

    public static void combineIntermediate(
        ExponentialHistogramStates.WithLongSingleState current,
        long timestamp,
        ExponentialHistogram value,
        boolean seen
    ) {
        if (seen) {
            if (current.isSeen()) {
                combine(current, value, timestamp);
            } else {
                current.set(timestamp, value);
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
        long timestamp
    ) {
        if (current.seen(groupId) == false || timestamp > current.longValue(groupId)) {
            current.set(groupId, timestamp, value);
        }
    }

    public static void combineIntermediate(
        ExponentialHistogramStates.WithLongGroupingState current,
        int groupId,
        long timestamp,
        ExponentialHistogram value,
        boolean seen
    ) {
        if (seen) {
            combine(current, groupId, value, timestamp);
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
