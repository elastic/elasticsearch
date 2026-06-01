/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "buckets", type = "BYTES_REF_BLOCK") })
@GroupingAggregator(processNulls = true)
class ClassicHistogramQuantileAggregator {
    public static String describe() {
        return "classic_histogram_quantile";
    }

    public static ClassicHistogramQuantileStates.SingleState initSingle(DriverContext driverContext, double quantile) {
        return new ClassicHistogramQuantileStates.SingleState(driverContext.breaker(), quantile);
    }

    public static void combine(ClassicHistogramQuantileStates.SingleState state, double count, double upperBound) {
        state.add(upperBound, count);
    }

    public static void combineIntermediate(ClassicHistogramQuantileStates.SingleState state, BytesRefBlock buckets) {
        if (buckets.isNull(0)) {
            return;
        }
        BytesRef scratch = new BytesRef();
        state.add(buckets.getBytesRef(0, scratch));
    }

    public static Block evaluateFinal(ClassicHistogramQuantileStates.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static ClassicHistogramQuantileStates.GroupingState initGrouping(DriverContext driverContext, double quantile) {
        return new ClassicHistogramQuantileStates.GroupingState(driverContext.breaker(), driverContext.bigArrays(), quantile);
    }

    public static void combine(ClassicHistogramQuantileStates.GroupingState state, int groupId, double count, double upperBound) {
        state.add(groupId, upperBound, count);
    }

    public static void combineIntermediate(
        ClassicHistogramQuantileStates.GroupingState state,
        int groupId,
        BytesRefBlock buckets,
        int valuesPosition
    ) {
        if (buckets.isNull(valuesPosition)) {
            return;
        }
        BytesRef scratch = new BytesRef();
        state.add(groupId, buckets.getBytesRef(valuesPosition, scratch));
    }

    public static void combineIntermediate(
        ClassicHistogramQuantileStates.GroupingState state,
        int positionOffset,
        IntVector groups,
        BytesRefBlock buckets
    ) {
        BytesRef scratch = new BytesRef();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuesPosition = groupPosition + positionOffset;
            if (buckets.isNull(valuesPosition)) {
                continue;
            }
            state.add(groups.getInt(groupPosition), buckets.getBytesRef(valuesPosition, scratch));
        }
    }

    public static void combineIntermediate(
        ClassicHistogramQuantileStates.GroupingState state,
        int positionOffset,
        IntBlock groups,
        BytesRefBlock buckets
    ) {
        BytesRef scratch = new BytesRef();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int valuesPosition = groupPosition + positionOffset;
            if (buckets.isNull(valuesPosition)) {
                continue;
            }
            BytesRef serialized = buckets.getBytesRef(valuesPosition, scratch);
            int groupStart = groups.getFirstValueIndex(groupPosition);
            int groupEnd = groupStart + groups.getValueCount(groupPosition);
            for (int g = groupStart; g < groupEnd; g++) {
                state.add(groups.getInt(g), serialized);
            }
        }
    }

    public static Block evaluateFinal(
        ClassicHistogramQuantileStates.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext context
    ) {
        return state.evaluateFinal(selected, context.driverContext());
    }
}
