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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * The intermediate state ships the pre-aggregated buckets as a single multi-value {@code DOUBLE_BLOCK} position holding
 * {@code (upperBound, count)} pairs, so no per-bucket encoding/decoding or {@code BytesRef} reuse is needed and only the
 * distinct upper bounds (not the raw input values) travel to the coordinating node.
 */
@Aggregator({ @IntermediateState(name = "buckets", type = "DOUBLE_BLOCK") })
@GroupingAggregator(processNulls = true)
class PromqlHistogramQuantileAggregator {
    public static String describe() {
        return "promql_histogram_quantile";
    }

    public static PromqlHistogramStates.Quantile.SingleState initSingle(DriverContext driverContext, double quantile, Warnings warnings) {
        return new PromqlHistogramStates.Quantile.SingleState(driverContext.breaker(), quantile, warnings);
    }

    public static void combine(PromqlHistogramStates.Quantile.SingleState state, double count, BytesRef upperBound) {
        // The state parses the `le` keyword bound, skipping (and warning about) buckets whose label is not a number.
        state.add(upperBound, count);
    }

    public static void combineIntermediate(PromqlHistogramStates.Quantile.SingleState state, DoubleBlock buckets) {
        state.combineIntermediate(buckets);
    }

    public static Block evaluateFinal(PromqlHistogramStates.Quantile.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static PromqlHistogramStates.Quantile.GroupingState initGrouping(
        DriverContext driverContext,
        double quantile,
        Warnings warnings
    ) {
        return new PromqlHistogramStates.Quantile.GroupingState(driverContext.breaker(), driverContext.bigArrays(), quantile, warnings);
    }

    public static void combine(PromqlHistogramStates.Quantile.GroupingState state, int groupId, double count, BytesRef upperBound) {
        // The state parses the `le` keyword bound, skipping (and warning about) buckets whose label is not a number.
        state.add(groupId, upperBound, count);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Quantile.GroupingState state,
        int groupId,
        DoubleBlock buckets,
        int valuesPosition
    ) {
        state.combineIntermediate(groupId, buckets, valuesPosition);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Quantile.GroupingState state,
        int positionOffset,
        IntVector groups,
        DoubleBlock buckets
    ) {
        state.combineIntermediate(positionOffset, groups, buckets);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Quantile.GroupingState state,
        int positionOffset,
        IntBlock groups,
        DoubleBlock buckets
    ) {
        state.combineIntermediate(positionOffset, groups, buckets);
    }

    public static Block evaluateFinal(
        PromqlHistogramStates.Quantile.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext context
    ) {
        return state.evaluateFinal(selected, context.driverContext());
    }

}
