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

/** Aggregates classic cumulative histogram buckets and evaluates the fraction between two bounds. */
@Aggregator({ @IntermediateState(name = "buckets", type = "DOUBLE_BLOCK") })
@GroupingAggregator(processNulls = true)
class PromqlHistogramFractionAggregator {
    public static String describe() {
        return "promql_histogram_fraction";
    }

    public static PromqlHistogramStates.Fraction.SingleState initSingle(
        DriverContext driverContext,
        double lower,
        double upper,
        Warnings warnings
    ) {
        return new PromqlHistogramStates.Fraction.SingleState(driverContext.breaker(), lower, upper, warnings);
    }

    public static void combine(PromqlHistogramStates.Fraction.SingleState state, double count, BytesRef upperBound) {
        state.add(upperBound, count);
    }

    public static void combineIntermediate(PromqlHistogramStates.Fraction.SingleState state, DoubleBlock buckets) {
        state.combineIntermediate(buckets);
    }

    public static Block evaluateFinal(PromqlHistogramStates.Fraction.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static PromqlHistogramStates.Fraction.GroupingState initGrouping(
        DriverContext driverContext,
        double lower,
        double upper,
        Warnings warnings
    ) {
        return new PromqlHistogramStates.Fraction.GroupingState(driverContext.breaker(), driverContext.bigArrays(), lower, upper, warnings);
    }

    public static void combine(PromqlHistogramStates.Fraction.GroupingState state, int groupId, double count, BytesRef upperBound) {
        state.add(groupId, upperBound, count);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Fraction.GroupingState state,
        int groupId,
        DoubleBlock buckets,
        int valuesPosition
    ) {
        state.combineIntermediate(groupId, buckets, valuesPosition);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Fraction.GroupingState state,
        int positionOffset,
        IntVector groups,
        DoubleBlock buckets
    ) {
        state.combineIntermediate(positionOffset, groups, buckets);
    }

    public static void combineIntermediate(
        PromqlHistogramStates.Fraction.GroupingState state,
        int positionOffset,
        IntBlock groups,
        DoubleBlock buckets
    ) {
        state.combineIntermediate(positionOffset, groups, buckets);
    }

    public static Block evaluateFinal(
        PromqlHistogramStates.Fraction.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext context
    ) {
        return state.evaluateFinal(selected, context.driverContext());
    }
}
