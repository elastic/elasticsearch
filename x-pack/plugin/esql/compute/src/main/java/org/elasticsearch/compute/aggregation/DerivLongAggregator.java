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

@Aggregator(
    {
        @IntermediateState(name = "count", type = "LONG"),
        @IntermediateState(name = "sumVal", type = "DOUBLE"),
        @IntermediateState(name = "sumTs", type = "LONG"),
        @IntermediateState(name = "sumTsVal", type = "DOUBLE"),
        @IntermediateState(name = "sumTsSq", type = "LONG") }
)
@GroupingAggregator
class DerivLongAggregator {

    public static SimpleLinearRegressionWithTimeseries initSingle(DriverContext driverContext) {
        return new SimpleLinearRegressionWithTimeseries();
    }

    public static void combine(SimpleLinearRegressionWithTimeseries current, long value, long timestamp) {
        DerivDoubleAggregator.combine(current, (double) value, timestamp);
    }

    public static void combineIntermediate(
        SimpleLinearRegressionWithTimeseries state,
        long count,
        double sumVal,
        long sumTs,
        double sumTsVal,
        long sumTsSq
    ) {
        DerivDoubleAggregator.combineIntermediate(state, count, sumVal, sumTs, sumTsVal, sumTsSq);
    }

    public static Block evaluateFinal(SimpleLinearRegressionWithTimeseries state, DriverContext driverContext) {
        return DerivDoubleAggregator.evaluateFinal(state, driverContext);
    }

    public static DerivDoubleAggregator.GroupingState initGrouping(DriverContext driverContext) {
        return new DerivDoubleAggregator.GroupingState(driverContext.bigArrays());
    }

    public static void combine(DerivDoubleAggregator.GroupingState state, int groupId, long value, long timestamp) {
        DerivDoubleAggregator.combine(state.getAndGrow(groupId), (double) value, timestamp);
    }

    public static void combineIntermediate(
        DerivDoubleAggregator.GroupingState state,
        int groupId,
        long count,
        double sumVal,
        long sumTs,
        double sumTsVal,
        long sumTsSq
    ) {
        combineIntermediate(state.getAndGrow(groupId), count, sumVal, sumTs, sumTsVal, sumTsSq);
    }

    public static Block evaluateFinal(
        DerivDoubleAggregator.GroupingState state,
        IntVector selectedGroups,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return DerivDoubleAggregator.evaluateFinal(state, selectedGroups, ctx);
    }
}
