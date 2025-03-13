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

/**
 * A standard deviation aggregation definition for long.
 * This class is generated. Edit `X-StdDevAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "mean", type = "DOUBLE"),
        @IntermediateState(name = "m2", type = "DOUBLE"),
        @IntermediateState(name = "count", type = "LONG") }
)
@GroupingAggregator
public class StdDevLongAggregator {

    public static StdDevStates.SingleState initSingle() {
        return new StdDevStates.SingleState();
    }

    public static void combine(StdDevStates.SingleState state, long value) {
        state.add(value);
    }

    public static void combineIntermediate(StdDevStates.SingleState state, double mean, double m2, long count) {
        state.combine(mean, m2, count);
    }

    public static Block evaluateFinal(StdDevStates.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static StdDevStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new StdDevStates.GroupingState(bigArrays);
    }

    public static void combine(StdDevStates.GroupingState current, int groupId, long value) {
        current.add(groupId, value);
    }

    public static void combineStates(StdDevStates.GroupingState current, int groupId, StdDevStates.GroupingState state, int statePosition) {
        current.combine(groupId, state.getOrNull(statePosition));
    }

    public static void combineIntermediate(StdDevStates.GroupingState state, int groupId, double mean, double m2, long count) {
        state.combine(groupId, mean, m2, count);
    }

    public static Block evaluateFinal(StdDevStates.GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluateFinal(selected, driverContext);
    }
}
