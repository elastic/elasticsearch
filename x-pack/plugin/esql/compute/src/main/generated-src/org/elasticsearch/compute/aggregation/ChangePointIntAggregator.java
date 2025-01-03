/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.ChangePointStates.GroupingState;
import org.elasticsearch.compute.aggregation.ChangePointStates.SingleState;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Change point detection for series of int values.
 * This class is generated. Edit @{code X-ChangePointAggregator.java.st} instead
 * of this file.
 */
@Aggregator(
    includeTimestamps = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") }
)
@GroupingAggregator(
    includeTimestamps = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") }
)
class ChangePointIntAggregator {

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.bigArrays());
    }

    public static void combine(SingleState state, long timestamp, int value) {
        state.add(timestamp, value);
    }

    public static void combineIntermediate(SingleState state, LongBlock timestamps, DoubleBlock values) {
        state.add(timestamps, values);
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int groupId, long timestamp, int value) {
        current.add(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps,
        DoubleBlock values,
        int otherPosition
    ) {
        current.combine(groupId, timestamps, values, otherPosition);
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState otherState, int otherGroupId) {
        current.combineState(currentGroupId, otherState, otherGroupId);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluateFinal(selected, driverContext.blockFactory());
    }
}
