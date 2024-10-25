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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "quart", type = "BYTES_REF") })
@GroupingAggregator
class MedianAbsoluteDeviationDoubleAggregator {

    public static QuantileStates.SingleState initSingle(DriverContext driverContext) {
        return new QuantileStates.SingleState(driverContext.breaker(), QuantileStates.MEDIAN);
    }

    public static void combine(QuantileStates.SingleState current, double v) {
        current.add(v);
    }

    public static void combineIntermediate(QuantileStates.SingleState state, BytesRef inValue) {
        state.add(inValue);
    }

    public static Block evaluateFinal(QuantileStates.SingleState state, DriverContext driverContext) {
        return state.evaluateMedianAbsoluteDeviation(driverContext);
    }

    public static QuantileStates.GroupingState initGrouping(DriverContext driverContext) {
        return new QuantileStates.GroupingState(driverContext.breaker(), driverContext.bigArrays(), QuantileStates.MEDIAN);
    }

    public static void combine(QuantileStates.GroupingState state, int groupId, double v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(QuantileStates.GroupingState state, int groupId, BytesRef inValue) {
        state.add(groupId, inValue);
    }

    public static void combineStates(
        QuantileStates.GroupingState current,
        int currentGroupId,
        QuantileStates.GroupingState state,
        int statePosition
    ) {
        current.add(currentGroupId, state.getOrNull(statePosition));
    }

    public static Block evaluateFinal(QuantileStates.GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluateMedianAbsoluteDeviation(selected, driverContext);
    }
}
