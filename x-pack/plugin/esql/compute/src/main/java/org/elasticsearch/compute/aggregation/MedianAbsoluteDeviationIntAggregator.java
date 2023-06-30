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

@Aggregator({ @IntermediateState(name = "aggstate", type = "UNKNOWN") })
@GroupingAggregator
class MedianAbsoluteDeviationIntAggregator {

    public static QuantileStates.SingleState initSingle() {
        return new QuantileStates.SingleState(QuantileStates.MEDIAN);
    }

    public static void combine(QuantileStates.SingleState current, int v) {
        current.add(v);
    }

    public static void combineStates(QuantileStates.SingleState current, QuantileStates.SingleState state) {
        current.add(state);
    }

    public static Block evaluateFinal(QuantileStates.SingleState state) {
        return state.evaluateMedianAbsoluteDeviation();
    }

    public static QuantileStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new QuantileStates.GroupingState(bigArrays, QuantileStates.MEDIAN);
    }

    public static void combine(QuantileStates.GroupingState state, int groupId, int v) {
        state.add(groupId, v);
    }

    public static void combineStates(
        QuantileStates.GroupingState current,
        int currentGroupId,
        QuantileStates.GroupingState state,
        int statePosition
    ) {
        current.add(currentGroupId, state.get(statePosition));
    }

    public static Block evaluateFinal(QuantileStates.GroupingState state, IntVector selected) {
        return state.evaluateMedianAbsoluteDeviation(selected);
    }
}
