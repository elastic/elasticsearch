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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;

@Aggregator
@GroupingAggregator
class MedianAbsoluteDeviationDoubleAggregator {

    public static QuantileStates.SingleState initSingle() {
        return new QuantileStates.SingleState(QuantileStates.MEDIAN_PARAMS);
    }

    public static void combine(QuantileStates.SingleState current, double v) {
        current.add(v);
    }

    public static void combineStates(QuantileStates.SingleState current, QuantileStates.SingleState state) {
        current.add(state);
    }

    public static Block evaluateFinal(QuantileStates.SingleState state) {
        return state.evaluateMedianAbsoluteDeviation();
    }

    public static QuantileStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new QuantileStates.GroupingState(bigArrays, QuantileStates.MEDIAN_PARAMS);
    }

    public static void combine(QuantileStates.GroupingState state, int groupId, double v) {
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
