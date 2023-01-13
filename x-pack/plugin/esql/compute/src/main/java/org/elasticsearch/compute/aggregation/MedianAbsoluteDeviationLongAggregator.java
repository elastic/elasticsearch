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

@Aggregator
@GroupingAggregator
class MedianAbsoluteDeviationLongAggregator {
    public static MedianAbsoluteDeviationStates.UngroupedState initSingle() {
        return new MedianAbsoluteDeviationStates.UngroupedState();
    }

    public static void combine(MedianAbsoluteDeviationStates.UngroupedState current, long v) {
        current.add(v);
    }

    public static void combineStates(
        MedianAbsoluteDeviationStates.UngroupedState current,
        MedianAbsoluteDeviationStates.UngroupedState state
    ) {
        current.add(state);
    }

    public static Block evaluateFinal(MedianAbsoluteDeviationStates.UngroupedState state) {
        return state.evaluateFinal();
    }

    public static MedianAbsoluteDeviationStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new MedianAbsoluteDeviationStates.GroupingState(bigArrays);
    }

    public static void combine(MedianAbsoluteDeviationStates.GroupingState state, int groupId, long v) {
        state.add(groupId, v);
    }

    public static void combineStates(
        MedianAbsoluteDeviationStates.GroupingState current,
        int currentGroupId,
        MedianAbsoluteDeviationStates.GroupingState state,
        int statePosition
    ) {
        current.add(currentGroupId, state.get(statePosition));
    }

    public static Block evaluateFinal(MedianAbsoluteDeviationStates.GroupingState state) {
        return state.evaluateFinal();
    }
}
