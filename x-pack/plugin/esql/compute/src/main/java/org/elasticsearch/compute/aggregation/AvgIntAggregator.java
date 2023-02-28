/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AvgLongAggregator.AvgState;
import org.elasticsearch.compute.aggregation.AvgLongAggregator.GroupingAvgState;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;

@Aggregator
@GroupingAggregator
class AvgIntAggregator {
    public static AvgState initSingle() {
        return new AvgState();
    }

    public static void combine(AvgState current, int v) {
        current.value = Math.addExact(current.value, v);
    }

    public static void combineValueCount(AvgState current, int positions) {
        current.count += positions;
    }

    public static void combineStates(AvgState current, AvgState state) {
        current.value = Math.addExact(current.value, state.value);
        current.count += state.count;
    }

    public static Block evaluateFinal(AvgState state) {
        double result = ((double) state.value) / state.count;
        return DoubleBlock.newConstantBlockWith(result, 1);
    }

    public static GroupingAvgState initGrouping(BigArrays bigArrays) {
        return new GroupingAvgState(bigArrays);
    }

    public static void combine(GroupingAvgState current, int groupId, int v) {
        current.add(v, groupId, 1);
    }

    public static void combineStates(GroupingAvgState current, int currentGroupId, GroupingAvgState state, int statePosition) {
        current.add(state.values.get(statePosition), currentGroupId, state.counts.get(statePosition));
    }

    public static Block evaluateFinal(GroupingAvgState state, IntVector selected) {
        DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            final long count = state.counts.get(group);
            if (count > 0) {
                builder.appendDouble((double) state.values.get(group) / count);
            } else {
                assert state.values.get(group) == 0;
                builder.appendNull();
            }
        }
        return builder.build();
    }
}
