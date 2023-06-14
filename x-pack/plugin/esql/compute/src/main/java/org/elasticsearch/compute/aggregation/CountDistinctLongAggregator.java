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
import org.elasticsearch.compute.data.LongBlock;

@Aggregator
@GroupingAggregator
public class CountDistinctLongAggregator {
    public static HllStates.SingleState initSingle(BigArrays bigArrays, int precision) {
        return new HllStates.SingleState(bigArrays, precision);
    }

    public static void combine(HllStates.SingleState current, long v) {
        current.collect(v);
    }

    public static void combineStates(HllStates.SingleState current, HllStates.SingleState state) {
        current.merge(0, state.hll, 0);
    }

    public static Block evaluateFinal(HllStates.SingleState state) {
        long result = state.cardinality();
        return LongBlock.newConstantBlockWith(result, 1);
    }

    public static HllStates.GroupingState initGrouping(BigArrays bigArrays, int precision) {
        return new HllStates.GroupingState(bigArrays, precision);
    }

    public static void combine(HllStates.GroupingState current, int groupId, long v) {
        current.collect(groupId, v);
    }

    public static void combineStates(
        HllStates.GroupingState current,
        int currentGroupId,
        HllStates.GroupingState state,
        int statePosition
    ) {
        current.merge(currentGroupId, state.hll, currentGroupId);
    }

    public static Block evaluateFinal(HllStates.GroupingState state, IntVector selected) {
        LongBlock.Builder builder = LongBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            long count = state.cardinality(group);
            builder.appendLong(count);
        }
        return builder.build();
    }
}
