/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "hll", type = "BYTES_REF") })
@GroupingAggregator
public class CountDistinctLongAggregator {

    public static HllStates.SingleState initSingle(BigArrays bigArrays, int precision) {
        return new HllStates.SingleState(bigArrays, precision);
    }

    public static void combine(HllStates.SingleState current, long v) {
        current.collect(v);
    }

    public static void combineIntermediate(HllStates.SingleState current, BytesRef inValue) {
        current.merge(0, inValue, 0);
    }

    public static Block evaluateFinal(HllStates.SingleState state, DriverContext driverContext) {
        long result = state.cardinality();
        return driverContext.blockFactory().newConstantLongBlockWith(result, 1);
    }

    public static HllStates.GroupingState initGrouping(BigArrays bigArrays, int precision) {
        return new HllStates.GroupingState(bigArrays, precision);
    }

    public static void combine(HllStates.GroupingState current, int groupId, long v) {
        current.collect(groupId, v);
    }

    public static void combineIntermediate(HllStates.GroupingState current, int groupId, BytesRef inValue) {
        current.merge(groupId, inValue, 0);
    }

    public static void combineStates(
        HllStates.GroupingState current,
        int currentGroupId,
        HllStates.GroupingState state,
        int statePosition
    ) {
        current.merge(currentGroupId, state.hll, statePosition);
    }

    public static Block evaluateFinal(HllStates.GroupingState state, IntVector selected, DriverContext driverContext) {
        try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                long count = state.cardinality(group);
                builder.appendLong(count);
            }
            return builder.build();
        }
    }
}
