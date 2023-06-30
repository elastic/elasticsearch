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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.ConstantLongVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;

@Aggregator({ @IntermediateState(name = "min", type = "LONG"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MinLongAggregator {

    public static long init() {
        return Long.MAX_VALUE;
    }

    public static long combine(long current, long v) {
        return Math.min(current, v);
    }

    public static void combineIntermediate(LongState state, LongVector values, BooleanVector seen) {
        if (seen.getBoolean(0)) {
            state.longValue(combine(state.longValue(), values.getLong(0)));
            state.seen(true);
        }
    }

    public static void evaluateIntermediate(LongState state, Block[] blocks, int offset) {
        assert blocks.length >= offset + 2;
        blocks[offset + 0] = new ConstantLongVector(state.longValue(), 1).asBlock();
        blocks[offset + 1] = new ConstantBooleanVector(state.seen(), 1).asBlock();
    }

    public static void combineIntermediate(LongVector groupIdVector, LongArrayState state, LongVector values, BooleanVector seen) {
        for (int position = 0; position < groupIdVector.getPositionCount(); position++) {
            int groupId = Math.toIntExact(groupIdVector.getLong(position));
            if (seen.getBoolean(position)) {
                state.set(MinLongAggregator.combine(state.getOrDefault(groupId), values.getLong(position)), groupId);
            } else {
                state.putNull(groupId);
            }
        }
    }

    public static void evaluateIntermediate(LongArrayState state, Block[] blocks, int offset, IntVector selected) {
        assert blocks.length >= offset + 2;
        var valuesBuilder = LongBlock.newBlockBuilder(selected.getPositionCount());
        var nullsBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            valuesBuilder.appendLong(state.get(group));
            nullsBuilder.appendBoolean(state.hasValue(group));
        }
        blocks[offset + 0] = valuesBuilder.build();
        blocks[offset + 1] = nullsBuilder.build();
    }
}
