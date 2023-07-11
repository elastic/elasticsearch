/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;

@Aggregator({ @IntermediateState(name = "fbit", type = "BOOLEAN"), @IntermediateState(name = "tbit", type = "BOOLEAN") })
@GroupingAggregator
public class CountDistinctBooleanAggregator {
    private static final byte BIT_FALSE = 0b01;
    private static final byte BIT_TRUE = 0b10;

    public static SingleState initSingle() {
        return new SingleState();
    }

    public static void combine(SingleState current, boolean v) {
        current.bits |= v ? BIT_TRUE : BIT_FALSE;
    }

    public static void combineStates(SingleState current, SingleState state) {
        current.bits |= state.bits;
    }

    public static void combineIntermediate(SingleState current, boolean fbit, boolean tbit) {
        if (fbit) current.bits |= BIT_FALSE;
        if (tbit) current.bits |= BIT_TRUE;
    }

    public static Block evaluateFinal(SingleState state) {
        long result = ((state.bits & BIT_TRUE) >> 1) + (state.bits & BIT_FALSE);
        return LongBlock.newConstantBlockWith(result, 1);
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState current, int groupId, boolean v) {
        current.collect(groupId, v);
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        current.combineStates(currentGroupId, state);
    }

    public static void combineIntermediate(GroupingState current, int groupId, boolean fbit, boolean tbit) {
        if (fbit) current.bits.set(groupId * 2);
        if (tbit) current.bits.set(groupId * 2 + 1);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected) {
        LongBlock.Builder builder = LongBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            long count = (state.bits.get(2 * group) ? 1 : 0) + (state.bits.get(2 * group + 1) ? 1 : 0);
            builder.appendLong(count);
        }
        return builder.build();
    }

    /**
     * State contains a byte variable where we set two bits. Bit 0 is set when a boolean false
     * value is collected. Bit 1 is set when a boolean true value is collected.
     */
    static class SingleState implements AggregatorState {

        byte bits;

        SingleState() {}

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset) {
            assert blocks.length >= offset + 2;
            blocks[offset + 0] = BooleanBlock.newConstantBlockWith((bits & BIT_FALSE) != 0, 1);
            blocks[offset + 1] = BooleanBlock.newConstantBlockWith((bits & BIT_TRUE) != 0, 1);
        }

        @Override
        public void close() {}
    }

    /**
     * Grouping state uses as a {@link BitArray} and stores two bits for each groupId.
     * First bit is set if boolean false value is collected and second bit is set
     * if boolean true value is collected.
     * This means that false values for a groupId are stored at bits[2*groupId] and
     * true values for a groupId are stored at bits[2*groupId + 1]
     */
    static class GroupingState implements GroupingAggregatorState {

        final BitArray bits;
        int largestGroupId; // total number of groups; <= bytes.length

        GroupingState(BigArrays bigArrays) {
            boolean success = false;
            try {
                this.bits = new BitArray(2, bigArrays); // Start with two bits for a single groupId
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        void collect(int groupId, boolean v) {
            ensureCapacity(groupId);
            bits.set(groupId * 2 + (v ? 1 : 0));
        }

        void combineStates(int currentGroupId, GroupingState state) {
            ensureCapacity(currentGroupId);
            bits.or(state.bits);
        }

        void putNull(int groupId) {
            ensureCapacity(groupId);
        }

        void ensureCapacity(int groupId) {
            if (groupId > largestGroupId) {
                largestGroupId = groupId;
            }
        }

        /** Extracts an intermediate view of the contents of this state.  */
        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected) {
            assert blocks.length >= offset + 2;
            var fbitBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
            var tbitBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                fbitBuilder.appendBoolean(bits.get(2 * group + 0));
                tbitBuilder.appendBoolean(bits.get(2 * group + 1));
            }
            blocks[offset + 0] = fbitBuilder.build();
            blocks[offset + 1] = tbitBuilder.build();
        }

        @Override
        public void close() {
            Releasables.close(bits);
        }
    }
}
