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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Aggregator
@GroupingAggregator
public class CountDistinctBooleanAggregator {
    public static AggregatorFunctionSupplier supplier(BigArrays bigArrays, int channel) {
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator() {
                return CountDistinctBooleanAggregatorFunction.create(bigArrays, channel, new Object[] {});
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator() {
                return CountDistinctBooleanGroupingAggregatorFunction.create(bigArrays, channel, new Object[] {});
            }

            @Override
            public String describe() {
                return "count_distinct of booleans";
            }
        };
    }

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
    static class SingleState implements AggregatorState<SingleState> {

        private final SingleStateSerializer serializer;
        byte bits;

        SingleState() {
            this.serializer = new SingleStateSerializer();
        }

        @Override
        public long getEstimatedSize() {
            return Byte.BYTES; // Serialize the two boolean values as two bits in a single byte
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<SingleState> serializer() {
            return serializer;
        }
    }

    static class SingleStateSerializer implements AggregatorStateSerializer<SingleState> {
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int serialize(SingleState state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            ba[offset] = state.bits;

            return Byte.BYTES;
        }

        @Override
        public void deserialize(SingleState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.bits = ba[offset];
        }
    }

    /**
     * Grouping state uses as a {@link BitArray} and stores two bits for each groupId.
     * First bit is set if boolean false value is collected and second bit is set
     * if boolean true value is collected.
     * This means that false values for a groupId are stored at bits[2*groupId] and
     * true values for a groupId are stored at bits[2*groupId + 1]
     */
    static class GroupingState implements AggregatorState<GroupingState> {

        private final GroupingStateSerializer serializer;
        final BitArray bits;
        int largestGroupId; // total number of groups; <= bytes.length

        GroupingState(BigArrays bigArrays) {
            this.serializer = new GroupingStateSerializer();
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

        @Override
        public long getEstimatedSize() {
            return Integer.BYTES + (largestGroupId + 1) * Byte.BYTES;
        }

        @Override
        public AggregatorStateSerializer<GroupingState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(bits);
        }
    }

    static class GroupingStateSerializer implements AggregatorStateSerializer<GroupingState> {

        private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        /**
         * The bit array is serialized using a whole byte for each group and the bits for each group are encoded
         * similar to {@link SingleState}.
         */
        @Override
        public int serialize(GroupingState state, byte[] ba, int offset, IntVector selected) {
            int origOffset = offset;
            intHandle.set(ba, offset, selected.getPositionCount());
            offset += Integer.BYTES;
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                ba[offset] |= state.bits.get(2 * groupId) ? BIT_FALSE : 0;
                ba[offset] |= state.bits.get(2 * groupId + 1) ? BIT_TRUE : 0;
                offset += Byte.BYTES;
            }
            return offset - origOffset;
        }

        @Override
        public void deserialize(GroupingState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) intHandle.get(ba, offset);
            offset += Integer.BYTES;
            state.ensureCapacity(positions - 1);
            for (int i = 0; i < positions; i++) {
                if ((ba[offset] & BIT_FALSE) > 0) {
                    state.bits.set(2 * i);
                }
                if ((ba[offset] & BIT_TRUE) > 0) {
                    state.bits.set(2 * i + 1);
                }
                offset += Byte.BYTES;
            }
        }
    }
}
