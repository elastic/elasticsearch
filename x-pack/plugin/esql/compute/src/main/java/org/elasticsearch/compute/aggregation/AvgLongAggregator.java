/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Aggregator
@GroupingAggregator
class AvgLongAggregator {
    public static AvgState initSingle() {
        return new AvgState();
    }

    public static void combine(AvgState current, long v) {
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

    public static void combine(GroupingAvgState current, int groupId, long v) {
        current.add(v, groupId, 1);
    }

    public static void combineStates(GroupingAvgState current, int currentGroupId, GroupingAvgState state, int statePosition) {
        current.add(state.values.get(statePosition), currentGroupId, state.counts.get(statePosition));
    }

    public static Block evaluateFinal(GroupingAvgState state) {
        int positions = state.largestGroupId + 1;
        double[] result = new double[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = (double) state.values.get(i) / state.counts.get(i);
        }
        return new DoubleArrayVector(result, positions).asBlock();
    }

    static class AvgState implements AggregatorState<AvgLongAggregator.AvgState> {

        long value;
        long count;

        private final AvgLongAggregator.AvgStateSerializer serializer;

        AvgState() {
            this(0, 0);
        }

        AvgState(long value, long count) {
            this.value = value;
            this.count = count;
            this.serializer = new AvgLongAggregator.AvgStateSerializer();
        }

        @Override
        public long getEstimatedSize() {
            return AvgLongAggregator.AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<AvgLongAggregator.AvgState> serializer() {
            return serializer;
        }
    }

    // @SerializedSize(value = Long.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgLongAggregator.AvgState> {

        // record Shape (long value, long count) {}

        static final int BYTES_SIZE = Long.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgLongAggregator.AvgState value, byte[] ba, int offset) {
            longHandle.set(ba, offset, value.value);
            longHandle.set(ba, offset + 8, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgLongAggregator.AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            long kvalue = (long) longHandle.get(ba, offset);
            long count = (long) longHandle.get(ba, offset + 8);

            value.value = kvalue;
            value.count = count;
        }
    }

    static class GroupingAvgState implements AggregatorState<GroupingAvgState> {
        private final BigArrays bigArrays;

        LongArray values;
        LongArray counts;

        // total number of groups; <= values.length
        int largestGroupId;

        private final GroupingAvgStateSerializer serializer;

        GroupingAvgState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.values = bigArrays.newLongArray(1);
                this.counts = bigArrays.newLongArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
            this.serializer = new GroupingAvgStateSerializer();
        }

        void add(long valueToAdd, int groupId, long increment) {
            if (groupId > largestGroupId) {
                largestGroupId = groupId;
                values = bigArrays.grow(values, groupId + 1);
                counts = bigArrays.grow(counts, groupId + 1);
            }
            values.set(groupId, Math.addExact(values.get(groupId), valueToAdd));
            counts.increment(groupId, increment);
        }

        @Override
        public long getEstimatedSize() {
            return Long.BYTES + (largestGroupId + 1) * AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public AggregatorStateSerializer<GroupingAvgState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(values, counts);
        }
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class GroupingAvgStateSerializer implements AggregatorStateSerializer<GroupingAvgState> {

        // record Shape (double value, double delta, long count) {}

        static final int BYTES_SIZE = Long.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(GroupingAvgState state, byte[] ba, int offset) {
            int positions = state.largestGroupId + 1;
            longHandle.set(ba, offset, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                longHandle.set(ba, offset, state.values.get(i));
                longHandle.set(ba, offset + 8, state.counts.get(i));
                offset += BYTES_SIZE;
            }
            return 8 + (BYTES_SIZE * positions); // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(GroupingAvgState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            // TODO replace deserialization with direct passing - no more non_recycling_instance then
            state.values = BigArrays.NON_RECYCLING_INSTANCE.grow(state.values, positions);
            state.counts = BigArrays.NON_RECYCLING_INSTANCE.grow(state.counts, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                state.values.set(i, (long) longHandle.get(ba, offset));
                state.counts.set(i, (long) longHandle.get(ba, offset + 8));
                offset += BYTES_SIZE;
            }
            state.largestGroupId = positions - 1;
        }
    }
}
