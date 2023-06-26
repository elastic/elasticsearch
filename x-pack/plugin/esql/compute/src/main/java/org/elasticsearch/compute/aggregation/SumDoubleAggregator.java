/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Aggregator
@GroupingAggregator
class SumDoubleAggregator {

    public static SumState initSingle() {
        return new SumState();
    }

    public static void combine(SumState current, double v) {
        current.add(v);
    }

    public static void combineStates(SumState current, SumState state) {
        current.add(state.value(), state.delta());
    }

    public static Block evaluateFinal(SumState state) {
        double result = state.value();
        return DoubleBlock.newConstantBlockWith(result, 1);
    }

    public static GroupingSumState initGrouping(BigArrays bigArrays) {
        return new GroupingSumState(bigArrays);
    }

    public static void combine(GroupingSumState current, int groupId, double v) {
        current.add(v, groupId);
    }

    public static void combineStates(GroupingSumState current, int groupId, GroupingSumState state, int statePosition) {
        if (state.hasValue(statePosition)) {
            current.add(state.values.get(statePosition), state.deltas.get(statePosition), groupId);
        } else {
            current.putNull(groupId);
        }
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected) {
        DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            if (state.hasValue(i)) {
                builder.appendDouble(state.values.get(selected.getInt(i)));
            } else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    static class SumState extends CompensatedSum implements AggregatorState<SumState> {

        private final SumStateSerializer serializer;
        private boolean seen;

        SumState() {
            this(0, 0);
        }

        SumState(double value, double delta) {
            super(value, delta);
            this.serializer = new SumStateSerializer();
        }

        @Override
        public long getEstimatedSize() {
            return SumStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<SumState> serializer() {
            return serializer;
        }

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }
    }

    static class SumStateSerializer implements AggregatorStateSerializer<SumState> {

        // record Shape (double value, double delta, boolean seen) {}
        static final int BYTES_SIZE = Double.BYTES + Double.BYTES + 1;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(SumState value, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            doubleHandle.set(ba, offset, value.value());
            doubleHandle.set(ba, offset + Double.BYTES, value.delta());
            ba[offset + Double.BYTES + Double.BYTES] = (byte) (value.seen ? 1 : 0);
            return size(); // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(SumState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            double kvalue = (double) doubleHandle.get(ba, offset);
            double kdelta = (double) doubleHandle.get(ba, offset + Double.BYTES);
            value.seen = ba[offset + Double.BYTES + Double.BYTES] == (byte) 1;
            value.reset(kvalue, kdelta);
        }
    }

    static class GroupingSumState implements AggregatorState<GroupingSumState> {
        private final BigArrays bigArrays;
        static final long BYTES_SIZE = Double.BYTES + Double.BYTES;

        DoubleArray values;
        DoubleArray deltas;

        // total number of groups; <= values.length
        int largestGroupId;

        private final GroupingSumStateSerializer serializer;
        private BitArray nonNulls;

        GroupingSumState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(1);
                this.deltas = bigArrays.newDoubleArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
            this.serializer = new GroupingSumStateSerializer();
        }

        void add(double valueToAdd, int groupId) {
            add(valueToAdd, 0d, groupId);
        }

        void add(double valueToAdd, double deltaToAdd, int groupId) {
            ensureCapacity(groupId);

            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                values.increment(groupId, valueToAdd);
                return;
            }

            double value = values.get(groupId);
            if (Double.isFinite(value) == false) {
                // It isn't going to get any more infinite.
                return;
            }
            double delta = deltas.get(groupId);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(groupId, correctedSum - (updatedValue - value));
            values.set(groupId, updatedValue);
            if (nonNulls != null) {
                nonNulls.set(groupId);
            }
        }

        void putNull(int groupId) {
            if (groupId > largestGroupId) {
                ensureCapacity(groupId);
                largestGroupId = groupId;
            }
            if (nonNulls == null) {
                nonNulls = new BitArray(groupId + 1, bigArrays);
                for (int i = 0; i < groupId; i++) {
                    nonNulls.set(i);
                }
            } else {
                nonNulls.ensureCapacity(groupId + 1);
            }
        }

        boolean hasValue(int index) {
            return nonNulls == null || nonNulls.get(index);
        }

        private void ensureCapacity(int groupId) {
            if (groupId > largestGroupId) {
                largestGroupId = groupId;
                values = bigArrays.grow(values, groupId + 1);
                deltas = bigArrays.grow(deltas, groupId + 1);
            }
        }

        @Override
        public long getEstimatedSize() {
            return Long.BYTES + (largestGroupId + 1) * BYTES_SIZE + LongArrayState.estimateSerializeSize(nonNulls);
        }

        @Override
        public AggregatorStateSerializer<GroupingSumState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(values, deltas, nonNulls);
        }
    }

    static class GroupingSumStateSerializer implements AggregatorStateSerializer<GroupingSumState> {

        // record Shape (double value, double delta) {}
        static final int BYTES_SIZE = Double.BYTES + Double.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(GroupingSumState state, byte[] ba, int offset, IntVector selected) {
            longHandle.set(ba, offset, selected.getPositionCount());
            offset += Long.BYTES;
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                doubleHandle.set(ba, offset, state.values.get(group));
                doubleHandle.set(ba, offset + 8, state.deltas.get(group));
                offset += BYTES_SIZE;
            }
            return 8 + (BYTES_SIZE * selected.getPositionCount()) + LongArrayState.serializeBitArray(state.nonNulls, ba, offset);
        }

        // sets the state in value
        @Override
        public void deserialize(GroupingSumState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            // TODO replace deserialization with direct passing - no more non_recycling_instance then
            state.values = BigArrays.NON_RECYCLING_INSTANCE.grow(state.values, positions);
            state.deltas = BigArrays.NON_RECYCLING_INSTANCE.grow(state.deltas, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                state.values.set(i, (double) doubleHandle.get(ba, offset));
                state.deltas.set(i, (double) doubleHandle.get(ba, offset + 8));
                offset += BYTES_SIZE;
            }
            state.largestGroupId = positions - 1;
            state.nonNulls = LongArrayState.deseralizeBitArray(state.bigArrays, ba, offset);
        }
    }
}
