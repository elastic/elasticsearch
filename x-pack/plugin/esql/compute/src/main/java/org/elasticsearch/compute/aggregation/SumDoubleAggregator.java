/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
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

    public static void combineStates(GroupingSumState current, int currentGroupId, GroupingSumState state, int statePosition) {
        current.add(state.values.get(statePosition), state.deltas.get(statePosition), currentGroupId);
    }

    public static Block evaluateFinal(GroupingSumState state, IntVector selected) {
        DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            builder.appendDouble(state.values.get(selected.getInt(i)));
        }
        return builder.build();
    }

    static class SumState extends CompensatedSum implements AggregatorState<SumState> {

        private final SumStateSerializer serializer;

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
    }

    static class SumStateSerializer implements AggregatorStateSerializer<SumState> {

        // record Shape (double value, double delta) {}
        static final int BYTES_SIZE = Double.BYTES + Double.BYTES;

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
            doubleHandle.set(ba, offset + 8, value.delta());
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(SumState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            double kvalue = (double) doubleHandle.get(ba, offset);
            double kdelta = (double) doubleHandle.get(ba, offset + 8);
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

        void add(double valueToAdd, double deltaToAdd, int position) {
            ensureCapacity(position);

            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                values.increment(position, valueToAdd);
                return;
            }

            double value = values.get(position);
            if (Double.isFinite(value) == false) {
                // It isn't going to get any more infinite.
                return;
            }
            double delta = deltas.get(position);
            double correctedSum = valueToAdd + (delta + deltaToAdd);
            double updatedValue = value + correctedSum;
            deltas.set(position, correctedSum - (updatedValue - value));
            values.set(position, updatedValue);
        }

        void putNull(int position) {
            // counts = 0 is for nulls
            ensureCapacity(position);
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
            return Long.BYTES + (largestGroupId + 1) * BYTES_SIZE;
        }

        @Override
        public AggregatorStateSerializer<GroupingSumState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(values, deltas);
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
            return 8 + (BYTES_SIZE * selected.getPositionCount()); // number of bytes written
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
        }
    }
}
