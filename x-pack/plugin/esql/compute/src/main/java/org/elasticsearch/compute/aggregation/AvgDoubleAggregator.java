/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Aggregator
@GroupingAggregator
class AvgDoubleAggregator {
    public static AvgState initSingle() {
        return new AvgState();
    }

    public static void combine(AvgState current, double v) {
        current.add(v);
    }

    public static void combineStates(AvgState current, AvgState state) {
        current.add(state.value, state.delta);
        current.count += state.count;
    }

    public static Block evaluateFinal(AvgState state) {
        double result = state.value / state.count;
        return BlockBuilder.newConstantDoubleBlockWith(result, 1);
    }

    public static GroupingAvgState initGrouping(BigArrays bigArrays) {
        return new GroupingAvgState(bigArrays);
    }

    public static void combineValueCount(AvgState current, int positions) {
        current.count += positions;
    }

    public static void combine(GroupingAvgState current, int groupId, double v) {
        current.add(v, groupId);
    }

    public static void combineStates(GroupingAvgState current, int currentGroupId, GroupingAvgState state, int statePosition) {
        current.add(state.values.get(statePosition), state.deltas.get(statePosition), currentGroupId, state.counts.get(statePosition));
    }

    public static Block evaluateFinal(GroupingAvgState state) {
        int positions = state.largestGroupId + 1;
        double[] result = new double[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = state.values.get(i) / state.counts.get(i);
        }
        return new DoubleVector(result, positions).asBlock();
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgState implements AggregatorState<AvgDoubleAggregator.AvgState> {

        private double value;
        private double delta;

        private long count;

        private final AvgDoubleAggregator.AvgStateSerializer serializer;

        AvgState() {
            this(0, 0, 0);
        }

        AvgState(double value, double delta, long count) {
            this.value = value;
            this.delta = delta;
            this.count = count;
            this.serializer = new AvgDoubleAggregator.AvgStateSerializer();
        }

        void add(double valueToAdd) {
            add(valueToAdd, 0d);
        }

        void add(double valueToAdd, double deltaToAdd) {
            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                value = valueToAdd + value;
            }

            if (Double.isFinite(value)) {
                double correctedSum = valueToAdd + (delta + deltaToAdd);
                double updatedValue = value + correctedSum;
                delta = correctedSum - (updatedValue - value);
                value = updatedValue;
            }
        }

        @Override
        public long getEstimatedSize() {
            return AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

        @Override
        public AggregatorStateSerializer<AvgDoubleAggregator.AvgState> serializer() {
            return serializer;
        }
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgDoubleAggregator.AvgState> {

        // record Shape (double value, double delta, long count) {}

        static final int BYTES_SIZE = Double.BYTES + Double.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgDoubleAggregator.AvgState value, byte[] ba, int offset) {
            doubleHandle.set(ba, offset, value.value);
            doubleHandle.set(ba, offset + 8, value.delta);
            longHandle.set(ba, offset + 16, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgDoubleAggregator.AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            double kvalue = (double) doubleHandle.get(ba, offset);
            double kdelta = (double) doubleHandle.get(ba, offset + 8);
            long count = (long) longHandle.get(ba, offset + 16);

            value.value = kvalue;
            value.delta = kdelta;
            value.count = count;
        }
    }

    static class GroupingAvgState implements AggregatorState<GroupingAvgState> {
        private final BigArrays bigArrays;

        DoubleArray values;
        DoubleArray deltas;
        LongArray counts;

        // total number of groups; <= values.length
        int largestGroupId;

        private final GroupingAvgStateSerializer serializer;

        GroupingAvgState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            try {
                this.values = bigArrays.newDoubleArray(1);
                this.deltas = bigArrays.newDoubleArray(1);
                this.counts = bigArrays.newLongArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
            this.serializer = new GroupingAvgStateSerializer();
        }

        void add(double valueToAdd, int groupId) {
            add(valueToAdd, 0d, groupId, 1);
        }

        void add(double valueToAdd, double deltaToAdd, int groupId, long increment) {
            if (groupId > largestGroupId) {
                largestGroupId = groupId;
                values = bigArrays.grow(values, groupId + 1);
                deltas = bigArrays.grow(deltas, groupId + 1);
                counts = bigArrays.grow(counts, groupId + 1);
            }
            add(valueToAdd, deltaToAdd, groupId);
            counts.increment(groupId, increment);
        }

        void add(double valueToAdd, double deltaToAdd, int position) {
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

        @Override
        public long getEstimatedSize() {
            return Long.BYTES + (largestGroupId + 1) * GroupingAvgAggregator.AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public AggregatorStateSerializer<GroupingAvgState> serializer() {
            return serializer;
        }

        @Override
        public void close() {
            Releasables.close(values, deltas, counts);
        }
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class GroupingAvgStateSerializer implements AggregatorStateSerializer<GroupingAvgState> {

        // record Shape (double value, double delta, long count) {}

        static final int BYTES_SIZE = Double.BYTES + Double.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(GroupingAvgState state, byte[] ba, int offset) {
            int positions = state.largestGroupId + 1;
            longHandle.set(ba, offset, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                doubleHandle.set(ba, offset, state.values.get(i));
                doubleHandle.set(ba, offset + 8, state.deltas.get(i));
                longHandle.set(ba, offset + 16, state.counts.get(i));
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
            state.deltas = BigArrays.NON_RECYCLING_INSTANCE.grow(state.deltas, positions);
            state.counts = BigArrays.NON_RECYCLING_INSTANCE.grow(state.counts, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                state.values.set(i, (double) doubleHandle.get(ba, offset));
                state.deltas.set(i, (double) doubleHandle.get(ba, offset + 8));
                state.counts.set(i, (long) longHandle.get(ba, offset + 16));
                offset += BYTES_SIZE;
            }
            state.largestGroupId = positions - 1;
        }
    }
}
