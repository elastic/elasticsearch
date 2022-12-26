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
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;

@Experimental
final class GroupingAvgAggregator implements GroupingAggregatorFunction {

    private final GroupingAvgState state;
    private final int channel;

    static GroupingAvgAggregator create(BigArrays bigArrays, int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingAvgAggregator(inputChannel, new GroupingAvgState(bigArrays));
    }

    static GroupingAvgAggregator createIntermediate(BigArrays bigArrays) {
        return new GroupingAvgAggregator(-1, new GroupingAvgState(bigArrays));
    }

    private GroupingAvgAggregator(int channel, GroupingAvgState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Vector groupIdVector, Page page) {
        assert channel >= 0;
        Block valuesBlock = page.getBlock(channel);
        Optional<Vector> vector = valuesBlock.asVector();
        if (vector.isPresent()) {
            addRawInputFromVector(groupIdVector, vector.get());
        } else {
            addRawInputFromBlock(groupIdVector, valuesBlock);
        }
    }

    private void addRawInputFromVector(Vector groupIdVector, Vector valuesVector) {
        final GroupingAvgState state = this.state;
        final int len = valuesVector.getPositionCount();
        for (int i = 0; i < len; i++) {
            state.add(valuesVector.getDouble(i), Math.toIntExact(groupIdVector.getLong(i)));
        }
    }

    private void addRawInputFromBlock(Vector groupIdVector, Block valuesBlock) {
        final GroupingAvgState state = this.state;
        final int len = groupIdVector.getPositionCount();
        for (int i = 0; i < len; i++) {
            if (valuesBlock.isNull(i) == false) {
                final int groupId = Math.toIntExact(groupIdVector.getLong(i));
                final int firstValueIndex = valuesBlock.getFirstValueIndex(i);
                for (int offset = 0; offset < valuesBlock.getValueCount(i); offset++) {
                    state.add(valuesBlock.getDouble(firstValueIndex + offset), groupId);
                }
            }
        }
    }

    @Override
    public void addIntermediateInput(Vector groupIdVector, Block block) {
        assert channel == -1;
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() && vector.get() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<GroupingAvgState> blobVector = (AggregatorStateVector<GroupingAvgState>) vector.get();
            // TODO exchange big arrays directly without funny serialization - no more copying
            GroupingAvgState tmpState = new GroupingAvgState(BigArrays.NON_RECYCLING_INSTANCE);
            blobVector.get(0, tmpState);
            this.state.addIntermediate(groupIdVector, tmpState);
        } else {
            throw new RuntimeException("expected AggregatorStateVector, got:" + block);
        }
    }

    @Override
    public void addIntermediateRowInput(int groupId, GroupingAggregatorFunction input, int position) {
        if (input.getClass() != getClass()) {
            throw new IllegalArgumentException("expected " + getClass() + " ; got " + input.getClass());
        }
        final GroupingAvgState inState = ((GroupingAvgAggregator) input).state;
        state.add(inState.values.get(position), inState.deltas.get(position), groupId, inState.counts.get(position));
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateVector.Builder<AggregatorStateVector<GroupingAvgState>, GroupingAvgState> builder = AggregatorStateVector
            .builderOfAggregatorState(GroupingAvgState.class, state.getEstimatedSize());
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {  // assume block positions == groupIds
        GroupingAvgState s = state;
        int positions = s.largestGroupId + 1;
        double[] result = new double[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = s.values.get(i) / s.counts.get(i);
        }
        return new DoubleVector(result, positions).asBlock();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel).append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }

    static class GroupingAvgState implements AggregatorState<GroupingAvgState> {
        private final BigArrays bigArrays;

        DoubleArray values;
        DoubleArray deltas;
        LongArray counts;

        // total number of groups; <= values.length
        int largestGroupId;

        private final AvgStateSerializer serializer;

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
            this.serializer = new AvgStateSerializer();
        }

        void addIntermediate(Vector groupIdVector, GroupingAvgState state) {
            final int positions = groupIdVector.getPositionCount();
            for (int i = 0; i < positions; i++) {
                int groupId = Math.toIntExact(groupIdVector.getLong(i));
                add(state.values.get(i), state.deltas.get(i), groupId, state.counts.get(i));
            }
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
            return Long.BYTES + (largestGroupId + 1) * AvgStateSerializer.BYTES_SIZE;
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
    static class AvgStateSerializer implements AggregatorStateSerializer<GroupingAvgState> {

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
