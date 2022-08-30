/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import org.elasticsearch.xpack.sql.action.compute.data.AggregatorStateBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Block;
import org.elasticsearch.xpack.sql.action.compute.data.DoubleBlock;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

class GroupingAvgAggregator implements GroupingAggregatorFunction {

    private final GroupingAvgState state;
    private final int channel;

    static GroupingAvgAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new GroupingAvgAggregator(inputChannel, new GroupingAvgState());
    }

    static GroupingAvgAggregator createIntermediate() {
        return new GroupingAvgAggregator(-1, new GroupingAvgState());
    }

    private GroupingAvgAggregator(int channel, GroupingAvgState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Block groupIdBlock, Page page) {
        assert channel >= 0;
        Block valuesBlock = page.getBlock(channel);
        GroupingAvgState state = this.state;
        for (int i = 0; i < valuesBlock.getPositionCount(); i++) {
            int groupId = (int) groupIdBlock.getLong(i);
            state.add(valuesBlock.getDouble(i), groupId);
            state.counts[groupId]++;
        }
    }

    @Override
    public void addIntermediateInput(Block groupIdBlock, Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<GroupingAvgState> blobBlock = (AggregatorStateBlock<GroupingAvgState>) block;
            GroupingAvgState state = this.state;
            GroupingAvgState tmpState = new GroupingAvgState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                long groupId = groupIdBlock.getLong(i);
                blobBlock.get(i, tmpState);
                state.add(tmpState.values[i], tmpState.deltas[i], (int) groupId);
                state.counts[(int) groupId]++;
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<GroupingAvgState>, GroupingAvgState> builder = AggregatorStateBlock
            .builderOfAggregatorState(GroupingAvgState.class);
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {  // assume block positions == groupIds
        GroupingAvgState s = state;
        int positions = s.counts.length;
        double[] result = new double[positions];
        for (int i = 0; i < positions; i++) {
            result[i] = s.values[i] / s.counts[i];
        }
        return new DoubleBlock(result, positions);
    }

    static class GroupingAvgState implements AggregatorState<GroupingAvgState> {

        double[] values;
        double[] deltas;
        long[] counts;

        // TODO prototype:
        // 1. BigDoubleArray BigDoubleArray, BigLongArray
        // 2. big byte array

        private final AvgStateSerializer serializer;

        GroupingAvgState() {
            this(new double[1], new double[1], new long[1]);
        }

        GroupingAvgState(double[] value, double[] delta, long[] count) {
            this.values = value;
            this.deltas = delta;
            this.counts = count;
            this.serializer = new AvgStateSerializer();
        }

        void add(double valueToAdd) {
            add(valueToAdd, 0d, 0);
        }

        void add(double valueToAdd, int position) {
            ensureCapacity(position);
            add(valueToAdd, 0d, position);
        }

        private void ensureCapacity(int position) {
            if (position >= values.length) {
                int newSize = values.length << 1;  // trivial
                values = Arrays.copyOf(values, newSize);
                deltas = Arrays.copyOf(deltas, newSize);
                counts = Arrays.copyOf(counts, newSize);
            }
        }

        void add(double valueToAdd, double deltaToAdd, int position) {
            // If the value is Inf or NaN, just add it to the running tally to "convert" to
            // Inf/NaN. This keeps the behavior bwc from before kahan summing
            if (Double.isFinite(valueToAdd) == false) {
                values[position] = valueToAdd + values[position];
            }

            if (Double.isFinite(values[position])) {
                double correctedSum = valueToAdd + (deltas[position] + deltaToAdd);
                double updatedValue = values[position] + correctedSum;
                deltas[position] = correctedSum - (updatedValue - values[position]);
                values[position] = updatedValue;
            }
        }

        @Override
        public AggregatorStateSerializer<GroupingAvgState> serializer() {
            return serializer;
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
            int positions = state.values.length;
            longHandle.set(ba, offset, positions);
            offset += 8;
            for (int i = 0; i < positions; i++) {
                doubleHandle.set(ba, offset, state.values[i]);
                doubleHandle.set(ba, offset + 8, state.deltas[i]);
                longHandle.set(ba, offset + 16, state.counts[i]);
                offset += BYTES_SIZE;
            }
            return 8 + (BYTES_SIZE * positions); // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(GroupingAvgState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            offset += 8;
            double[] values = new double[positions];
            double[] deltas = new double[positions];
            long[] counts = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = (double) doubleHandle.get(ba, offset);
                deltas[i] = (double) doubleHandle.get(ba, offset + 8);
                counts[i] = (long) longHandle.get(ba, offset + 16);
                offset += BYTES_SIZE;
            }
            state.values = values;
            state.deltas = deltas;
            state.counts = counts;
        }
    }
}
