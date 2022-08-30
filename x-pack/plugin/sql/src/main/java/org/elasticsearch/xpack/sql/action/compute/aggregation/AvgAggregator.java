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
import java.util.Objects;

class AvgAggregator implements AggregatorFunction {

    private final AvgState state;
    private final int channel;

    static AvgAggregator create(int inputChannel) {
        if (inputChannel < 0) {
            throw new IllegalArgumentException();
        }
        return new AvgAggregator(inputChannel, new AvgState());
    }

    static AvgAggregator createIntermediate() {
        return new AvgAggregator(-1, new AvgState());
    }

    private AvgAggregator(int channel, AvgState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        AvgState state = this.state;
        for (int i = 0; i < block.getPositionCount(); i++) {
            state.add(block.getDouble(i));
        }
        state.count += block.getPositionCount();
    }

    @Override
    public void addIntermediateInput(Block block) {
        assert channel == -1;
        if (block instanceof AggregatorStateBlock) {
            @SuppressWarnings("unchecked")
            AggregatorStateBlock<AvgState> blobBlock = (AggregatorStateBlock<AvgState>) block;
            AvgState state = this.state;
            AvgState tmpState = new AvgState();
            for (int i = 0; i < block.getPositionCount(); i++) {
                blobBlock.get(i, tmpState);
                state.add(tmpState.value, tmpState.delta);
                state.count += tmpState.count;
            }
        } else {
            throw new RuntimeException("expected AggregatorStateBlock, got:" + block);
        }
    }

    @Override
    public Block evaluateIntermediate() {
        AggregatorStateBlock.Builder<AggregatorStateBlock<AvgState>, AvgState> builder = AggregatorStateBlock.builderOfAggregatorState(
            AvgState.class
        );
        builder.add(state);
        return builder.build();
    }

    @Override
    public Block evaluateFinal() {
        AvgState s = state;
        double result = s.value / s.count;
        return new DoubleBlock(new double[] { result }, 1);
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgState implements AggregatorState<AvgState> {

        private double value;
        private double delta;

        private long count;

        private final AvgStateSerializer serializer;

        AvgState() {
            this(0, 0, 0);
        }

        AvgState(double value, double delta, long count) {
            this.value = value;
            this.delta = delta;
            this.count = count;
            this.serializer = new AvgStateSerializer();
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
        public AggregatorStateSerializer<AvgState> serializer() {
            return serializer;
        }
    }

    // @SerializedSize(value = Double.BYTES + Double.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgState> {

        // record Shape (double value, double delta, long count) {}

        static final int BYTES_SIZE = Double.BYTES + Double.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgState value, byte[] ba, int offset) {
            doubleHandle.set(ba, offset, value.value);
            doubleHandle.set(ba, offset + 8, value.delta);
            longHandle.set(ba, offset + 16, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            double kvalue = (double) doubleHandle.get(ba, offset);
            double kdelta = (double) doubleHandle.get(ba, offset + 8);
            long count = (long) longHandle.get(ba, offset + 16);

            value.value = kvalue;
            value.delta = kdelta;
            value.count = count;
        }
    }
}
