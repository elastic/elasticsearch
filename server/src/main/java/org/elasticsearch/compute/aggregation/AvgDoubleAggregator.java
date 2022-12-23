/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.AggregatorStateVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;

@Experimental
class AvgDoubleAggregator implements AggregatorFunction {

    private final AvgState state;
    private final int channel;

    static AvgDoubleAggregator create(int inputChannel) {
        return new AvgDoubleAggregator(inputChannel, new AvgState());
    }

    private AvgDoubleAggregator(int channel, AvgState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block valuesBlock = page.getBlock(channel);
        Optional<Vector> vector = valuesBlock.asVector();
        if (vector.isPresent()) {
            addRawInputFromVector(vector.get());
        } else {
            addRawInputFromBlock(valuesBlock);
        }
    }

    private void addRawInputFromVector(Vector valuesVector) {
        final AvgState state = this.state;
        for (int i = 0; i < valuesVector.getPositionCount(); i++) {
            state.add(valuesVector.getDouble(i));
        }
        state.count += valuesVector.getPositionCount();
    }

    private void addRawInputFromBlock(Block valuesBlock) {
        final AvgState state = this.state;
        for (int i = 0; i < valuesBlock.getTotalValueCount(); i++) {  // all values, for now
            if (valuesBlock.isNull(i) == false) { // skip null values
                state.add(valuesBlock.getDouble(i));
            }
        }
        state.count += valuesBlock.validPositionCount();
    }

    @Override
    public void addIntermediateInput(Block block) {
        assert channel == -1;
        Optional<Vector> vector = block.asVector();
        if (vector.isPresent() && vector.get() instanceof AggregatorStateVector) {
            @SuppressWarnings("unchecked")
            AggregatorStateVector<AvgState> blobBlock = (AggregatorStateVector<AvgState>) vector.get();
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
        AggregatorStateVector.Builder<AggregatorStateVector<AvgState>, AvgState> builder = AggregatorStateVector.builderOfAggregatorState(
            AvgState.class,
            state.getEstimatedSize()
        );
        builder.add(state);
        return builder.build().asBlock();
    }

    @Override
    public Block evaluateFinal() {
        AvgState s = state;
        double result = s.value / s.count;
        return BlockBuilder.newConstantDoubleBlockWith(result, 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channel=").append(channel);
        sb.append("]");
        return sb.toString();
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
        public long getEstimatedSize() {
            return AvgStateSerializer.BYTES_SIZE;
        }

        @Override
        public void close() {}

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
