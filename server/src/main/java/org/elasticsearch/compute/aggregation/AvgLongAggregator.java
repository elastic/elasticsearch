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
class AvgLongAggregator implements AggregatorFunction {

    private final AvgState state;
    private final int channel;

    static AvgLongAggregator create(int inputChannel) {
        return new AvgLongAggregator(inputChannel, new AvgState());
    }

    private AvgLongAggregator(int channel, AvgState state) {
        this.channel = channel;
        this.state = state;
    }

    @Override
    public void addRawInput(Page page) {
        assert channel >= 0;
        Block block = page.getBlock(channel);
        Optional<Vector> singleValued = page.getBlock(channel).asVector();
        if (singleValued.isPresent()) {
            addRawInputFromSingleValued(singleValued.get());
        } else {
            addRawInputFromBlock(block);
        }
    }

    final void addRawInputFromSingleValued(Vector block) {
        AvgState state = this.state;
        for (int i = 0; i < block.getPositionCount(); i++) {
            state.value = Math.addExact(state.value, block.getLong(i));
        }
        state.count += block.getPositionCount();
    }

    final void addRawInputFromBlock(Block block) {
        AvgState state = this.state;
        for (int i = 0; i < block.getPositionCount(); i++) {  // TODO: this is not correct, should be value count?
            state.value = Math.addExact(state.value, block.getLong(i));
        }
        state.count += block.validPositionCount();
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
                state.value = Math.addExact(state.value, tmpState.value);
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
        double result = ((double) s.value) / s.count;
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

        long value;
        long count;

        private final AvgStateSerializer serializer;

        AvgState() {
            this(0, 0);
        }

        AvgState(long value, long count) {
            this.value = value;
            this.count = count;
            this.serializer = new AvgStateSerializer();
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

    // @SerializedSize(value = Long.BYTES + Long.BYTES)
    static class AvgStateSerializer implements AggregatorStateSerializer<AvgState> {

        // record Shape (long value, long count) {}

        static final int BYTES_SIZE = Long.BYTES + Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(AvgState value, byte[] ba, int offset) {
            longHandle.set(ba, offset, value.value);
            longHandle.set(ba, offset + 8, value.count);
            return BYTES_SIZE; // number of bytes written
        }

        // sets the state in value
        @Override
        public void deserialize(AvgState value, byte[] ba, int offset) {
            Objects.requireNonNull(value);
            long kvalue = (long) longHandle.get(ba, offset);
            long count = (long) longHandle.get(ba, offset + 8);

            value.value = kvalue;
            value.count = count;
        }
    }
}
