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
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Aggregator state for an array of doubles.
 * This class is generated. Do not edit it.
 */
@Experimental
final class DoubleArrayState implements AggregatorState<DoubleArrayState> {
    private final BigArrays bigArrays;
    private final double init;

    private DoubleArray values;
    /**
     * Total number of groups {@code <=} values.length.
     */
    private int largestIndex;
    private BitArray nonNulls;

    DoubleArrayState(BigArrays bigArrays, double init) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newDoubleArray(1, false);
        this.values.set(0, init);
        this.init = init;
    }

    double get(int index) {
        return values.get(index);
    }

    double getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : init;
    }

    void set(double value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

    void putNull(int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        if (nonNulls == null) {
            nonNulls = new BitArray(index + 1, bigArrays);
            for (int i = 0; i < index; i++) {
                nonNulls.set(i);
            }
        } else {
            nonNulls.ensureCapacity(index + 1);
        }
    }

    boolean hasValue(int index) {
        return nonNulls == null || nonNulls.get(index);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected) {
        if (nonNulls == null) {
            DoubleVector.Builder builder = DoubleVector.newVectorBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                builder.appendDouble(values.get(selected.getInt(i)));
            }
            return builder.build().asBlock();
        }
        DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            if (hasValue(group)) {
                builder.appendDouble(values.get(group));
            } else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    private void ensureCapacity(int position) {
        if (position >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, position + 1);
            values.fill(prevSize, values.size(), init);
        }
    }

    @Override
    public long getEstimatedSize() {
        return Long.BYTES + (largestIndex + 1L) * Double.BYTES + LongArrayState.estimateSerializeSize(nonNulls);
    }

    @Override
    public void close() {
        Releasables.close(values, nonNulls);
    }

    @Override
    public AggregatorStateSerializer<DoubleArrayState> serializer() {
        return new DoubleArrayStateSerializer();
    }

    private static class DoubleArrayStateSerializer implements AggregatorStateSerializer<DoubleArrayState> {
        private static final VarHandle lengthHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle valueHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return Double.BYTES;
        }

        @Override
        public int serialize(DoubleArrayState state, byte[] ba, int offset, org.elasticsearch.compute.data.IntVector selected) {
            lengthHandle.set(ba, offset, selected.getPositionCount());
            offset += Long.BYTES;
            for (int i = 0; i < selected.getPositionCount(); i++) {
                valueHandle.set(ba, offset, state.values.get(selected.getInt(i)));
                offset += Double.BYTES;
            }
            final int valuesBytes = Long.BYTES + (Double.BYTES * selected.getPositionCount());
            return valuesBytes + LongArrayState.serializeBitArray(state.nonNulls, ba, offset);
        }

        @Override
        public void deserialize(DoubleArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) lengthHandle.get(ba, offset);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                state.set((double) valueHandle.get(ba, offset), i);
                offset += Double.BYTES;
            }
            state.largestIndex = positions - 1;
            state.nonNulls = LongArrayState.deseralizeBitArray(state.bigArrays, ba, offset);
        }
    }
}
