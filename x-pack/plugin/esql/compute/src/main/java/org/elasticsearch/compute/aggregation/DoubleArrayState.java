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
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Experimental
final class DoubleArrayState implements AggregatorState<DoubleArrayState> {

    private final BigArrays bigArrays;

    private final double initialDefaultValue;

    private DoubleArray values;
    // total number of groups; <= values.length
    int largestIndex;
    private BitArray nonNulls;

    private final DoubleArrayStateSerializer serializer;

    DoubleArrayState(BigArrays bigArrays, double initialDefaultValue) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newDoubleArray(1, false);
        this.values.set(0, initialDefaultValue);
        this.initialDefaultValue = initialDefaultValue;
        this.serializer = new DoubleArrayStateSerializer();
    }

    double get(int index) {
        // TODO bounds check
        return values.get(index);
    }

    double getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : initialDefaultValue;
    }

    void set(double value, int index) {
        ensureCapacity(index);
        if (index > largestIndex) {
            largestIndex = index;
        }
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

    void putNull(int index) {
        if (index > largestIndex) {
            largestIndex = index;
        }
        ensureCapacity(index);
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

    Block toValuesBlock() {
        final int positions = largestIndex + 1;
        if (nonNulls == null) {
            final double[] vs = new double[positions];
            for (int i = 0; i < positions; i++) {
                vs[i] = values.get(i);
            }
            return new DoubleArrayVector(vs, positions).asBlock();
        } else {
            final DoubleBlock.Builder builder = DoubleBlock.newBlockBuilder(positions);
            for (int i = 0; i < positions; i++) {
                if (hasValue(i)) {
                    builder.appendDouble(values.get(i));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int position) {
        if (position >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, position + 1);
            values.fill(prevSize, values.size(), initialDefaultValue);
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
        return serializer;
    }

    static class DoubleArrayStateSerializer implements AggregatorStateSerializer<DoubleArrayState> {

        static final int BYTES_SIZE = Double.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(DoubleArrayState state, byte[] ba, int offset) {
            int positions = state.largestIndex + 1;
            longHandle.set(ba, offset, positions);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                doubleHandle.set(ba, offset, state.values.get(i));
                offset += BYTES_SIZE;
            }
            final int valuesBytes = Long.BYTES + (BYTES_SIZE * positions);
            return valuesBytes + LongArrayState.serializeBitArray(state.nonNulls, ba, offset);

        }

        @Override
        public void deserialize(DoubleArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                state.set((double) doubleHandle.get(ba, offset), i);
                offset += BYTES_SIZE;
            }
            state.largestIndex = positions - 1;
            state.nonNulls = LongArrayState.deseralizeBitArray(state.bigArrays, ba, offset);
        }
    }
}
