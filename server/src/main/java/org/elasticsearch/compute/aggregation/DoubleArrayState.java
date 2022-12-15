/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.Experimental;

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

    private final DoubleArrayStateSerializer serializer;

    DoubleArrayState(double initialDefaultValue) {  // For now, to shortcut refactoring. Remove
        this(new double[1], initialDefaultValue, BigArrays.NON_RECYCLING_INSTANCE);
        values.set(0, initialDefaultValue);
    }

    DoubleArrayState(double[] values, double initialDefaultValue, BigArrays bigArrays) {
        this.values = bigArrays.newDoubleArray(values.length, false);
        for (int i = 0; i < values.length; i++) {
            this.values.set(i, values[i]);
        }
        this.initialDefaultValue = initialDefaultValue;
        this.bigArrays = bigArrays;
        this.serializer = new DoubleArrayStateSerializer();
    }

    double get(int index) {
        // TODO bounds check
        return values.get(index);
    }

    double getOrDefault(int index) {
        if (index > largestIndex) {
            return initialDefaultValue;
        } else {
            return values.get(index);
        }
    }

    void set(double value, int index) {
        ensureCapacity(index);
        if (index > largestIndex) {
            largestIndex = index;
        }
        values.set(index, value);
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
        return Long.BYTES + (largestIndex + 1) * Double.BYTES;
    }

    @Override
    public void close() {
        values.close();
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
            return Long.BYTES + (BYTES_SIZE * positions); // number of bytes written
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
        }
    }
}
