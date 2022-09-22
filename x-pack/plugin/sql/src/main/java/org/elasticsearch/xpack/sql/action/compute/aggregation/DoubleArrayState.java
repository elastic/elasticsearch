/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.aggregation;

import org.elasticsearch.xpack.sql.action.compute.data.Block;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

final class DoubleArrayState implements AggregatorState<DoubleArrayState> {

    private double[] values;
    // total number of groups; <= values.length
    int largestIndex;

    private final DoubleArrayStateSerializer serializer;

    DoubleArrayState() {
        this(new double[1]);
    }

    DoubleArrayState(double[] values) {
        this.values = values;
        this.serializer = new DoubleArrayStateSerializer();
    }

    void addIntermediate(Block groupIdBlock, DoubleArrayState state) {
        final double[] values = state.values;
        final int positions = groupIdBlock.getPositionCount();
        for (int i = 0; i < positions; i++) {
            int groupId = (int) groupIdBlock.getLong(i);
            set(Math.max(getOrDefault(groupId, Double.MIN_VALUE), values[i]), groupId);
        }
    }

    double get(int index) {
        // TODO bounds check
        return values[index];
    }

    double getOrDefault(int index, double defaultValue) {
        if (index > largestIndex) {
            return defaultValue;
        } else {
            return values[index];
        }
    }

    void set(double value, int index) {
        ensureCapacity(index);
        if (index > largestIndex) {
            largestIndex = index;
        }
        values[index] = value;
    }

    private void ensureCapacity(int position) {
        if (position >= values.length) {
            int newSize = values.length << 1;  // trivial
            values = Arrays.copyOf(values, newSize);
        }
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
                doubleHandle.set(ba, offset, state.values[i]);
                offset += BYTES_SIZE;
            }
            return Long.BYTES + (BYTES_SIZE * positions); // number of bytes written
        }

        @Override
        public void deserialize(DoubleArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            offset += Long.BYTES;
            double[] values = new double[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = (double) doubleHandle.get(ba, offset);
                offset += BYTES_SIZE;
            }
            state.values = values;
            state.largestIndex = positions - 1;
        }
    }
}
