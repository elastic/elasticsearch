/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Experimental;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

@Experimental
final class LongArrayState implements AggregatorState<LongArrayState> {

    private long[] values;
    // total number of groups; <= values.length
    int largestIndex;

    private final LongArrayStateSerializer serializer;

    LongArrayState(long... values) {
        this.values = values;
        this.serializer = new LongArrayStateSerializer();
    }

    long[] getValues() {
        return values;
    }

    long get(int index) {
        // TODO bounds check
        return values[index];
    }

    long getOrDefault(int index, long defaultValue) {
        if (index > largestIndex) {
            return defaultValue;
        } else {
            return values[index];
        }
    }

    void set(long value, int index) {
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
    public long getEstimatedSize() {
        return Long.BYTES + (largestIndex + 1) * Long.BYTES;
    }

    @Override
    public AggregatorStateSerializer<LongArrayState> serializer() {
        return serializer;
    }

    static class LongArrayStateSerializer implements AggregatorStateSerializer<LongArrayState> {

        static final int BYTES_SIZE = Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int serialize(LongArrayState state, byte[] ba, int offset) {
            int positions = state.largestIndex + 1;
            longHandle.set(ba, offset, positions);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                longHandle.set(ba, offset, state.values[i]);
                offset += BYTES_SIZE;
            }
            return Long.BYTES + (BYTES_SIZE * positions); // number of bytes written
        }

        @Override
        public void deserialize(LongArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            offset += Long.BYTES;
            long[] values = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = (long) longHandle.get(ba, offset);
                offset += BYTES_SIZE;
            }
            state.values = values;
            state.largestIndex = positions - 1;
        }
    }
}
