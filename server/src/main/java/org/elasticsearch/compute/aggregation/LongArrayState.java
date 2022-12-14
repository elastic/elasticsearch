/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.Experimental;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

@Experimental
final class LongArrayState implements AggregatorState<LongArrayState> {

    private final BigArrays bigArrays;

    private final long initialDefaultValue;

    private LongArray values;
    // total number of groups; <= values.length
    int largestIndex;

    private final LongArrayStateSerializer serializer;

    LongArrayState(long initialDefaultValue) {
        this(new long[1], initialDefaultValue, BigArrays.NON_RECYCLING_INSTANCE);
    }

    LongArrayState(long[] values, long initialDefaultValue, BigArrays bigArrays) {
        this.values = bigArrays.newLongArray(values.length, false);
        for (int i = 0; i < values.length; i++) {
            this.values.set(i, values[i]);
        }
        this.initialDefaultValue = initialDefaultValue;
        this.bigArrays = bigArrays;
        this.serializer = new LongArrayStateSerializer();
    }

    long get(int index) {
        // TODO bounds check
        return values.get(index);
    }

    void increment(long value, int index) {
        ensureCapacity(index);
        if (index > largestIndex) {
            largestIndex = index;
        }
        values.increment(index, value);
    }

    void set(long value, int index) {
        ensureCapacity(index);
        if (index > largestIndex) {
            largestIndex = index;
        }
        values.set(index, value);
    }

    private void ensureCapacity(int position) {
        if (position >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, prevSize + 1);
            values.fill(prevSize, values.size(), initialDefaultValue);
        }
    }

    @Override
    public long getEstimatedSize() {
        return Long.BYTES + (largestIndex + 1) * Long.BYTES;
    }

    @Override
    public void close() {
        values.close();
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
                longHandle.set(ba, offset, state.values.get(i));
                offset += BYTES_SIZE;
            }
            return Long.BYTES + (BYTES_SIZE * positions); // number of bytes written
        }

        @Override
        public void deserialize(LongArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) longHandle.get(ba, offset);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                state.set((long) longHandle.get(ba, offset), i);
                offset += BYTES_SIZE;
            }
            state.largestIndex = positions - 1;
        }
    }
}
