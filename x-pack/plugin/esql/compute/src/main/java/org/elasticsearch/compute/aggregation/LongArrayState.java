/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;

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

    private BitArray nonNulls;

    private final LongArrayStateSerializer serializer;

    LongArrayState(BigArrays bigArrays, long initialDefaultValue) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newLongArray(1, false);
        this.values.set(0, initialDefaultValue);
        this.initialDefaultValue = initialDefaultValue;
        this.serializer = new LongArrayStateSerializer();
    }

    long get(int index) {
        // TODO bounds check
        return values.get(index);
    }

    void increment(long value, int index) {
        ensureCapacity(index);
        values.increment(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

    void set(long value, int index) {
        ensureCapacity(index);
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

    void putNull(int index) {
        ensureCapacity(index);
        if (nonNulls == null) {
            nonNulls = new BitArray(index + 1, bigArrays);
            for (int i = 0; i < index; i++) {
                nonNulls.set(i); // TODO: bulk API
            }
        } else {
            nonNulls.ensureCapacity(index);
        }
    }

    boolean hasValue(int index) {
        return nonNulls == null || nonNulls.get(index);
    }

    Block toValuesBlock() {
        final int positions = largestIndex + 1;
        if (nonNulls == null) {
            final long[] vs = new long[positions];
            for (int i = 0; i < positions; i++) {
                vs[i] = values.get(i);
            }
            return new LongArrayVector(vs, positions).asBlock();
        } else {
            final LongBlock.Builder builder = LongBlock.newBlockBuilder(positions);
            for (int i = 0; i < positions; i++) {
                if (hasValue(i)) {
                    builder.appendLong(values.get(i));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    long getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : initialDefaultValue;
    }

    private void ensureCapacity(int position) {
        if (position > largestIndex) {
            largestIndex = position;
        }
        if (position >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, position + 1);
            values.fill(prevSize, values.size(), initialDefaultValue);
        }
    }

    @Override
    public long getEstimatedSize() {
        final long positions = largestIndex + 1L;
        return Long.BYTES + (positions * Long.BYTES) + estimateSerializeSize(nonNulls);
    }

    @Override
    public void close() {
        Releasables.close(values, nonNulls);
    }

    @Override
    public AggregatorStateSerializer<LongArrayState> serializer() {
        return serializer;
    }

    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    static int estimateSerializeSize(BitArray bits) {
        if (bits == null) {
            return Long.BYTES;
        } else {
            return Long.BYTES + Math.toIntExact(bits.getBits().size() * Long.BYTES);
        }
    }

    static int serializeBitArray(BitArray bits, byte[] ba, int offset) {
        if (bits == null) {
            longHandle.set(ba, offset, 0);
            return Long.BYTES;
        }
        final LongArray array = bits.getBits();
        longHandle.set(ba, offset, array.size());
        offset += Long.BYTES;
        for (long i = 0; i < array.size(); i++) {
            longHandle.set(ba, offset, array.get(i));
        }
        return Long.BYTES + Math.toIntExact(array.size() * Long.BYTES);
    }

    static BitArray deseralizeBitArray(BigArrays bigArrays, byte[] ba, int offset) {
        long size = (long) longHandle.get(ba, offset);
        if (size == 0) {
            return null;
        } else {
            offset += Long.BYTES;
            final LongArray array = bigArrays.newLongArray(size);
            for (long i = 0; i < size; i++) {
                array.set(i, (long) longHandle.get(ba, offset));
            }
            return new BitArray(bigArrays, array);
        }
    }

    static class LongArrayStateSerializer implements AggregatorStateSerializer<LongArrayState> {

        static final int BYTES_SIZE = Long.BYTES;

        @Override
        public int size() {
            return BYTES_SIZE;
        }

        @Override
        public int serialize(LongArrayState state, byte[] ba, int offset) {
            int positions = state.largestIndex + 1;
            longHandle.set(ba, offset, positions);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                longHandle.set(ba, offset, state.values.get(i));
                offset += BYTES_SIZE;
            }
            final int valuesBytes = Long.BYTES + (BYTES_SIZE * positions) + Long.BYTES;
            return valuesBytes + serializeBitArray(state.nonNulls, ba, offset);
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
            state.nonNulls = deseralizeBitArray(state.bigArrays, ba, offset);
        }
    }
}
