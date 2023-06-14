/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.$Type$Array;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Aggregator state for an array of $type$s.
 * This class is generated. Do not edit it.
 */
@Experimental
final class $Type$ArrayState implements AggregatorState<$Type$ArrayState> {
    private final BigArrays bigArrays;
    private final $type$ init;

    private $Type$Array values;
    /**
     * Total number of groups {@code <=} values.length.
     */
    private int largestIndex;
    private BitArray nonNulls;

    $Type$ArrayState(BigArrays bigArrays, $type$ init) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.new$Type$Array(1, false);
        this.values.set(0, init);
        this.init = init;
    }

    $type$ get(int index) {
        return values.get(index);
    }

    $type$ getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : init;
    }

    void set($type$ value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

$if(long)$
    void increment(long value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.increment(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }
$endif$

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
            $Type$Vector.Builder builder = $Type$Vector.newVectorBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                builder.append$Type$(values.get(selected.getInt(i)));
            }
            return builder.build().asBlock();
        }
        $Type$Block.Builder builder = $Type$Block.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            if (hasValue(group)) {
                builder.append$Type$(values.get(group));
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
        return Long.BYTES + (largestIndex + 1L) * $BYTES$ + LongArrayState.estimateSerializeSize(nonNulls);
    }

    @Override
    public void close() {
        Releasables.close(values, nonNulls);
    }

    @Override
    public AggregatorStateSerializer<$Type$ArrayState> serializer() {
        return new $Type$ArrayStateSerializer();
    }

$if(long)$
    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

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

    static int estimateSerializeSize(BitArray bits) {
        if (bits == null) {
            return Long.BYTES;
        }
        return Long.BYTES + Math.toIntExact(bits.getBits().size() * Long.BYTES);
    }
$endif$

    private static class $Type$ArrayStateSerializer implements AggregatorStateSerializer<$Type$ArrayState> {
        private static final VarHandle lengthHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
        private static final VarHandle valueHandle = MethodHandles.byteArrayViewVarHandle($type$[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return $BYTES$;
        }

        @Override
        public int serialize($Type$ArrayState state, byte[] ba, int offset, org.elasticsearch.compute.data.IntVector selected) {
            lengthHandle.set(ba, offset, selected.getPositionCount());
            offset += Long.BYTES;
            for (int i = 0; i < selected.getPositionCount(); i++) {
                valueHandle.set(ba, offset, state.values.get(selected.getInt(i)));
                offset += $BYTES$;
            }
            final int valuesBytes = Long.BYTES + ($BYTES$ * selected.getPositionCount());
            return valuesBytes + LongArrayState.serializeBitArray(state.nonNulls, ba, offset);
        }

        @Override
        public void deserialize($Type$ArrayState state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            int positions = (int) (long) lengthHandle.get(ba, offset);
            offset += Long.BYTES;
            for (int i = 0; i < positions; i++) {
                state.set(($type$) valueHandle.get(ba, offset), i);
                offset += $BYTES$;
            }
            state.largestIndex = positions - 1;
            state.nonNulls = LongArrayState.deseralizeBitArray(state.bigArrays, ba, offset);
        }
    }
}
