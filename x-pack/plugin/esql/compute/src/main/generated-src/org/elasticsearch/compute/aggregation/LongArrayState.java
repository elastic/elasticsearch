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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of longs.
 * This class is generated. Do not edit it.
 */
@Experimental
final class LongArrayState implements AggregatorState<LongArrayState> {
    private final BigArrays bigArrays;
    private final long init;

    private LongArray values;
    /**
     * Total number of groups {@code <=} values.length.
     */
    private int largestIndex;
    private BitArray nonNulls;

    LongArrayState(BigArrays bigArrays, long init) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newLongArray(1, false);
        this.values.set(0, init);
        this.init = init;
    }

    long get(int index) {
        return values.get(index);
    }

    long getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : init;
    }

    void set(long value, int index) {
        if (index > largestIndex) {
            ensureCapacity(index);
            largestIndex = index;
        }
        values.set(index, value);
        if (nonNulls != null) {
            nonNulls.set(index);
        }
    }

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
            LongVector.Builder builder = LongVector.newVectorBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                builder.appendLong(values.get(selected.getInt(i)));
            }
            return builder.build().asBlock();
        }
        LongBlock.Builder builder = LongBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            if (hasValue(group)) {
                builder.appendLong(values.get(group));
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

    /** Extracts an intermediate view of the contents of this state.  */
    void toIntermediate(Block[] blocks, int offset, IntVector selected) {
        assert blocks.length >= offset + 2;
        var valuesBuilder = LongBlock.newBlockBuilder(selected.getPositionCount());
        var nullsBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            valuesBuilder.appendLong(values.get(group));
            nullsBuilder.appendBoolean(hasValue(group));
        }
        blocks[offset + 0] = valuesBuilder.build();
        blocks[offset + 1] = nullsBuilder.build();
    }

    @Override
    public long getEstimatedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Releasables.close(values, nonNulls);
    }

    @Override
    public AggregatorStateSerializer<LongArrayState> serializer() {
        throw new UnsupportedOperationException();
    }
}
