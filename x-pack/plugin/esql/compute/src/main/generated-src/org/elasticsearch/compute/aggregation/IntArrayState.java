/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of ints.
 * This class is generated. Do not edit it.
 */
@Experimental
final class IntArrayState implements AggregatorState<IntArrayState> {
    private final BigArrays bigArrays;
    private final int init;

    private IntArray values;
    /**
     * Total number of groups {@code <=} values.length.
     */
    private int largestIndex;
    private BitArray nonNulls;

    IntArrayState(BigArrays bigArrays, int init) {
        this.bigArrays = bigArrays;
        this.values = bigArrays.newIntArray(1, false);
        this.values.set(0, init);
        this.init = init;
    }

    int get(int index) {
        return values.get(index);
    }

    int getOrDefault(int index) {
        return index <= largestIndex ? values.get(index) : init;
    }

    void set(int value, int index) {
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
            IntVector.Builder builder = IntVector.newVectorBuilder(selected.getPositionCount());
            for (int i = 0; i < selected.getPositionCount(); i++) {
                builder.appendInt(values.get(selected.getInt(i)));
            }
            return builder.build().asBlock();
        }
        IntBlock.Builder builder = IntBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            if (hasValue(group)) {
                builder.appendInt(values.get(group));
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
        var valuesBuilder = IntBlock.newBlockBuilder(selected.getPositionCount());
        var nullsBuilder = BooleanBlock.newBlockBuilder(selected.getPositionCount());
        for (int i = 0; i < selected.getPositionCount(); i++) {
            int group = selected.getInt(i);
            valuesBuilder.appendInt(values.get(group));
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
    public AggregatorStateSerializer<IntArrayState> serializer() {
        throw new UnsupportedOperationException();
    }
}
