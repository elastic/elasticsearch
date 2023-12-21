/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;

/**
 * Block implementation that stores values in a BooleanArray.
 * This class is generated. Do not edit it.
 */
public final class BooleanBigArrayBlock extends AbstractArrayBlock implements BooleanBlock {

    private static final long BASE_RAM_BYTES_USED = 0; // TODO: fix this
    private final BooleanBigArrayVector values;

    public BooleanBigArrayBlock(
        BitArray values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering, blockFactory);
        this.values = new BooleanBigArrayVector(values, (int) values.size(), blockFactory);
    }

    @Override
    public BooleanVector asVector() {
        return null;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return values.getBoolean(valueIndex);
    }

    @Override
    public BooleanBlock filter(int... positions) {
        try (var builder = blockFactory().newBooleanBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendBoolean(getBoolean(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendBoolean(getBoolean(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public BooleanBlock expand() {
        if (firstValueIndexes == null) {
            incRef();
            return this;
        }
        // TODO use reference counting to share the values
        try (var builder = blockFactory().newBooleanBlockBuilder(firstValueIndexes[getPositionCount()])) {
            for (int pos = 0; pos < getPositionCount(); pos++) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int first = getFirstValueIndex(pos);
                int end = first + getValueCount(pos);
                for (int i = first; i < end; i++) {
                    builder.appendBoolean(getBoolean(i));
                }
            }
            return builder.mvOrdering(MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING).build();
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanBlock that) {
            return BooleanBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", ramBytesUsed="
            + values.ramBytesUsed()
            + ']';
    }

    @Override
    public void closeInternal() {
        blockFactory().adjustBreaker(-ramBytesUsed() + RamUsageEstimator.sizeOf(values), true);
        Releasables.closeExpectNoException(values);
    }
}
