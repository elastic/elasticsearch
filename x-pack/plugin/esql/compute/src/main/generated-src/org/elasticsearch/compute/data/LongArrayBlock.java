/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;

/**
 * Block implementation that stores an array of long.
 * This class is generated. Do not edit it.
 */
public final class LongArrayBlock extends AbstractArrayBlock implements LongBlock {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongArrayBlock.class);

    private final long[] values;

    public LongArrayBlock(long[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        this(values, positionCount, firstValueIndexes, nulls, mvOrdering, BlockFactory.getNonBreakingInstance());
    }

    public LongArrayBlock(
        long[] values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering, blockFactory);
        this.values = values;
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int valueIndex) {
        return values[valueIndex];
    }

    @Override
    public LongBlock filter(int... positions) {
        try (var builder = blockFactory.newLongBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendLong(getLong(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendLong(getLong(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public LongBlock expand() {
        if (firstValueIndexes == null) {
            return this;
        }
        int end = firstValueIndexes[getPositionCount()];
        if (nullsMask == null) {
            return new LongArrayVector(values, end).asBlock();
        }
        int[] firstValues = IntStream.range(0, end + 1).toArray();
        return new LongArrayBlock(values, end, firstValues, shiftNullsToExpandedPositions(), MvOrdering.UNORDERED, blockFactory);
    }

    public static long ramBytesEstimated(long[] values, int[] firstValueIndexes, BitSet nullsMask) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values) + BlockRamUsageEstimator.sizeOf(firstValueIndexes)
            + BlockRamUsageEstimator.sizeOfBitSet(nullsMask) + RamUsageEstimator.shallowSizeOfInstance(MvOrdering.class);
        // TODO mvordering is shared
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values, firstValueIndexes, nullsMask);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", values="
            + Arrays.toString(values)
            + ']';
    }

    @Override
    public void close() {
        super.close();
        if (hasReferences() == false) {
            blockFactory.adjustBreaker(-ramBytesUsed(), true);
        }
    }
}
