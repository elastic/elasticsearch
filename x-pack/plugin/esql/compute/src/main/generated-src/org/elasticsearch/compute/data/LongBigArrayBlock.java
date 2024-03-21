/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.BitSet;

/**
 * Block implementation that stores values in a {@link LongBigArrayVector}. Does not take ownership of the given
 * {@link LongArray} and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class LongBigArrayBlock extends AbstractArrayBlock implements LongBlock {

    private static final long BASE_RAM_BYTES_USED = 0; // TODO: fix this
    private final LongBigArrayVector vector;

    public LongBigArrayBlock(
        LongArray values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        this(
            new LongBigArrayVector(values, firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount], blockFactory),
            positionCount,
            firstValueIndexes,
            nulls,
            mvOrdering
        );
    }

    private LongBigArrayBlock(
        LongBigArrayVector vector, // stylecheck
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering
    ) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering);
        this.vector = vector;
        assert firstValueIndexes == null
            ? vector.getPositionCount() == getPositionCount()
            : firstValueIndexes[getPositionCount()] == vector.getPositionCount();
    }

    static LongBigArrayBlock readArrayBlock(BlockFactory blockFactory, BlockStreamInput in) throws IOException {
        final SubFields sub = new SubFields(blockFactory, in);
        LongBigArrayVector vector = null;
        boolean success = false;
        try {
            vector = LongBigArrayVector.readArrayVector(sub.vectorPositions(), in, blockFactory);
            var block = new LongBigArrayBlock(vector, sub.positionCount, sub.firstValueIndexes, sub.nullsMask, sub.mvOrdering);
            blockFactory.adjustBreaker(block.ramBytesUsed() - vector.ramBytesUsed() - sub.bytesReserved);
            success = true;
            return block;
        } finally {
            if (success == false) {
                Releasables.close(vector);
                blockFactory.adjustBreaker(-sub.bytesReserved);
            }
        }
    }

    void writeArrayBlock(StreamOutput out) throws IOException {
        writeSubFields(out);
        vector.writeArrayVector(vector.getPositionCount(), out);
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int valueIndex) {
        return vector.getLong(valueIndex);
    }

    @Override
    public LongBlock filter(int... positions) {
        try (var builder = blockFactory().newLongBlockBuilder(positions.length)) {
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
            incRef();
            return this;
        }
        if (nullsMask == null) {
            vector.incRef();
            return vector.asBlock();
        }

        // The following line is correct because positions with multi-values are never null.
        int expandedPositionCount = vector.getPositionCount();
        long bitSetRamUsedEstimate = Math.max(nullsMask.size(), BlockRamUsageEstimator.sizeOfBitSet(expandedPositionCount));
        blockFactory().adjustBreaker(bitSetRamUsedEstimate);

        LongBigArrayBlock expanded = new LongBigArrayBlock(
            vector,
            expandedPositionCount,
            null,
            shiftNullsToExpandedPositions(),
            MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
        blockFactory().adjustBreaker(expanded.ramBytesUsedOnlyBlock() - bitSetRamUsedEstimate);
        // We need to incRef after adjusting any breakers, otherwise we might leak the vector if the breaker trips.
        vector.incRef();
        return expanded;
    }

    private long ramBytesUsedOnlyBlock() {
        return BASE_RAM_BYTES_USED + BlockRamUsageEstimator.sizeOf(firstValueIndexes) + BlockRamUsageEstimator.sizeOfBitSet(nullsMask);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsedOnlyBlock() + RamUsageEstimator.sizeOf(vector);
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
            + ", ramBytesUsed="
            + vector.ramBytesUsed()
            + ']';
    }

    @Override
    public void allowPassingToDifferentDriver() {
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }

    @Override
    public void closeInternal() {
        blockFactory().adjustBreaker(-ramBytesUsedOnlyBlock());
        Releasables.closeExpectNoException(vector);
    }
}
