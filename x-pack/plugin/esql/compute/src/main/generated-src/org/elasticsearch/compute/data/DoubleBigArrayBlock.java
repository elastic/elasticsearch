/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.BitSet;

/**
 * Block implementation that stores values in a {@link DoubleBigArrayVector}. Does not take ownership of the given
 * {@link DoubleArray} and does not adjust circuit breakers to account for it.
 * This class is generated. Do not edit it.
 */
public final class DoubleBigArrayBlock extends AbstractArrayBlock implements DoubleBlock {

    private static final long BASE_RAM_BYTES_USED = 0; // TODO: fix this
    private final DoubleBigArrayVector vector;

    public DoubleBigArrayBlock(
        DoubleArray values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        this(
            new DoubleBigArrayVector(values, firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount], blockFactory),
            positionCount,
            firstValueIndexes,
            nulls,
            mvOrdering
        );
    }

    private DoubleBigArrayBlock(
        DoubleBigArrayVector vector, // stylecheck
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

    static DoubleBigArrayBlock readArrayBlock(BlockFactory blockFactory, BlockStreamInput in) throws IOException {
        final SubFields sub = new SubFields(blockFactory, in);
        DoubleBigArrayVector vector = null;
        boolean success = false;
        try {
            vector = DoubleBigArrayVector.readArrayVector(sub.vectorPositions(), in, blockFactory);
            var block = new DoubleBigArrayBlock(vector, sub.positionCount, sub.firstValueIndexes, sub.nullsMask, sub.mvOrdering);
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
    public DoubleVector asVector() {
        return null;
    }

    @Override
    public double getDouble(int valueIndex) {
        return vector.getDouble(valueIndex);
    }

    @Override
    public DoubleBlock filter(int... positions) {
        try (var builder = blockFactory().newDoubleBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendDouble(getDouble(getFirstValueIndex(pos)));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendDouble(getDouble(first + c));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public DoubleBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return this;
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return this;
            }
            return (DoubleBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (DoubleBlock.Builder builder = blockFactory().newDoubleBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (false == mask.getBoolean(p)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int start = getFirstValueIndex(p);
                if (valueCount == 1) {
                    builder.appendDouble(getDouble(start));
                    continue;
                }
                int end = start + valueCount;
                builder.beginPositionEntry();
                for (int i = start; i < end; i++) {
                    builder.appendDouble(getDouble(i));
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new DoubleLookup(this, positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public DoubleBlock expand() {
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

        DoubleBigArrayBlock expanded = new DoubleBigArrayBlock(
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
        if (obj instanceof DoubleBlock that) {
            return DoubleBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleBlock.hash(this);
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
