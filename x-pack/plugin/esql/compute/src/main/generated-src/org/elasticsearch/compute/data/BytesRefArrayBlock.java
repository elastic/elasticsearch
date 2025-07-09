/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.BitSet;
// end generated imports

/**
 * Block implementation that stores values in a {@link BytesRefArrayVector}.
 * Does not take ownership of the given {@link BytesRefArray} and does not adjust circuit breakers to account for it.
 * This class is generated. Edit {@code X-ArrayBlock.java.st} instead.
 */
public final class BytesRefArrayBlock extends AbstractArrayBlock implements BytesRefBlock {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefArrayBlock.class);

    private final BytesRefArrayVector vector;

    BytesRefArrayBlock(
        BytesRefArray values,
        int positionCount,
        int[] firstValueIndexes,
        BitSet nulls,
        MvOrdering mvOrdering,
        BlockFactory blockFactory
    ) {
        this(
            new BytesRefArrayVector(values, firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount], blockFactory),
            positionCount,
            firstValueIndexes,
            nulls,
            mvOrdering
        );
    }

    private BytesRefArrayBlock(
        BytesRefArrayVector vector, // stylecheck
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

    static BytesRefArrayBlock readArrayBlock(BlockFactory blockFactory, BlockStreamInput in) throws IOException {
        final SubFields sub = new SubFields(blockFactory, in);
        BytesRefArrayVector vector = null;
        boolean success = false;
        try {
            vector = BytesRefArrayVector.readArrayVector(sub.vectorPositions(), in, blockFactory);
            var block = new BytesRefArrayBlock(vector, sub.positionCount, sub.firstValueIndexes, sub.nullsMask, sub.mvOrdering);
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
    public BytesRefVector asVector() {
        return null;
    }

    @Override
    public OrdinalBytesRefBlock asOrdinals() {
        return null;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return vector.getBytesRef(valueIndex, dest);
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        final BytesRef scratch = new BytesRef();
        try (var builder = blockFactory().newBytesRefBlockBuilder(positions.length)) {
            for (int pos : positions) {
                if (isNull(pos)) {
                    builder.appendNull();
                    continue;
                }
                int valueCount = getValueCount(pos);
                int first = getFirstValueIndex(pos);
                if (valueCount == 1) {
                    builder.appendBytesRef(getBytesRef(first, scratch));
                } else {
                    builder.beginPositionEntry();
                    for (int c = 0; c < valueCount; c++) {
                        builder.appendBytesRef(getBytesRef(first + c, scratch));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.mvOrdering(mvOrdering()).build();
        }
    }

    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return this;
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return this;
            }
            return (BytesRefBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        BytesRef scratch = new BytesRef();
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(getPositionCount())) {
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
                    builder.appendBytesRef(getBytesRef(start, scratch));
                    continue;
                }
                int end = start + valueCount;
                builder.beginPositionEntry();
                for (int i = start; i < end; i++) {
                    builder.appendBytesRef(getBytesRef(i, scratch));
                }
                builder.endPositionEntry();
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefLookup(this, positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public BytesRefBlock expand() {
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

        BytesRefArrayBlock expanded = new BytesRefArrayBlock(
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
        return ramBytesUsedOnlyBlock() + vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefBlock that) {
            return BytesRefBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", vector="
            + vector
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
