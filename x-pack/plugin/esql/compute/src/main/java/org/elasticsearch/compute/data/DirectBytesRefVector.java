/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

/**
 * A {@link BytesRefVector} that wraps a flat bytes array and offsets directly
 */
public final class DirectBytesRefVector extends AbstractVector implements BytesRefVector {
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DirectBytesRefVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(BytesRefVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final byte[] bytes;
    private final int[] startOffsets;

    DirectBytesRefVector(byte[] bytes, int[] startOffsets, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.bytes = bytes;
        this.startOffsets = startOffsets;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        dest.bytes = bytes;
        dest.offset = startOffsets[position];
        dest.length = startOffsets[position + 1] - dest.offset;
        return dest;
    }

    @Override
    public PagedBytesCursor get(int position, PagedBytesCursor scratch) {
        final int startOffset = startOffsets[position];
        scratch.init(bytes, startOffset, startOffsets[position + 1] - startOffset);
        return scratch;
    }

    @Override
    public BytesRefBlock asBlock() {
        return new BytesRefVectorBlock(this);
    }

    @Override
    public OrdinalBytesRefVector asOrdinals() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BytesRefVector filter(boolean mayContainDuplicates, int... positions) {
        var scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(positions.length)) {
            for (int p : positions) {
                builder.appendBytesRef(getBytesRef(p, scratch));
            }
            return builder.build();
        }
    }

    @Override
    public BytesRefBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new BytesRefVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new BytesRefVectorBlock(this);
            }
            return (BytesRefBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        var scratch = new BytesRef();
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(getPositionCount())) {
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendBytesRef(getBytesRef(p, scratch));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BytesRefBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new BytesRefLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public BytesRefVector slice(int beginInclusive, int endExclusive) {
        if (beginInclusive == 0 && endExclusive == getPositionCount()) {
            incRef();
            return this;
        }
        var scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(endExclusive - beginInclusive)) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                builder.appendBytesRef(getBytesRef(i, scratch));
            }
            return builder.build();
        }
    }

    @Override
    public int valueMaxByteSize() {
        int max = 0;
        for (int i = 0; i < getPositionCount(); i++) {
            max = Math.max(max, startOffsets[i + 1] - startOffsets[i]);
        }
        return max;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bytes) + RamUsageEstimator.sizeOf(startOffsets);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefVector that) {
            return BytesRefVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ']';
    }
}
