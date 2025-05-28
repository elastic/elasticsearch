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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
// end generated imports

/**
 * Vector implementation that stores an array of long values.
 * This class is generated. Edit {@code X-ArrayVector.java.st} instead.
 */
final class LongArrayVector extends AbstractVector implements LongVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(LongVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final long[] values;

    LongArrayVector(long[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static LongArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        final long preAdjustedBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) positions * Long.BYTES;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            long[] values = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.readLong();
            }
            final var block = new LongArrayVector(values, positions, blockFactory);
            blockFactory.adjustBreaker(block.ramBytesUsed() - preAdjustedBytes);
            success = true;
            return block;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-preAdjustedBytes);
            }
        }
    }

    void writeArrayVector(int positions, StreamOutput out) throws IOException {
        for (int i = 0; i < positions; i++) {
            out.writeLong(values[i]);
        }
    }

    @Override
    public LongBlock asBlock() {
        return new LongVectorBlock(this);
    }

    @Override
    public long getLong(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public LongVector filter(int... positions) {
        try (LongVector.Builder builder = blockFactory().newLongVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendLong(values[pos]);
            }
            return builder.build();
        }
    }

    @Override
    public LongBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new LongVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new LongVectorBlock(this);
            }
            return (LongBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (LongBlock.Builder builder = blockFactory().newLongBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendLong(getLong(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new LongLookup(asBlock(), positions, targetBlockSize);
    }

    public static long ramBytesEstimated(long[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongVector that) {
            return LongVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongVector.hash(this);
    }

    @Override
    public String toString() {
        String valuesString = IntStream.range(0, getPositionCount())
            .limit(10)
            .mapToObj(n -> String.valueOf(values[n]))
            .collect(Collectors.joining(", ", "[", getPositionCount() > 10 ? ", ...]" : "]"));
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + valuesString + ']';
    }

}
