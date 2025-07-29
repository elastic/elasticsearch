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
 * Vector implementation that stores an array of int values.
 * This class is generated. Edit {@code X-ArrayVector.java.st} instead.
 */
final class IntArrayVector extends AbstractVector implements IntVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(IntVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final int[] values;

    /**
     * The minimum value in the block.
     */
    private Integer min;

    /**
     * The minimum value in the block.
     */
    private Integer max;

    IntArrayVector(int[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static IntArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        final long preAdjustedBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) positions * Integer.BYTES;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            int[] values = new int[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.readInt();
            }
            final var block = new IntArrayVector(values, positions, blockFactory);
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
            out.writeInt(values[i]);
        }
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public int getInt(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public IntVector filter(int... positions) {
        try (IntVector.Builder builder = blockFactory().newIntVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendInt(values[pos]);
            }
            return builder.build();
        }
    }

    @Override
    public IntBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new IntVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new IntVectorBlock(this);
            }
            return (IntBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (IntBlock.Builder builder = blockFactory().newIntBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendInt(getInt(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    public static long ramBytesEstimated(int[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        if (min == null) {
            int v = Integer.MAX_VALUE;
            for (int i = 0; i < getPositionCount(); i++) {
                v = Math.min(v, values[i]);
            }
            min = v;
        }
        return min;
    }

    /**
     * The maximum value in the block.
     */
    @Override
    public int max() {
        if (max == null) {
            int v = Integer.MIN_VALUE;
            for (int i = 0; i < getPositionCount(); i++) {
                v = Math.max(v, values[i]);
            }
            max = v;
        }
        return max;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntVector that) {
            return IntVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntVector.hash(this);
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
