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
 * Vector implementation that stores an array of float values.
 * This class is generated. Edit {@code X-ArrayVector.java.st} instead.
 */
final class FloatArrayVector extends AbstractVector implements FloatVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FloatArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(FloatVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final float[] values;

    FloatArrayVector(float[] values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
    }

    static FloatArrayVector readArrayVector(int positions, StreamInput in, BlockFactory blockFactory) throws IOException {
        final long preAdjustedBytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) positions * Float.BYTES;
        blockFactory.adjustBreaker(preAdjustedBytes);
        boolean success = false;
        try {
            float[] values = new float[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = in.readFloat();
            }
            final var block = new FloatArrayVector(values, positions, blockFactory);
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
            out.writeFloat(values[i]);
        }
    }

    @Override
    public FloatBlock asBlock() {
        return new FloatVectorBlock(this);
    }

    @Override
    public float getFloat(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public FloatVector filter(int... positions) {
        try (FloatVector.Builder builder = blockFactory().newFloatVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendFloat(values[pos]);
            }
            return builder.build();
        }
    }

    @Override
    public FloatBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new FloatVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new FloatVectorBlock(this);
            }
            return (FloatBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (FloatBlock.Builder builder = blockFactory().newFloatBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendFloat(getFloat(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<FloatBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new FloatLookup(asBlock(), positions, targetBlockSize);
    }

    public static long ramBytesEstimated(float[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FloatVector that) {
            return FloatVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return FloatVector.hash(this);
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
