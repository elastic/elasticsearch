/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Vector implementation that stores a constant int value.
 * This class is generated. Do not edit it.
 */
final class ConstantIntVector extends AbstractVector implements IntVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantIntVector.class);

    private final int value;

    ConstantIntVector(int value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public int getInt(int position) {
        return value;
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public IntVector filter(int... positions) {
        return blockFactory().newConstantIntVector(value, positions.length);
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
                    builder.appendInt(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new IntLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single((IntBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantIntBlockWith(value, positions.getPositionCount()));
        }
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    /**
     * The minimum value in the block.
     */
    @Override
    public int min() {
        return value;
    }

    /**
     * The maximum value in the block.
     */
    @Override
    public int max() {
        return value;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
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

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
