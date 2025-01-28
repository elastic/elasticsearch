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
 * Vector implementation that stores a constant boolean value.
 * This class is generated. Do not edit it.
 */
final class ConstantBooleanVector extends AbstractVector implements BooleanVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantBooleanVector.class);

    private final boolean value;

    ConstantBooleanVector(boolean value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public boolean getBoolean(int position) {
        return value;
    }

    @Override
    public BooleanBlock asBlock() {
        return new BooleanVectorBlock(this);
    }

    @Override
    public BooleanVector filter(int... positions) {
        return blockFactory().newConstantBooleanVector(value, positions.length);
    }

    @Override
    public BooleanBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new BooleanVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new BooleanVectorBlock(this);
            }
            return (BooleanBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (BooleanBlock.Builder builder = blockFactory().newBooleanBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendBoolean(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<BooleanBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new BooleanLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single((BooleanBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantBooleanBlockWith(value, positions.getPositionCount()));
        }
        return new BooleanLookup(asBlock(), positions, targetBlockSize);
    }

    /**
     * Are all values {@code true}? This will scan all values to check and always answer accurately.
     */
    @Override
    public boolean allTrue() {
        return value;
    }

    /**
     * Are all values {@code false}? This will scan all values to check and always answer accurately.
     */
    @Override
    public boolean allFalse() {
        return value == false;
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
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
        if (obj instanceof BooleanVector that) {
            return BooleanVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanVector.hash(this);
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
