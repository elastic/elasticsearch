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
 * Vector implementation that stores a constant float value.
 * This class is generated. Do not edit it.
 */
final class ConstantFloatVector extends AbstractVector implements FloatVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantFloatVector.class);

    private final float value;

    ConstantFloatVector(float value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public float getFloat(int position) {
        return value;
    }

    @Override
    public FloatBlock asBlock() {
        return new FloatVectorBlock(this);
    }

    @Override
    public FloatVector filter(int... positions) {
        return blockFactory().newConstantFloatVector(value, positions.length);
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
                    builder.appendFloat(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<FloatBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new FloatLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single((FloatBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantFloatBlockWith(value, positions.getPositionCount()));
        }
        return new FloatLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.FLOAT;
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
        if (obj instanceof FloatVector that) {
            return FloatVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return FloatVector.hash(this);
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
