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
 * Vector implementation that stores a constant long value.
 * This class is generated. Do not edit it.
 */
final class ConstantLongVector extends AbstractVector implements LongVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantLongVector.class);

    private final long value;

    ConstantLongVector(long value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
    }

    @Override
    public long getLong(int position) {
        return value;
    }

    @Override
    public LongBlock asBlock() {
        return new LongVectorBlock(this);
    }

    @Override
    public LongVector filter(int... positions) {
        return blockFactory().newConstantLongVector(value, positions.length);
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
                    builder.appendLong(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        if (positions.getPositionCount() == 0) {
            return ReleasableIterator.empty();
        }
        IntVector positionsVector = positions.asVector();
        if (positionsVector == null) {
            return new LongLookup(asBlock(), positions, targetBlockSize);
        }
        int min = positionsVector.min();
        if (min < 0) {
            throw new IllegalArgumentException("invalid position [" + min + "]");
        }
        if (min > getPositionCount()) {
            return ReleasableIterator.single((LongBlock) positions.blockFactory().newConstantNullBlock(positions.getPositionCount()));
        }
        if (positionsVector.max() < getPositionCount()) {
            return ReleasableIterator.single(positions.blockFactory().newConstantLongBlockWith(value, positions.getPositionCount()));
        }
        return new LongLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
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
        if (obj instanceof LongVector that) {
            return LongVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongVector.hash(this);
    }

    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
