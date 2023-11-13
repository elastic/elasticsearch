/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Vector implementation that stores a constant int value.
 * This class is generated. Do not edit it.
 */
public final class ConstantIntVector extends AbstractVector implements IntVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantIntVector.class);

    private final int value;

    private final IntBlock block;

    public ConstantIntVector(int value, int positionCount) {
        this(value, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public ConstantIntVector(int value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
        this.block = new IntVectorBlock(this);
    }

    @Override
    public int getInt(int position) {
        return value;
    }

    @Override
    public IntBlock asBlock() {
        return block;
    }

    @Override
    public IntVector filter(int... positions) {
        return new ConstantIntVector(value, positions.length);
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

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    }
}
