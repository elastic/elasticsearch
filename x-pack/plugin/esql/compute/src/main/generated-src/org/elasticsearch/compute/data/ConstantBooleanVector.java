/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Vector implementation that stores a constant boolean value.
 * This class is generated. Do not edit it.
 */
public final class ConstantBooleanVector extends AbstractVector implements BooleanVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantBooleanVector.class);

    private final boolean value;

    private final BooleanBlock block;

    public ConstantBooleanVector(boolean value, int positionCount) {
        this(value, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public ConstantBooleanVector(boolean value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
        this.block = new BooleanVectorBlock(this);
    }

    @Override
    public boolean getBoolean(int position) {
        return value;
    }

    @Override
    public BooleanBlock asBlock() {
        return block;
    }

    @Override
    public BooleanVector filter(int... positions) {
        return new ConstantBooleanVector(value, positions.length);
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

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed(), true);
    }
}
