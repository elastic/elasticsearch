/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Vector implementation that stores a constant double value.
 * This class is generated. Do not edit it.
 */
public final class ConstantDoubleVector extends AbstractVector implements DoubleVector {

    static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantDoubleVector.class);

    private final double value;

    private final DoubleBlock block;

    public ConstantDoubleVector(double value, int positionCount) {
        this(value, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public ConstantDoubleVector(double value, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.value = value;
        this.block = new DoubleVectorBlock(this);
    }

    @Override
    public double getDouble(int position) {
        return value;
    }

    @Override
    public DoubleBlock asBlock() {
        return block;
    }

    @Override
    public DoubleVector filter(int... positions) {
        return new ConstantDoubleVector(value, positions.length);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
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
        if (obj instanceof DoubleVector that) {
            return DoubleVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleVector.hash(this);
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
