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

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ConstantIntVector.class);

    private final int value;

    public ConstantIntVector(int value, int positionCount) {
        super(positionCount);
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
        return BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOfInstance(int.class);
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
        // no-op
    }
}
