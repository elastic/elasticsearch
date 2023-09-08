/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores a constant double value.
 * This class is generated. Do not edit it.
 */
public final class ConstantDoubleVector extends AbstractVector implements DoubleVector {

    private final double value;

    public ConstantDoubleVector(double value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    @Override
    public double getDouble(int position) {
        return value;
    }

    @Override
    public DoubleBlock asBlock() {
        return new DoubleVectorBlock(this);
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
}
