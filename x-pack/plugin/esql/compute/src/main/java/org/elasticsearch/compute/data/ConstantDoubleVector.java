/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores a constant double value.
 */
final class ConstantDoubleVector extends AbstractVector {

    private final double value;

    ConstantDoubleVector(double value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    @Override
    public double getDouble(int position) {
        return value;
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    public Vector filter(int... positions) {
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
    public String toString() {
        return "ConstantDoubleVector[positions=" + getPositionCount() + ", value=" + value + "]";
    }
}
