/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores a constant long value.
 */
final class ConstantLongVector extends AbstractVector {

    private final long value;

    ConstantLongVector(long value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    @Override
    public long getLong(int position) {
        return value;
    }

    @Override
    public double getDouble(int position) {
        return value;  // Widening primitive conversions, no loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public Vector filter(int... positions) {
        return new ConstantLongVector(value, positions.length);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public String toString() {
        return "ConstantLongVector[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
