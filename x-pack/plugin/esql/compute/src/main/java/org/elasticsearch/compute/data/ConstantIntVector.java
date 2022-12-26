/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Vector implementation that stores a constant integer value.
 */
public final class ConstantIntVector extends AbstractVector {

    private final int value;

    public ConstantIntVector(int value, int positionCount) {
        super(positionCount);
        this.value = value;
    }

    public int getInt(int position) {
        return value;
    }

    public long getLong(int position) {
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    public double getDouble(int position) {
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    public Object getObject(int position) {
        return getInt(position);
    }

    @Override
    public Vector filter(int... positions) {
        return new ConstantIntVector(value, positions.length);
    }

    @Override
    public Class<?> elementType() {
        return int.class;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public String toString() {
        return "ConstantIntVector[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
