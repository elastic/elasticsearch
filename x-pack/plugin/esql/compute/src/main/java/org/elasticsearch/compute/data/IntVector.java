/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Vector implementation that stores an array of integers.
 */
public final class IntVector extends AbstractVector {

    private final int[] values;

    public IntVector(int[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public int getInt(int position) {
        return values[position];
    }

    @Override
    public long getLong(int position) {
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    @Override
    public double getDouble(int position) {
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getInt(position);
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }
}
