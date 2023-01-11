/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Vector implementation that stores an array of double values.
 */
public final class DoubleVector extends AbstractVector {

    private final double[] values;

    public DoubleVector(double[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public double getDouble(int position) {
        return values[position];
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }
}
