/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    public Class<?> elementType() {
        return long.class;
    }

    @Override
    public String toString() {
        return "ConstantLongVector[positions=" + getPositionCount() + ", value=" + value + ']';
    }
}
