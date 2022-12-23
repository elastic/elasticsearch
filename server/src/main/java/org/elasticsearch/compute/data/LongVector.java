/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Vector implementation that stores an array of long values.
 */
public final class LongVector extends AbstractVector {

    private final long[] values;

    public LongVector(long[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public long getLong(int position) {
        return values[position];
    }

    @Override
    public double getDouble(int position) {
        return getLong(position);  // Widening primitive conversions, possible loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public Vector filter(int... positions) {
        return null; // new FilteredBlock(this, positions); TODO
    }

    @Override
    public Class<?> elementType() {
        return long.class;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

    public long[] getRawLongArray() {
        return values;
    }
}
