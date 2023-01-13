/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Vector implementation that stores an array of long values.
 */
public final class LongArrayVector extends AbstractVector implements LongVector {

    private final long[] values;

    public LongArrayVector(long[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    @Override
    public LongBlock asBlock() {
        return new LongVectorBlock(this);
    }

    @Override
    public long getLong(int position) {
        return values[position];
    }

    @Override
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public LongVector filter(int... positions) {
        return new FilterLongVector(this, positions);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }
}
