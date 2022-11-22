/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Block implementation that stores an array of double values.
 */
public final class DoubleArrayBlock extends NumberArrayBlock {

    private final double[] values;

    public DoubleArrayBlock(double[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    public DoubleArrayBlock(Number[] values, int positionCount) {
        super(values, positionCount);
        this.values = new double[positionCount];
        for (int i = 0; i < positionCount; i++) {
            this.values[i] = internalNumberValues[i].doubleValue();
        }
    }

    public DoubleArrayBlock(double[] values, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.values = values;
    }

    @Override
    public double getDouble(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return values[position];
    }

    @Override
    public Object getObject(int position) {
        return getDouble(position);
    }

    @Override
    Number nullValue() {
        return 0.0d;
    }

    @Override
    public String toString() {
        return "DoubleArrayBlock{positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + '}';
    }
}
