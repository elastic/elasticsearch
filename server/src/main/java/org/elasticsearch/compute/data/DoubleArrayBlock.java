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
public final class DoubleArrayBlock extends Block {

    private final double[] values;

    public DoubleArrayBlock(double[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    public DoubleArrayBlock(Number[] values, int positionCount) {
        super(positionCount);
        assert values.length == positionCount;
        this.values = new double[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (values[i] == null) {
                nullsMask.set(i);
                this.values[i] = nullValue();
            } else {
                this.values[i] = values[i].doubleValue();
            }
        }
    }

    public DoubleArrayBlock(double[] values, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.values = values;
        for (int i = nullsMask.nextSetBit(0); i >= 0; i = nullsMask.nextSetBit(i + 1)) {
            this.values[i] = nullValue();
        }
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
    public String toString() {
        return "DoubleArrayBlock{positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + '}';
    }

    private double nullValue() {
        return 0.0d;
    }
}
