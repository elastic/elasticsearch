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
 * Block implementation that stores an array of integers.
 */
public final class IntArrayBlock extends Block {

    private final int[] values;

    public IntArrayBlock(int[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    public IntArrayBlock(Number[] values, int positionCount) {
        super(positionCount);
        assert values.length == positionCount;
        this.values = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (values[i] == null) {
                nullsMask.set(i);
                this.values[i] = nullValue();
            } else {
                this.values[i] = values[i].intValue();
            }
        }
    }

    public IntArrayBlock(int[] values, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.values = values;
        for (int i = nullsMask.nextSetBit(0); i >= 0; i = nullsMask.nextSetBit(i + 1)) {
            this.values[i] = nullValue();
        }
    }

    @Override
    public int getInt(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return values[position];
    }

    @Override
    public long getLong(int position) {
        assert assertPosition(position);
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    @Override
    public double getDouble(int position) {
        assert assertPosition(position);
        return getInt(position);  // Widening primitive conversions, no loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getInt(position);
    }

    @Override
    public String toString() {
        return "IntArrayBlock{positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + '}';
    }

    private int nullValue() {
        return 0;
    }
}
