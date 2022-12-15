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
public final class IntArrayBlock extends NullsAwareBlock {

    private final int[] values;

    public IntArrayBlock(int[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    public IntArrayBlock(int[] values, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.values = values;
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
}
