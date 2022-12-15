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
 * Block implementation that stores an array of long values.
 */
public final class LongArrayBlock extends NullsAwareBlock {

    private final long[] values;

    public LongArrayBlock(long[] values, int positionCount) {
        super(positionCount);
        this.values = values;
    }

    public LongArrayBlock(long[] values, int positionCount, BitSet nulls) {
        super(positionCount, nulls);
        this.values = values;
    }

    @Override
    public long getLong(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return values[position];
    }

    @Override
    public double getDouble(int position) {
        assert assertPosition(position);
        return getLong(position);  // Widening primitive conversions, possible loss of precision
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public String toString() {
        return "LongArrayBlock{positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + '}';
    }

    public long[] getRawLongArray() {
        assert nullValuesCount() == 0;
        return values;
    }
}
