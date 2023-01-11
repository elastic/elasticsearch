/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Block implementation that stores an array of long values.
 */
public final class LongBlock extends AbstractBlock {

    private final long[] values;

    public LongBlock(long[] values, int positionCount, int[] firstValueIndexes, BitSet nulls) {
        super(positionCount, firstValueIndexes, nulls);
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
    public ElementType elementType() {
        return ElementType.LONG;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }

    public long[] getRawLongArray() {
        assert nullValuesCount() == 0;
        return values;
    }
}
