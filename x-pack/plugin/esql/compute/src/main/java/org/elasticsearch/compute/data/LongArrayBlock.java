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
public final class LongArrayBlock extends AbstractBlock implements LongBlock {

    private final long[] values;

    public LongArrayBlock(long[] values, int positionCount, int[] firstValueIndexes, BitSet nulls) {
        super(positionCount, firstValueIndexes, nulls);
        this.values = values;
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public long getLong(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return values[position];
    }

    @Override
    public Object getObject(int position) {
        return getLong(position);
    }

    @Override
    public LongBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public LongBlock filter(int... positions) {
        return new FilterLongBlock(this, positions);
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
