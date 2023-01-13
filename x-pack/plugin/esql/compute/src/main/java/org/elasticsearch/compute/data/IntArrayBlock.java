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
 * Block implementation that stores an array of integers.
 */
public final class IntArrayBlock extends AbstractBlock implements IntBlock {

    private final int[] values;

    public IntArrayBlock(int[] values, int positionCount, int[] firstValueIndexes, BitSet nulls) {
        super(positionCount, firstValueIndexes, nulls);
        this.values = values;
    }

    @Override
    public IntVector asVector() {
        return null;
    }

    @Override
    public int getInt(int position) {
        assert assertPosition(position);
        assert isNull(position) == false;
        return values[position];
    }

    @Override
    public Object getObject(int position) {
        return getInt(position);
    }

    @Override
    public IntBlock getRow(int position) {
        return filter(position);
    }

    @Override
    public IntBlock filter(int... positions) {
        return new FilterIntBlock(this, positions);
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public LongBlock asLongBlock() {  // copy rather than view, for now
        final int positions = getPositionCount();
        long[] longValues = new long[positions];
        for (int i = 0; i < positions; i++) {
            longValues[i] = values[i];
        }
        return new LongArrayBlock(longValues, getPositionCount(), firstValueIndexes, nullsMask);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }
}
