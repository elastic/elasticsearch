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
 * Block implementation that stores an array of long.
 * This class is generated. Do not edit it.
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
    public long getLong(int valueIndex) {
        return values[valueIndex];
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
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + Arrays.toString(values) + ']';
    }
}
