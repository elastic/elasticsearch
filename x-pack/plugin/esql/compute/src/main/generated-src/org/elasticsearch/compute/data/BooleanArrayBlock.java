/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;

/**
 * Block implementation that stores an array of boolean.
 * This class is generated. Do not edit it.
 */
public final class BooleanArrayBlock extends AbstractArrayBlock implements BooleanBlock {

    private final boolean[] values;

    public BooleanArrayBlock(boolean[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        super(positionCount, firstValueIndexes, nulls, mvOrdering);
        this.values = values;
    }

    @Override
    public BooleanVector asVector() {
        return null;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return values[valueIndex];
    }

    @Override
    public BooleanBlock filter(int... positions) {
        return new FilterBooleanBlock(this, positions);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    public BooleanBlock expand() {
        if (firstValueIndexes == null) {
            return this;
        }
        int end = firstValueIndexes[getPositionCount()];
        if (nullsMask == null) {
            return new BooleanArrayVector(values, end).asBlock();
        }
        int[] firstValues = IntStream.range(0, end + 1).toArray();
        return new BooleanArrayBlock(values, end, firstValues, shiftNullsToExpandedPositions(), MvOrdering.UNORDERED);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanBlock that) {
            return BooleanBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[positions="
            + getPositionCount()
            + ", mvOrdering="
            + mvOrdering()
            + ", values="
            + Arrays.toString(values)
            + ']';
    }
}
