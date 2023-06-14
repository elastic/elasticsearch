/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Nullable;

import java.util.BitSet;

abstract class AbstractBlock implements Block {

    private final int positionCount;

    @Nullable
    protected final int[] firstValueIndexes;

    @Nullable
    protected final BitSet nullsMask;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractBlock(int positionCount) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.firstValueIndexes = null;
        this.nullsMask = null;
    }

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractBlock(int positionCount, @Nullable int[] firstValueIndexes, @Nullable BitSet nullsMask) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.firstValueIndexes = firstValueIndexes;
        this.nullsMask = nullsMask == null || nullsMask.isEmpty() ? null : nullsMask;
        assert (firstValueIndexes == null && this.nullsMask == null) == false;
    }

    @Override
    public int getTotalValueCount() {
        if (firstValueIndexes == null) {
            return positionCount - nullValuesCount();
        }
        return firstValueIndexes[positionCount] - nullValuesCount();
    }

    @Override
    public final int getPositionCount() {
        return positionCount;
    }

    /** Gets the index of the first value for the given position. */
    public int getFirstValueIndex(int position) {
        return firstValueIndexes == null ? position : firstValueIndexes[position];
    }

    /** Gets the number of values for the given position, possibly 0. */
    @Override
    public int getValueCount(int position) {
        return isNull(position) ? 0 : firstValueIndexes == null ? 1 : firstValueIndexes[position + 1] - firstValueIndexes[position];
    }

    @Override
    public boolean isNull(int position) {
        return mayHaveNulls() && nullsMask.get(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return nullsMask != null;
    }

    @Override
    public int nullValuesCount() {
        return mayHaveNulls() ? nullsMask.cardinality() : 0;
    }

    @Override
    public boolean areAllValuesNull() {
        return nullValuesCount() == getPositionCount();
    }
}
