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
     * Constructor for SingletonBlock
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
            return positionCount;
        } else {
            return getFirstValueIndex(positionCount - 1) + getValueCount(positionCount - 1);  // TODO: verify this
        }
    }

    @Override
    public final int getPositionCount() {
        return positionCount; // TODO remove? firstValueIndexes.length - 1;
    }

    /** Gets the index of the first value for the given position. */
    public int getFirstValueIndex(int position) {
        return firstValueIndexes == null ? position : firstValueIndexes[position];
    }

    /** Gets the number of values for the given position, possibly 0. */
    public int getValueCount(int position) {
        return firstValueIndexes == null ? 1 :

        // if (position == positionCount - 1) {
        // return positionCount - firstValueIndexes[position] - 1;
        // } else {
            firstValueIndexes[position + 1] - firstValueIndexes[position]; // TODO: check for overflow
        // }
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

    @Override
    public int validPositionCount() {
        return positionCount - nullValuesCount();
    }

    protected final boolean assertPosition(int position) {
        assert (position >= 0 || position < getPositionCount())
            : "illegal position, " + position + ", position count:" + getPositionCount();
        return true;
    }
}
