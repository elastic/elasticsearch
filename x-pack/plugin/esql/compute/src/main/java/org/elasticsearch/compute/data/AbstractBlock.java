/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Nullable;

import java.util.BitSet;

abstract class AbstractBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final int positionCount;

    @Nullable
    protected final int[] firstValueIndexes;

    @Nullable
    protected final BitSet nullsMask;

    private BlockFactory blockFactory;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractBlock(int positionCount, BlockFactory blockFactory) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
        this.firstValueIndexes = null;
        this.nullsMask = null;
        assert assertInvariants();
    }

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractBlock(int positionCount, @Nullable int[] firstValueIndexes, @Nullable BitSet nullsMask, BlockFactory blockFactory) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
        this.firstValueIndexes = firstValueIndexes;
        this.nullsMask = nullsMask == null || nullsMask.isEmpty() ? null : nullsMask;
        assert nullsMask != null || firstValueIndexes != null : "Create VectorBlock instead";
        assert assertInvariants();
    }

    private boolean assertInvariants() {
        if (firstValueIndexes != null) {
            assert firstValueIndexes.length == getPositionCount() + 1;
            for (int i = 0; i < getPositionCount(); i++) {
                assert (firstValueIndexes[i + 1] - firstValueIndexes[i]) >= 0;
            }
        }
        if (nullsMask != null) {
            assert nullsMask.nextSetBit(getPositionCount() + 1) == -1;
        }
        if (firstValueIndexes != null && nullsMask != null) {
            for (int i = 0; i < getPositionCount(); i++) {
                // Either we have multi-values or a null but never both.
                assert ((nullsMask.get(i) == false) || (firstValueIndexes[i + 1] - firstValueIndexes[i]) == 1);
            }
        }
        return true;
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

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        blockFactory = blockFactory.parent();
    }

    @Override
    public final boolean isReleased() {
        return hasReferences() == false;
    }
}
