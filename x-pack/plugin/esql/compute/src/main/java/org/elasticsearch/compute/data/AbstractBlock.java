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
    private int references = 1;
    private boolean released = false;
    private final int positionCount;

    @Nullable
    protected final int[] firstValueIndexes;

    @Nullable
    protected final BitSet nullsMask;

    protected final BlockFactory blockFactory;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractBlock(int positionCount, BlockFactory blockFactory) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
        this.firstValueIndexes = null;
        this.nullsMask = null;
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
    public boolean isReleased() {
        return released;
    }

    @Override
    public final void incRef() {
        if (references <= 0) {
            throw new IllegalStateException("can't increase refCount on already released block [" + this + "]");
        }
        references++;
    }

    @Override
    public final boolean tryIncRef() {
        if (references <= 0) {
            return false;
        }
        references++;
        return true;
    }

    @Override
    public final boolean decRef() {
        if (references <= 0) {
            throw new IllegalStateException("can't release already released block [" + this + "]");
        }

        references--;

        if (references <= 0) {
            close();
            return true;
        }
        return false;
    }

    @Override
    public final boolean hasReferences() {
        return references >= 1;
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released block [" + this + "]");
        }
        if (references > 1) {
            throw new IllegalStateException("can't close block that is still referenced elsewhere [" + this + "]");
        }
        released = true;
        // In case that there is only 1 reference, allow closing without having to call decRef.
        references = 0;
    }
}
