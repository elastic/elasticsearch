/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Nullable;

import java.util.BitSet;

abstract class AbstractArrayBlock extends AbstractBlock {

    private final MvOrdering mvOrdering;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractArrayBlock(int positionCount, MvOrdering mvOrdering) {
        super(positionCount);
        this.mvOrdering = mvOrdering;
    }

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractArrayBlock(int positionCount, @Nullable int[] firstValueIndexes, @Nullable BitSet nullsMask, MvOrdering mvOrdering) {
        super(positionCount, firstValueIndexes, nullsMask);
        this.mvOrdering = mvOrdering;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        /*
         * This could return a false positive if all the indices are one away from
         * each other. But we will try to avoid that.
         */
        return firstValueIndexes != null;
    }

    @Override
    public final MvOrdering mvOrdering() {
        return mvOrdering;
    }

    protected BitSet shiftNullsToExpandedPositions() {
        BitSet expanded = new BitSet(getTotalValueCount());
        int next = -1;
        while ((next = nullsMask.nextSetBit(next + 1)) != -1) {
            expanded.set(getFirstValueIndex(next));
        }
        return expanded;
    }
}
