/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Nullable;

abstract class AbstractMultiValuedBlock extends AbstractNonThreadSafeRefCounted implements Block {

    protected final int positionCount;
    @Nullable
    protected final int[] firstValueIndexes;

    /**
     * @param positionCount the number of values in this block
     */
    protected AbstractMultiValuedBlock(int positionCount, @Nullable int[] firstValueIndexes) {
        this.positionCount = positionCount;
        this.firstValueIndexes = firstValueIndexes;
    }

    @Override
    public final boolean mayHaveMultivaluedFields() {
        /*
         * This could return a false positive if all the indices are one away from
         * each other. But we will try to avoid that.
         */
        return firstValueIndexes != null;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        if (false == mayHaveMultivaluedFields()) {
            return false;
        }
        for (int p = 0; p < getPositionCount(); p++) {
            if (getValueCount(p) > 1) {
                return true;
            }
        }
        return false;
    }

    protected boolean assertInvariants() {
        if (firstValueIndexes != null) {
            assert firstValueIndexes.length >= getPositionCount() + 1 : firstValueIndexes.length + " < " + positionCount;
            for (int i = 0; i < getPositionCount(); i++) {
                assert firstValueIndexes[i + 1] > firstValueIndexes[i] : firstValueIndexes[i + 1] + " <= " + firstValueIndexes[i];
            }
        }
        for (int i = 0; i < getPositionCount(); i++) {
            // Either we have multi-values or a null but never both.
            assert ((isNull(i) == false) || (firstValueIndexes[i + 1] - firstValueIndexes[i]) == 1);
        }
        return true;
    }

    @Override
    public final int getPositionCount() {
        return positionCount;
    }


    final int getTotalValueCountIncludingNulls() {
        if (firstValueIndexes == null) {
            return positionCount;
        }
        return firstValueIndexes[positionCount];
    }

    /** Gets the index of the first value for the given position. */
    public final int getFirstValueIndex(int position) {
        return firstValueIndexes == null ? position : firstValueIndexes[position];
    }

    /** Gets the number of values for the given position, possibly 0. */
    @Override
    public final int getValueCount(int position) {
        return isNull(position) ? 0 : firstValueIndexes == null ? 1 : firstValueIndexes[position + 1] - firstValueIndexes[position];
    }
}
