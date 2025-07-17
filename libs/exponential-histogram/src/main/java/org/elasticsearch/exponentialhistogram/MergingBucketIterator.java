/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

/**
 * An iterator that merges two bucket iterators, aligning them to a common scale and combining buckets with the same index.
 */
final class MergingBucketIterator implements BucketIterator {

    private final BucketIterator itA;
    private final BucketIterator itB;

    private boolean endReached;
    private long currentIndex;
    private long currentCount;

    /**
     * Creates a new merging iterator.
     *
     * @param itA         the first iterator to merge
     * @param itB         the second iterator to merge
     * @param targetScale the histogram scale to which both iterators should be aligned
     */
    MergingBucketIterator(BucketIterator itA, BucketIterator itB, int targetScale) {
        this.itA = new ScaleAdjustingBucketIterator(itA, targetScale);
        this.itB = new ScaleAdjustingBucketIterator(itB, targetScale);
        endReached = false;
        advance();
    }

    @Override
    public void advance() {
        boolean hasNextA = itA.hasNext();
        boolean hasNextB = itB.hasNext();
        endReached = hasNextA == false && hasNextB == false;
        if (endReached) {
            return;
        }
        long idxA = 0;
        long idxB = 0;
        if (hasNextA) {
            idxA = itA.peekIndex();
        }
        if (hasNextB) {
            idxB = itB.peekIndex();
        }

        currentCount = 0;
        boolean advanceA = hasNextA && (hasNextB == false || idxA <= idxB);
        boolean advanceB = hasNextB && (hasNextA == false || idxB <= idxA);
        if (advanceA) {
            currentIndex = idxA;
            currentCount += itA.peekCount();
            itA.advance();
        }
        if (advanceB) {
            currentIndex = idxB;
            currentCount += itB.peekCount();
            itB.advance();
        }
    }

    @Override
    public boolean hasNext() {
        return endReached == false;
    }

    @Override
    public long peekCount() {
        assertEndNotReached();
        return currentCount;
    }

    @Override
    public long peekIndex() {
        assertEndNotReached();
        return currentIndex;
    }

    @Override
    public int scale() {
        return itA.scale();
    }

    private void assertEndNotReached() {
        if (endReached) {
            throw new IllegalStateException("Iterator has no more buckets");
        }
    }
}
