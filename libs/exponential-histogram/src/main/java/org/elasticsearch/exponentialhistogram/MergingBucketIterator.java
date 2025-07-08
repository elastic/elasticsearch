/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

public class MergingBucketIterator implements ExponentialHistogram.BucketIterator {

    private final ExponentialHistogram.BucketIterator itA;
    private final ExponentialHistogram.BucketIterator itB;

    private boolean endReached;
    private long currentIndex;
    private long currentCount;

    public MergingBucketIterator(ExponentialHistogram.BucketIterator itA, ExponentialHistogram.BucketIterator itB, int targetScale) {
        this.itA = new ScaleAdjustingBucketIterator(itA, targetScale);
        this.itB = new ScaleAdjustingBucketIterator(itB, targetScale);
        endReached = false;
        advance();
    }

    @Override
    public void advance() {
        boolean hasNextA = itA.hasNext() ;
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

    @Override
    public ExponentialHistogram.BucketIterator copy() {
        throw new UnsupportedOperationException();
    }

    private void assertEndNotReached() {
        if (endReached) {
            throw new IllegalStateException("No more buckets");
        }
    }
}
