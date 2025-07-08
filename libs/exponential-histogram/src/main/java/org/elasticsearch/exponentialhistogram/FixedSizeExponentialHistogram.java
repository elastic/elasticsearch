/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

import java.util.Arrays;

public class FixedSizeExponentialHistogram implements ExponentialHistogramBuilder, ExponentialHistogram {

    // scale of 52 is the largest scale being able to represent the smallest and largest double numbers
    // while giving a relative error less
    public static final int DEFAULT_BUCKET_SCALE = 52;

    private final long[] bucketIndices;
    private final long[] bucketCounts;
    private int negativeBucketCount;
    private int positiveBucketCount;
    private int bucketScale;

    private ZeroBucket zeroBucket;

    public FixedSizeExponentialHistogram(int bucketCount) {
        bucketCount = Math.max(bucketCount, 2); // we need at least two buckets, one for positive values, one for negative
        bucketIndices = new long[bucketCount];
        bucketCounts = new long[bucketCount];
        reset();
    }

    void reset() {
        setZeroBucket(ZeroBucket.minimalEmpty());
        resetBuckets(DEFAULT_BUCKET_SCALE);
    }

    @Override
    public void resetBuckets(int newScale) {
        negativeBucketCount = 0;
        positiveBucketCount = 0;
        bucketScale = newScale;
    }

    @Override
    public ZeroBucket zeroBucket() {
        return zeroBucket;
    }

    @Override
    public void setZeroBucket(ZeroBucket zeroBucket) {
        this.zeroBucket = zeroBucket;
    }

    @Override
    public boolean tryAddBucket(long index, long count, boolean isPositive) {
        if (isPositive == false && positiveBucketCount > 0) {
            throw new IllegalArgumentException("Cannot add negative buckets after a positive bucket was added");
        }
        int slot = negativeBucketCount + positiveBucketCount;
        if (slot >= bucketCounts.length) {
            return false; // no more space
        }
        bucketIndices[slot] = index;
        bucketCounts[slot] = count;
        if (isPositive) {
            if (positiveBucketCount > 0 && bucketIndices[slot - 1] > index) {
                throw new IllegalStateException("Buckets must be added in ascending index order!");
            }
            positiveBucketCount++;
        } else {
            if (negativeBucketCount > 0 && bucketIndices[slot - 1] > index) {
                throw new IllegalStateException("Buckets must be added in ascending index order!");
            }
            negativeBucketCount++;
        }
        return true;
    }

    @Override
    public int scale() {
        return bucketScale;
    }

    @Override
    public BucketIterator negativeBuckets() {
        return new BucketArrayIterator(0, negativeBucketCount);
    }

    @Override
    public long maximumBucketIndex() {
        long maxIndex = Long.MIN_VALUE;
        if (negativeBucketCount > 0) {
            maxIndex = bucketIndices[negativeBucketCount - 1];
        }
        if (positiveBucketCount > 0) {
            maxIndex = Math.max(maxIndex, bucketIndices[negativeBucketCount + positiveBucketCount - 1]);
        }
        return maxIndex;
    }

    @Override
    public BucketIterator positiveBuckets() {
        return new BucketArrayIterator(negativeBucketCount, negativeBucketCount + positiveBucketCount);
    }

    private class BucketArrayIterator implements BucketIterator {

        int current;
        final int limit;

        private BucketArrayIterator(int start, int limit) {
            this.current = start;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public long peekCount() {
            if (hasNext() == false) {
                throw new IllegalStateException("No more buckets");
            }
            return bucketCounts[current];
        }

        @Override
        public long peekIndex() {
            if (hasNext() == false) {
                throw new IllegalStateException("No more buckets");
            }
            return bucketIndices[current];
        }

        @Override
        public void advance() {
            if (hasNext() == false) {
                throw new IllegalStateException("No more buckets");
            }
            current++;
        }

        @Override
        public int scale() {
            return FixedSizeExponentialHistogram.this.scale();
        }

        @Override
        public BucketIterator copy() {
            return new BucketArrayIterator(current, limit);
        }
    }

}
