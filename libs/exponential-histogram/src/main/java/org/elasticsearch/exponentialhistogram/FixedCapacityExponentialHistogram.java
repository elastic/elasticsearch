/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import java.util.OptionalLong;

/**
 * An implementation of a mutable {@link ExponentialHistogram} with a sparse, array-backed representation.
 * <br>
 * Consumers must ensure that if the histogram is mutated, all previously acquired {@link ExponentialHistogram.BucketIterator}
 * instances are no longer used.
 */
public final class FixedCapacityExponentialHistogram implements ExponentialHistogram {

    // TODO: maybe switch to BigArrays?

    // These arrays represent both the positive and the negative buckets.
    // They store all negative buckets first, in ascending index order, followed by all positive buckets, also in ascending index order.
    private final long[] bucketIndices;
    private final long[] bucketCounts;

    private int negativeBucketCount;
    private int positiveBucketCount;
    private int bucketScale;

    private ZeroBucket zeroBucket;

    /**
     * Creates an empty histogram with the given capacity and a {@link ZeroBucket#minimalEmpty()} zero bucket.
     * The scale is initialized to the maximum possible precision ({@link #MAX_SCALE}).
     *
     * @param bucketCapacity the maximum total number of positive and negative buckets this histogram can hold.
     */
    public FixedCapacityExponentialHistogram(int bucketCapacity) {
        bucketIndices = new long[bucketCapacity];
        bucketCounts = new long[bucketCapacity];
        reset();
    }

    /**
     * Resets this histogram to the same state as a newly constructed one with the same capacity.
     */
    public void reset() {
        setZeroBucket(ZeroBucket.minimalEmpty());
        resetBuckets(MAX_SCALE);
    }

    /**
     * Removes all positive and negative buckets from this histogram and sets the scale to the given value.
     */
    public void resetBuckets(int scale) {
        if (scale > MAX_SCALE || scale < MIN_SCALE) {
            throw new IllegalArgumentException("scale must be in range [" + MIN_SCALE + ".." + MAX_SCALE + "]");
        }
        negativeBucketCount = 0;
        positiveBucketCount = 0;
        bucketScale = scale;
    }

    @Override
    public ZeroBucket zeroBucket() {
        return zeroBucket;
    }

    /**
     * Replaces the zero bucket of this histogram with the given one.
     * Callers must ensure that the given {@link ZeroBucket} does not overlap with any of the positive or negative buckets of this histogram.
     */
    public void setZeroBucket(ZeroBucket zeroBucket) {
        this.zeroBucket = zeroBucket;
    }

    /**
     * Attempts to add a bucket to the positive or negative range of this histogram.
     * <br>
     * Callers must adhere to the following rules:
     * <ul>
     *     <li>All buckets from the negative range must be provided before the first one from the positive range.</li>
     *     <li>For both the negative and positive ranges, buckets must be provided in ascending index order.</li>
     *     <li>It is not allowed to provide the same bucket more than once.</li>
     *     <li>It is not allowed to add empty buckets (count <= 0).</li>
     * </ul>
     *
     * If any of these rules are violated, this call will fail with an exception.
     * If the bucket cannot be added because the maximum capacity has been reached, the call will not modify the state
     * of this histogram and will return {@code false}.
     *
     * @param index      the index of the bucket to add
     * @param count      the count to associate with the given bucket
     * @param isPositive {@code true} if the bucket belongs to the positive range, {@code false} if it belongs to the negative range
     * @return {@code true} if the bucket was added, {@code false} if it could not be added due to insufficient capacity
     */
    public boolean tryAddBucket(long index, long count, boolean isPositive) {
        if (index < MIN_INDEX || index > MAX_INDEX) {
            throw new IllegalArgumentException("index must be in range [" + MIN_INDEX + ".." + MAX_INDEX + "]");
        }
        if (isPositive == false && positiveBucketCount > 0) {
            throw new IllegalArgumentException("Cannot add negative buckets after a positive bucket has been added");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("Cannot add an empty or negative bucket");
        }
        int slot = negativeBucketCount + positiveBucketCount;
        if (slot >= bucketCounts.length) {
            return false; // no more space
        }
        bucketIndices[slot] = index;
        bucketCounts[slot] = count;
        if (isPositive) {
            if (positiveBucketCount > 0 && bucketIndices[slot - 1] >= index) {
                throw new IllegalStateException("Buckets must be added in strictly ascending index order");
            }
            positiveBucketCount++;
        } else {
            if (negativeBucketCount > 0 && bucketIndices[slot - 1] >= index) {
                throw new IllegalStateException("Buckets must be added in strictly ascending index order");
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
    public CopyableBucketIterator negativeBuckets() {
        return new BucketArrayIterator(0, negativeBucketCount);
    }

    @Override
    public OptionalLong maximumBucketIndex() {
        long maxIndex = Long.MIN_VALUE;
        if (negativeBucketCount > 0) {
            maxIndex = bucketIndices[negativeBucketCount - 1];
        }
        if (positiveBucketCount > 0) {
            maxIndex = Math.max(maxIndex, bucketIndices[negativeBucketCount + positiveBucketCount - 1]);
        }
        return maxIndex == Long.MIN_VALUE ? OptionalLong.empty() : OptionalLong.of(maxIndex);
    }

    @Override
    public CopyableBucketIterator positiveBuckets() {
        return new BucketArrayIterator(negativeBucketCount, negativeBucketCount + positiveBucketCount);
    }

    private class BucketArrayIterator implements CopyableBucketIterator {

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
            ensureEndNotReached();
            return bucketCounts[current];
        }

        @Override
        public long peekIndex() {
            ensureEndNotReached();
            return bucketIndices[current];
        }

        @Override
        public void advance() {
            ensureEndNotReached();
            current++;
        }

        @Override
        public int scale() {
            return FixedCapacityExponentialHistogram.this.scale();
        }

        @Override
        public CopyableBucketIterator copy() {
            return new BucketArrayIterator(current, limit);
        }

        private void ensureEndNotReached() {
            if (hasNext() == false) {
                throw new IllegalStateException("Iterator has no more buckets");
            }
        }
    }
}
