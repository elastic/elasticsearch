/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.OptionalLong;

/**
 * An implementation of a mutable {@link ExponentialHistogram} with a sparse, array-backed representation.
 * <br>
 * Consumers must ensure that if the histogram is mutated, all previously acquired {@link BucketIterator}
 * instances are no longer used.
 * <br>
 * This implementation is thread-safe for all operations provided via {@link ReleasableExponentialHistogram} and its superclasses,
 * as long as it is not mutated concurrently using any of the methods declared in addition in this class
 * (e.g. {@link #tryAddBucket(long, long, boolean)}).
 */
final class FixedCapacityExponentialHistogram extends AbstractExponentialHistogram implements ReleasableExponentialHistogram {

    static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedCapacityExponentialHistogram.class) + ZeroBucket.SHALLOW_SIZE
        + 2 * Buckets.SHALLOW_SIZE;

    // These arrays represent both the positive and the negative buckets.
    // To avoid confusion, we refer to positions within the array as "slots" instead of indices in this file
    // When we use term "index", we mean the exponential histogram bucket index.
    // They store all buckets for the negative range first, with the bucket indices in ascending order,
    // followed by all buckets for the positive range, also with their indices in ascending order.
    // This means we store all the negative buckets first, ordered by their boundaries in descending order (from 0 to -INF),
    // followed by all the positive buckets, ordered by their boundaries in ascending order (from 0 to +INF).
    private final long[] bucketIndices;
    private final long[] bucketCounts;

    private int bucketScale;

    private final Buckets negativeBuckets = new Buckets(false);

    private ZeroBucket zeroBucket;

    private final Buckets positiveBuckets = new Buckets(true);

    private double sum;
    private double min;
    private double max;

    private final ExponentialHistogramCircuitBreaker circuitBreaker;
    private boolean closed = false;

    static FixedCapacityExponentialHistogram create(int bucketCapacity, ExponentialHistogramCircuitBreaker circuitBreaker) {
        circuitBreaker.adjustBreaker(estimateSize(bucketCapacity));
        return new FixedCapacityExponentialHistogram(bucketCapacity, circuitBreaker);
    }

    /**
     * Creates an empty histogram with the given capacity and a {@link ZeroBucket#minimalEmpty()} zero bucket.
     * The scale is initialized to the maximum possible precision ({@link #MAX_SCALE}).
     *
     * @param bucketCapacity the maximum total number of positive and negative buckets this histogram can hold.
     */
    private FixedCapacityExponentialHistogram(int bucketCapacity, ExponentialHistogramCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        bucketIndices = new long[bucketCapacity];
        bucketCounts = new long[bucketCapacity];
        reset();
    }

    int getCapacity() {
        return bucketIndices.length;
    }

    /**
     * Resets this histogram to the same state as a newly constructed one with the same capacity.
     */
    void reset() {
        sum = 0;
        min = Double.NaN;
        max = Double.NaN;
        setZeroBucket(ZeroBucket.minimalEmpty());
        resetBuckets(MAX_SCALE);
    }

    /**
     * Removes all positive and negative buckets from this histogram and sets the scale to the given value.
     *
     * @param scale the scale to set for this histogram
     */
    void resetBuckets(int scale) {
        setScale(scale);
        negativeBuckets.reset();
        positiveBuckets.reset();
    }

    @Override
    public ZeroBucket zeroBucket() {
        return zeroBucket;
    }

    /**
     * Replaces the zero bucket of this histogram with the given one.
     * Callers must ensure that the given {@link ZeroBucket} does not
     * overlap with any of the positive or negative buckets of this histogram.
     *
     * @param zeroBucket the zero bucket to set
     */
    void setZeroBucket(ZeroBucket zeroBucket) {
        this.zeroBucket = zeroBucket;
    }

    @Override
    public double sum() {
        return sum;
    }

    void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public double min() {
        return min;
    }

    void setMin(double min) {
        this.min = min;
    }

    @Override
    public double max() {
        return max;
    }

    void setMax(double max) {
        this.max = max;
    }

    /**
     * Attempts to add a bucket to the positive or negative range of this histogram.
     * <br>
     * Callers must adhere to the following rules:
     * <ul>
     *     <li>All buckets for the negative values range must be provided before the first one from the positive values range.</li>
     *     <li>For both the negative and positive ranges, buckets must be provided with their indices in ascending order.</li>
     *     <li>It is not allowed to provide the same bucket more than once.</li>
     *     <li>It is not allowed to add empty buckets ({@code count <= 0}).</li>
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
    boolean tryAddBucket(long index, long count, boolean isPositive) {
        assert index >= MIN_INDEX && index <= MAX_INDEX : "index must be in range [" + MIN_INDEX + ".." + MAX_INDEX + "]";
        assert isPositive || positiveBuckets.numBuckets == 0 : "Cannot add negative buckets after a positive bucket has been added";
        assert count > 0 : "Cannot add a bucket with empty or negative count";
        if (isPositive) {
            return positiveBuckets.tryAddBucket(index, count);
        } else {
            return negativeBuckets.tryAddBucket(index, count);
        }
    }

    @Override
    public int scale() {
        return bucketScale;
    }

    void setScale(int scale) {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ".." + MAX_SCALE + "]";
        bucketScale = scale;
    }

    @Override
    public ExponentialHistogram.Buckets negativeBuckets() {
        return negativeBuckets;
    }

    @Override
    public ExponentialHistogram.Buckets positiveBuckets() {
        return positiveBuckets;
    }

    /**
     * @return the index of the last bucket added successfully via {@link #tryAddBucket(long, long, boolean)},
     * or {@link Long#MIN_VALUE} if no buckets have been added yet.
     */
    long getLastAddedBucketIndex() {
        if (positiveBuckets.numBuckets + negativeBuckets.numBuckets > 0) {
            return bucketIndices[negativeBuckets.numBuckets + positiveBuckets.numBuckets - 1];
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * @return true, if the last bucket added successfully via {@link #tryAddBucket(long, long, boolean)} was a positive one.
     */
    boolean wasLastAddedBucketPositive() {
        return positiveBuckets.numBuckets > 0;
    }

    @Override
    public void close() {
        if (closed) {
            assert false : "FixedCapacityExponentialHistogram closed multiple times";
        } else {
            closed = true;
            circuitBreaker.adjustBreaker(-ramBytesUsed());
        }
    }

    static long estimateSize(int bucketCapacity) {
        return BASE_SIZE + 2 * RamEstimationUtil.estimateLongArray(bucketCapacity);
    }

    @Override
    public long ramBytesUsed() {
        return estimateSize(bucketIndices.length);
    }

    private class Buckets implements ExponentialHistogram.Buckets {

        static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Buckets.class);

        private final boolean isPositive;
        private int numBuckets;

        private record CachedCountsSum(int numBuckets, long countsSum) {}

        private CachedCountsSum cachedCountsSum;

        /**
         * @param isPositive true, if this object should represent the positive bucket range, false for the negative range
         */
        Buckets(boolean isPositive) {
            this.isPositive = isPositive;
            reset();
        }

        /**
         * @return the position of the first bucket of this set of buckets within {@link #bucketCounts} and {@link #bucketIndices}.
         */
        int startSlot() {
            return isPositive ? negativeBuckets.numBuckets : 0;
        }

        final void reset() {
            numBuckets = 0;
            cachedCountsSum = null;
        }

        boolean tryAddBucket(long index, long count) {
            int slot = startSlot() + numBuckets;
            assert numBuckets == 0 || bucketIndices[slot - 1] < index
                : "Histogram buckets must be added with their indices in ascending order";
            if (slot >= bucketCounts.length) {
                return false; // no more space
            }
            bucketIndices[slot] = index;
            bucketCounts[slot] = count;
            numBuckets++;
            return true;
        }

        @Override
        public CopyableBucketIterator iterator() {
            int start = startSlot();
            return new BucketArrayIterator(bucketScale, bucketCounts, bucketIndices, start, start + numBuckets);
        }

        @Override
        public OptionalLong maxBucketIndex() {
            if (numBuckets == 0) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(bucketIndices[startSlot() + numBuckets - 1]);
            }
        }

        @Override
        public long valueCount() {
            // copy a reference to the field to avoid problems with concurrent updates
            CachedCountsSum cachedVal = cachedCountsSum;
            if (cachedVal != null && cachedVal.numBuckets == numBuckets) {
                return cachedVal.countsSum;
            }

            long countsSum = 0;
            int position = 0;
            if (cachedVal != null) {
                countsSum = cachedVal.countsSum;
                position = cachedVal.numBuckets;
            }

            int startSlot = startSlot();
            while (position < numBuckets) {
                countsSum += bucketCounts[startSlot + position];
                position++;
            }
            this.cachedCountsSum = new CachedCountsSum(position, countsSum);
            return countsSum;
        }

        @Override
        public int bucketCount() {
            return numBuckets;
        }
    }

}
