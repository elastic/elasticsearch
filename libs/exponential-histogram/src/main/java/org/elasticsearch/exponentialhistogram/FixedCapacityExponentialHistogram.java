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

import java.util.OptionalLong;

/**
 * An implementation of a mutable {@link ExponentialHistogram} with a sparse, array-backed representation.
 * <br>
 * Consumers must ensure that if the histogram is mutated, all previously acquired {@link BucketIterator}
 * instances are no longer used.
 */
final class FixedCapacityExponentialHistogram implements ExponentialHistogram {

    // These arrays represent both the positive and the negative buckets.
    // To avoid confusion, we refer to positions within the array as "slots" instead of indices in this file
    // When we use term "index", we mean the exponential histogram bucket index.
    // They store all buckets for the negative range first, with the bucket indices in ascending order,
    // followed by all buckets for the positive range, also with their indices in ascending order.
    // This means we store the buckets ordered by their boundaries in ascending order (from -INF to +INF).
    private final long[] bucketIndices;
    private final long[] bucketCounts;

    private int bucketScale;

    private final Buckets negativeBuckets = new Buckets(false);

    private ZeroBucket zeroBucket;

    private final Buckets positiveBuckets = new Buckets(true);

    /**
     * Creates an empty histogram with the given capacity and a {@link ZeroBucket#minimalEmpty()} zero bucket.
     * The scale is initialized to the maximum possible precision ({@link #MAX_SCALE}).
     *
     * @param bucketCapacity the maximum total number of positive and negative buckets this histogram can hold.
     */
    FixedCapacityExponentialHistogram(int bucketCapacity) {
        bucketIndices = new long[bucketCapacity];
        bucketCounts = new long[bucketCapacity];
        reset();
    }

    /**
     * Resets this histogram to the same state as a newly constructed one with the same capacity.
     */
    void reset() {
        setZeroBucket(ZeroBucket.minimalEmpty());
        resetBuckets(MAX_SCALE);
    }

    /**
     * Removes all positive and negative buckets from this histogram and sets the scale to the given value.
     *
     * @param scale the scale to set for this histogram
     */
    void resetBuckets(int scale) {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ".." + MAX_SCALE + "]";
        negativeBuckets.reset();
        positiveBuckets.reset();
        bucketScale = scale;
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

    @Override
    public ExponentialHistogram.Buckets negativeBuckets() {
        return negativeBuckets;
    }

    @Override
    public ExponentialHistogram.Buckets positiveBuckets() {
        return positiveBuckets;
    }

    private class Buckets implements ExponentialHistogram.Buckets {

        private final boolean isPositive;
        private int numBuckets;
        private int cachedValueSumForNumBuckets;
        private long cachedValueSum;

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
            cachedValueSumForNumBuckets = 0;
            cachedValueSum = 0;
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
            return new BucketArrayIterator(start, start + numBuckets);
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
            int startSlot = startSlot();
            while (cachedValueSumForNumBuckets < numBuckets) {
                cachedValueSum += bucketCounts[startSlot + cachedValueSumForNumBuckets];
                cachedValueSumForNumBuckets++;
            }
            return cachedValueSum;
        }
    }

    private class BucketArrayIterator implements CopyableBucketIterator {

        int currentSlot;
        final int limit;

        private BucketArrayIterator(int startSlot, int limit) {
            this.currentSlot = startSlot;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return currentSlot < limit;
        }

        @Override
        public long peekCount() {
            ensureEndNotReached();
            return bucketCounts[currentSlot];
        }

        @Override
        public long peekIndex() {
            ensureEndNotReached();
            return bucketIndices[currentSlot];
        }

        @Override
        public void advance() {
            ensureEndNotReached();
            currentSlot++;
        }

        @Override
        public int scale() {
            return FixedCapacityExponentialHistogram.this.scale();
        }

        @Override
        public CopyableBucketIterator copy() {
            return new BucketArrayIterator(currentSlot, limit);
        }

        private void ensureEndNotReached() {
            if (hasNext() == false) {
                throw new IllegalStateException("Iterator has no more buckets");
            }
        }
    }
}
