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

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.TreeMap;

/**
 * A builder for building a {@link ReleasableExponentialHistogram} directly from buckets.
 */
public class ExponentialHistogramBuilder implements Releasable {

    private static final int DEFAULT_ESTIMATED_BUCKET_COUNT = 32;

    private final ExponentialHistogramCircuitBreaker breaker;

    private int scale;
    private ZeroBucket zeroBucket = ZeroBucket.minimalEmpty();
    private Double sum;
    private Double min;
    private Double max;

    private int estimatedBucketCount = DEFAULT_ESTIMATED_BUCKET_COUNT;

    // If the buckets are provided in order, we directly build the histogram to avoid unnecessary copies and allocations
    // If a bucket is received out of order, we fallback to storing the buckets in the TreeMaps and build the histogram at the end.
    private FixedCapacityExponentialHistogram result;
    // Visible for testing to ensure that the low-allocation path is taken for ordered buckets
    TreeMap<Long, Long> negativeBuckets;
    TreeMap<Long, Long> positiveBuckets;

    private boolean resultAlreadyReturned = false;

    ExponentialHistogramBuilder(int scale, ExponentialHistogramCircuitBreaker breaker) {
        this.breaker = breaker;
        this.scale = scale;
    }

    ExponentialHistogramBuilder(ExponentialHistogram toCopy, ExponentialHistogramCircuitBreaker breaker) {
        this(toCopy.scale(), breaker);
        zeroBucket(toCopy.zeroBucket());
        sum(toCopy.sum());
        min(toCopy.min());
        max(toCopy.max());
        estimatedBucketCount(toCopy.negativeBuckets().bucketCount() + toCopy.positiveBuckets().bucketCount());
        BucketIterator negBuckets = toCopy.negativeBuckets().iterator();
        while (negBuckets.hasNext()) {
            setNegativeBucket(negBuckets.peekIndex(), negBuckets.peekCount());
            negBuckets.advance();
        }
        BucketIterator posBuckets = toCopy.positiveBuckets().iterator();
        while (posBuckets.hasNext()) {
            setPositiveBucket(posBuckets.peekIndex(), posBuckets.peekCount());
            posBuckets.advance();
        }
    }

    /**
     * If known, sets the estimated total number of buckets to minimize unnecessary allocations.
     * Only has an effect if invoked before the first call to
     * {@link #setPositiveBucket(long, long)} and {@link #setNegativeBucket(long, long)}.
     *
     * @param totalBuckets the total number of buckets expected to be added
     * @return the builder
     */
    public ExponentialHistogramBuilder estimatedBucketCount(int totalBuckets) {
        estimatedBucketCount = totalBuckets;
        return this;
    }

    public ExponentialHistogramBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public ExponentialHistogramBuilder zeroBucket(ZeroBucket zeroBucket) {
        this.zeroBucket = zeroBucket;
        return this;
    }

    /**
     * Sets the sum of the histogram values. If not set, the sum will be estimated from the buckets.
     * @param sum the sum value
     * @return the builder
     */
    public ExponentialHistogramBuilder sum(double sum) {
        this.sum = sum;
        return this;
    }

    /**
     * Sets the min value of the histogram values. If not set, the min will be estimated from the buckets.
     * @param min the min value
     * @return the builder
     */
    public ExponentialHistogramBuilder min(double min) {
        this.min = min;
        return this;
    }

    /**
     * Sets the max value of the histogram values. If not set, the max will be estimated from the buckets.
     * @param max the max value
     * @return the builder
     */
    public ExponentialHistogramBuilder max(double max) {
        this.max = max;
        return this;
    }

    /**
     * Sets the given bucket of the positive buckets. If the bucket already exists, it will be replaced.
     * Buckets may be set in arbitrary order. However, for best performance and minimal allocations,
     * buckets should be set in order of increasing index and all negative buckets should be set before positive buckets.
     *
     * @param index the index of the bucket
     * @param count the count of the bucket, must be at least 1
     * @return the builder
     */
    public ExponentialHistogramBuilder setPositiveBucket(long index, long count) {
        setBucket(index, count, true);
        return this;
    }

    /**
     * Sets the given bucket of the negative buckets. If the bucket already exists, it will be replaced.
     * Buckets may be set in arbitrary order. However, for best performance and minimal allocations,
     * buckets should be set in order of increasing index and all negative buckets should be set before positive buckets.
     *
     * @param index the index of the bucket
     * @param count the count of the bucket, must be at least 1
     * @return the builder
     */
    public ExponentialHistogramBuilder setNegativeBucket(long index, long count) {
        setBucket(index, count, false);
        return this;
    }

    private void setBucket(long index, long count, boolean isPositive) {
        if (count < 1) {
            throw new IllegalArgumentException("Bucket count must be at least 1");
        }
        if (negativeBuckets == null && positiveBuckets == null) {
            // so far, all received buckets were in order, try to directly build the result
            if (result == null) {
                // Initialize the result buffer if required
                reallocateResultWithCapacity(estimatedBucketCount, false);
            }
            if ((isPositive && result.wasLastAddedBucketPositive() == false)
                || (isPositive == result.wasLastAddedBucketPositive() && index > result.getLastAddedBucketIndex())) {
                // the new bucket is in order too, we can directly add the bucket
                addBucketToResult(index, count, isPositive);
                return;
            }
        }
        // fallback to TreeMap if a bucket is received out of order
        initializeBucketTreeMapsIfNeeded();
        if (isPositive) {
            positiveBuckets.put(index, count);
        } else {
            negativeBuckets.put(index, count);
        }
    }

    private void initializeBucketTreeMapsIfNeeded() {
        if (negativeBuckets == null) {
            negativeBuckets = new TreeMap<>();
            positiveBuckets = new TreeMap<>();
            // copy existing buckets to the maps
            if (result != null) {
                BucketIterator it = result.negativeBuckets().iterator();
                while (it.hasNext()) {
                    negativeBuckets.put(it.peekIndex(), it.peekCount());
                    it.advance();
                }
                it = result.positiveBuckets().iterator();
                while (it.hasNext()) {
                    positiveBuckets.put(it.peekIndex(), it.peekCount());
                    it.advance();
                }
            }
        }
    }

    private void addBucketToResult(long index, long count, boolean isPositive) {
        if (resultAlreadyReturned) {
            // we cannot modify the result anymore, create a new one
            reallocateResultWithCapacity(result.getCapacity(), true);
        }
        assert resultAlreadyReturned == false;
        boolean sufficientCapacity = result.tryAddBucket(index, count, isPositive);
        if (sufficientCapacity == false) {
            int newCapacity = Math.max(result.getCapacity() * 2, DEFAULT_ESTIMATED_BUCKET_COUNT);
            reallocateResultWithCapacity(newCapacity, true);
            boolean bucketAdded = result.tryAddBucket(index, count, isPositive);
            assert bucketAdded : "Output histogram should have enough capacity";
        }
    }

    private void reallocateResultWithCapacity(int newCapacity, boolean copyBucketsFromPreviousResult) {
        FixedCapacityExponentialHistogram newResult = FixedCapacityExponentialHistogram.create(newCapacity, breaker);
        if (copyBucketsFromPreviousResult && result != null) {
            BucketIterator it = result.negativeBuckets().iterator();
            while (it.hasNext()) {
                boolean added = newResult.tryAddBucket(it.peekIndex(), it.peekCount(), false);
                assert added : "Output histogram should have enough capacity";
                it.advance();
            }
            it = result.positiveBuckets().iterator();
            while (it.hasNext()) {
                boolean added = newResult.tryAddBucket(it.peekIndex(), it.peekCount(), true);
                assert added : "Output histogram should have enough capacity";
                it.advance();
            }
        }
        if (result != null && resultAlreadyReturned == false) {
            Releasables.close(result);
        }
        resultAlreadyReturned = false;
        result = newResult;
    }

    public ReleasableExponentialHistogram build() {
        if (resultAlreadyReturned) {
            // result was already returned on a previous call, return a new instance
            reallocateResultWithCapacity(result.getCapacity(), true);
        }
        assert resultAlreadyReturned == false;
        if (negativeBuckets != null) {
            // copy buckets from tree maps into result
            reallocateResultWithCapacity(negativeBuckets.size() + positiveBuckets.size(), false);
            result.resetBuckets(scale);
            negativeBuckets.forEach((index, count) -> result.tryAddBucket(index, count, false));
            positiveBuckets.forEach((index, count) -> result.tryAddBucket(index, count, true));
        } else {
            if (result == null) {
                // no buckets were added
                reallocateResultWithCapacity(0, false);
            }
            result.setScale(scale);
        }

        result.setZeroBucket(zeroBucket);
        double sumVal = (sum != null)
            ? sum
            : ExponentialHistogramUtils.estimateSum(result.negativeBuckets().iterator(), result.positiveBuckets().iterator());
        double minVal = (min != null)
            ? min
            : ExponentialHistogramUtils.estimateMin(zeroBucket, result.negativeBuckets(), result.positiveBuckets()).orElse(Double.NaN);
        double maxVal = (max != null)
            ? max
            : ExponentialHistogramUtils.estimateMax(zeroBucket, result.negativeBuckets(), result.positiveBuckets()).orElse(Double.NaN);

        result.setMin(minVal);
        result.setMax(maxVal);
        result.setSum(sumVal);

        resultAlreadyReturned = true;
        return result;
    }

    @Override
    public void close() {
        if (resultAlreadyReturned == false) {
            Releasables.close(result);
        }
    }
}
