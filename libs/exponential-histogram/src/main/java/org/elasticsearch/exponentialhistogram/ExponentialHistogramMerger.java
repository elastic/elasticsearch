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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.OptionalLong;
import java.util.function.DoubleBinaryOperator;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getMaximumScaleIncrease;

/**
 * Allows accumulating multiple {@link ExponentialHistogram} into a single one
 * while keeping the bucket count in the result below a given limit.
 */
public class ExponentialHistogramMerger implements Accountable, Releasable {

    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramMerger.class) + DownscaleStats.SIZE;

    // Our algorithm is not in-place, therefore we use two histograms and ping-pong between them
    @Nullable
    private FixedCapacityExponentialHistogram result;
    @Nullable
    private FixedCapacityExponentialHistogram buffer;

    private final int bucketLimit;
    private final int maxScale;

    private final DownscaleStats downscaleStats;

    private final ExponentialHistogramCircuitBreaker circuitBreaker;
    private boolean closed = false;

    /**
     * Creates a new instance with the specified bucket limit.
     *
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have, must be at least 4
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static ExponentialHistogramMerger create(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        circuitBreaker.adjustBreaker(BASE_SIZE);
        boolean success = false;
        try {
            ExponentialHistogramMerger result = new ExponentialHistogramMerger(bucketLimit, circuitBreaker);
            success = true;
            return result;
        } finally {
            if (success == false) {
                circuitBreaker.adjustBreaker(-BASE_SIZE);
            }
        }
    }

    private ExponentialHistogramMerger(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        this(bucketLimit, ExponentialHistogram.MAX_SCALE, circuitBreaker);
    }

    // Only intended for testing, using this in production means an unnecessary reduction of precision
    private ExponentialHistogramMerger(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        // We need at least four buckets to represent any possible distribution
        if (bucketLimit < 4) {
            throw new IllegalArgumentException("The bucket limit must be at least 4");
        }
        this.bucketLimit = bucketLimit;
        this.maxScale = maxScale;
        this.circuitBreaker = circuitBreaker;
        downscaleStats = new DownscaleStats();
    }

    public static ExponentialHistogramMerger createWithMaxScale(
        int bucketLimit,
        int maxScale,
        ExponentialHistogramCircuitBreaker circuitBreaker
    ) {
        circuitBreaker.adjustBreaker(BASE_SIZE);
        return new ExponentialHistogramMerger(bucketLimit, maxScale, circuitBreaker);
    }

    @Override
    public void close() {
        if (closed) {
            assert false : "ExponentialHistogramMerger closed multiple times";
        } else {
            closed = true;
            if (result != null) {
                result.close();
                result = null;
            }
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
            circuitBreaker.adjustBreaker(-BASE_SIZE);
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_SIZE;
        if (result != null) {
            size += result.ramBytesUsed();
        }
        if (buffer != null) {
            size += buffer.ramBytesUsed();
        }
        return size;
    }

    /**
     * Returns the merged histogram and clears this merger.
     * The caller takes ownership of the returned histogram and must ensure that {@link #close()} is called.
     *
     * @return the merged histogram
     */
    public ReleasableExponentialHistogram getAndClear() {
        assert closed == false : "ExponentialHistogramMerger already closed";
        ReleasableExponentialHistogram retVal = (result == null) ? ReleasableExponentialHistogram.empty() : result;
        result = null;
        return retVal;
    }

    /**
     * Gets the current merged histogram without clearing this merger.
     * Note that the ownership of the returned histogram remains with this merger,
     * so the caller must not close it.
     * The returned histogram is only valid until the next call to {@link #add(ExponentialHistogram)}, or until the merger is closed.
     *
     * @return the current merged histogram
     */
    public ExponentialHistogram get() {
        assert closed == false : "ExponentialHistogramMerger already closed";
        return (result == null) ? ExponentialHistogram.empty() : result;
    }

    /**
     * Merges the given histogram into the current result, not upscaling it.
     * This should be used when merging intermediate results to prevent accumulating errors.
     *
     * @param toAdd the histogram to merge
     */
    public void addWithoutUpscaling(ExponentialHistogram toAdd) {
        add(toAdd, false);
    }

    /**
     * Merges the given histogram into the current result. The histogram might be upscaled if needed.
     *
     * @param toAdd the histogram to merge
     */
    public void add(ExponentialHistogram toAdd) {
        add(toAdd, true);
    }

    // This algorithm is very efficient if B has roughly as many buckets as A.
    // However, if B is much smaller we still have to iterate over all buckets of A.
    // This can be optimized by buffering the buckets of small histograms and only merging them when we have enough buckets.
    // The buffered histogram buckets would first be merged with each other, and then be merged with accumulator.
    //
    // However, benchmarks of a PoC implementation have shown that this only brings significant improvements
    // if the accumulator size is 500+ and the merged histograms are smaller than 50 buckets
    // and otherwise slows down the merging.
    // It would be possible to only enable the buffering for small histograms,
    // but the optimization seems not worth the added complexity at this point.

    private void add(ExponentialHistogram toAdd, boolean allowUpscaling) {
        ExponentialHistogram a = result == null ? ExponentialHistogram.empty() : result;
        ExponentialHistogram b = toAdd;

        CopyableBucketIterator posBucketsA = a.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsA = a.negativeBuckets().iterator();
        CopyableBucketIterator posBucketsB = b.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsB = b.negativeBuckets().iterator();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBucketsForAll(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        if (buffer == null) {
            buffer = FixedCapacityExponentialHistogram.create(bucketLimit, circuitBreaker);
        }
        buffer.setZeroBucket(zeroBucket);
        buffer.setSum(a.sum() + b.sum());
        buffer.setMin(nanAwareAggregate(a.min(), b.min(), Math::min));
        buffer.setMax(nanAwareAggregate(a.max(), b.max(), Math::max));
        // We attempt to bring everything to the scale of A.
        // This might involve increasing the scale for B, which would increase its indices.
        // We need to ensure that we do not exceed MAX_INDEX / MIN_INDEX in this case.
        int targetScale = Math.min(maxScale, a.scale());
        if (allowUpscaling == false) {
            targetScale = Math.min(targetScale, b.scale());
        }

        if (targetScale > b.scale()) {
            if (negBucketsB.hasNext()) {
                long smallestIndex = negBucketsB.peekIndex();
                OptionalLong maximumIndex = b.negativeBuckets().maxBucketIndex();
                assert maximumIndex.isPresent()
                    : "We checked that the negative bucket range is not empty, therefore the maximum index should be present";
                int maxScaleIncrease = Math.min(getMaximumScaleIncrease(smallestIndex), getMaximumScaleIncrease(maximumIndex.getAsLong()));
                targetScale = Math.min(targetScale, b.scale() + maxScaleIncrease);
            }
            if (posBucketsB.hasNext()) {
                long smallestIndex = posBucketsB.peekIndex();
                OptionalLong maximumIndex = b.positiveBuckets().maxBucketIndex();
                assert maximumIndex.isPresent()
                    : "We checked that the positive bucket range is not empty, therefore the maximum index should be present";
                int maxScaleIncrease = Math.min(getMaximumScaleIncrease(smallestIndex), getMaximumScaleIncrease(maximumIndex.getAsLong()));
                targetScale = Math.min(targetScale, b.scale() + maxScaleIncrease);
            }
        }

        // Now we are sure that everything fits numerically into targetScale.
        // However, we might exceed our limit for the total number of buckets.
        // Therefore, we try the merge optimistically. If we fail, we reduce the target scale to make everything fit.

        MergingBucketIterator positiveMerged = new MergingBucketIterator(posBucketsA.copy(), posBucketsB.copy(), targetScale);
        MergingBucketIterator negativeMerged = new MergingBucketIterator(negBucketsA.copy(), negBucketsB.copy(), targetScale);

        buffer.resetBuckets(targetScale);
        downscaleStats.reset();
        int overflowCount = putBuckets(buffer, negativeMerged, false, downscaleStats);
        overflowCount += putBuckets(buffer, positiveMerged, true, downscaleStats);

        if (overflowCount > 0) {
            // UDD-sketch approach: decrease the scale and retry.
            int reduction = downscaleStats.getRequiredScaleReductionToReduceBucketCountBy(overflowCount);
            targetScale -= reduction;
            buffer.resetBuckets(targetScale);
            positiveMerged = new MergingBucketIterator(posBucketsA, posBucketsB, targetScale);
            negativeMerged = new MergingBucketIterator(negBucketsA, negBucketsB, targetScale);
            overflowCount = putBuckets(buffer, negativeMerged, false, null);
            overflowCount += putBuckets(buffer, positiveMerged, true, null);

            assert overflowCount == 0 : "Should never happen, the histogram should have had enough space";
        }
        FixedCapacityExponentialHistogram temp = result;
        result = buffer;
        buffer = temp;
    }

    private static int putBuckets(
        FixedCapacityExponentialHistogram output,
        BucketIterator buckets,
        boolean isPositive,
        DownscaleStats downscaleStats
    ) {
        boolean collectDownScaleStatsOnNext = false;
        long prevIndex = 0;
        int overflowCount = 0;
        while (buckets.hasNext()) {
            long idx = buckets.peekIndex();
            if (collectDownScaleStatsOnNext) {
                downscaleStats.add(prevIndex, idx);
            } else {
                collectDownScaleStatsOnNext = downscaleStats != null;
            }

            if (output.tryAddBucket(idx, buckets.peekCount(), isPositive) == false) {
                overflowCount++;
            }

            prevIndex = idx;
            buckets.advance();
        }
        return overflowCount;
    }

    private static double nanAwareAggregate(double a, double b, DoubleBinaryOperator aggregator) {
        if (Double.isNaN(a)) {
            return b;
        }
        if (Double.isNaN(b)) {
            return a;
        }
        return aggregator.applyAsDouble(a, b);
    }

}
