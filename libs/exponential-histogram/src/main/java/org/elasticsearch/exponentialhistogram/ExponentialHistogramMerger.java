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

import java.util.OptionalDouble;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;

/**
 * Allows accumulating multiple {@link ExponentialHistogram} into a single one
 * while keeping the bucket count in the result below a given limit.
 */
public class ExponentialHistogramMerger implements Accountable, Releasable {
    // OpenTelemetry SDK default, we might make this configurable later
    public static final int DEFAULT_MAX_HISTOGRAM_BUCKETS = 320;

    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramMerger.class) + DownscaleStats.SIZE;

    public interface Factory extends Accountable, Releasable {

        /**
         * Creates a new {@link ExponentialHistogramMerger}.
         * All mergers created by this factory must not be used concurrently.
         */
        ExponentialHistogramMerger createMerger();

    }

    /**
     * The factory that created this merger.
     */
    private final MergerFactoryImpl factory;

    // Our algorithm is not in-place, therefore we use two histograms and ping-pong between them
    // The temporary buffer is managed by the factory to allow sharing between multiple mergers
    @Nullable
    private FixedCapacityExponentialHistogram result;
    private boolean closed = false;

    /**
     * Creates a new factory with the OpenTelemetry SDK default bucket limit of
     * {@link ExponentialHistogramMerger#DEFAULT_MAX_HISTOGRAM_BUCKETS}.
     *
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static Factory createFactory(ExponentialHistogramCircuitBreaker circuitBreaker) {
        return MergerFactoryImpl.create(DEFAULT_MAX_HISTOGRAM_BUCKETS, ExponentialHistogram.MAX_SCALE, circuitBreaker);
    }

    /**
     * Creates a new factory with the specified bucket limit.
     *
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have, must be at least 4
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static Factory createFactory(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        return MergerFactoryImpl.create(bucketLimit, ExponentialHistogram.MAX_SCALE, circuitBreaker);
    }

    /**
     * Creates a new instance with the OpenTelemetry SDK default bucket limit of
     * {@link ExponentialHistogramMerger#DEFAULT_MAX_HISTOGRAM_BUCKETS}.
     *
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static ExponentialHistogramMerger create(ExponentialHistogramCircuitBreaker circuitBreaker) {
        return create(DEFAULT_MAX_HISTOGRAM_BUCKETS, circuitBreaker);
    }

    /**
     * Creates a new instance with the specified bucket limit.
     *
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have, must be at least 4
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static ExponentialHistogramMerger create(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        return createWithMaxScale(bucketLimit, ExponentialHistogram.MAX_SCALE, circuitBreaker);
    }

    public static ExponentialHistogramMerger createWithMaxScale(
        int bucketLimit,
        int maxScale,
        ExponentialHistogramCircuitBreaker circuitBreaker
    ) {
        try (var factory = MergerFactoryImpl.create(bucketLimit, maxScale, circuitBreaker)) {
            return factory.createMerger();
        }
    }

    static ExponentialHistogramMerger create(MergerFactoryImpl owningFactory) {
        owningFactory.circuitBreaker.adjustBreaker(BASE_SIZE);
        boolean success = false;
        ExponentialHistogramMerger merger = null;
        try {
            merger = new ExponentialHistogramMerger(owningFactory);
            success = true;
            return merger;
        } finally {
            if (success == false) {
                owningFactory.circuitBreaker.adjustBreaker(-BASE_SIZE);
            }
        }
    }

    private ExponentialHistogramMerger(MergerFactoryImpl owningFactory) {
        this.factory = owningFactory;
    }

    @Override
    public void close() {
        if (closed) {
            assert false : "ExponentialHistogramMerger closed multiple times";
        } else {
            closed = true;
            if (result != null) {
                factory.releaseBuffer(result);
                result = null;
            }
            factory.circuitBreaker.adjustBreaker(-BASE_SIZE);
            factory.notifyMergerClosed();
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_SIZE;
        if (result != null) {
            size += result.ramBytesUsed();
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

    /**
     * Merges the given histogram into the current result.
     *
     * @param toAdd the histogram to merge
     */
    public void add(ExponentialHistogram toAdd) {
        if (toAdd.isEmpty()) {
            return;
        }
        ExponentialHistogram a = result == null ? ExponentialHistogram.empty() : result;
        ExponentialHistogram b = toAdd;

        CopyableBucketIterator posBucketsA = a.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsA = a.negativeBuckets().iterator();
        CopyableBucketIterator posBucketsB = b.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsB = b.negativeBuckets().iterator();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBucketsForAll(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        FixedCapacityExponentialHistogram buffer = factory.acquireBuffer();
        try {

            buffer.setZeroBucket(zeroBucket);
            buffer.setSum(a.sum() + b.sum());
            buffer.setMin(nanAwareAggregate(a.min(), b.min(), Math::min));
            buffer.setMax(nanAwareAggregate(a.max(), b.max(), Math::max));

            int targetScale = Math.min(factory.maxScale, Math.min(a.scale(), b.scale()));

            // We might exceed our limit for the total number of buckets for the targetScale.
            // We try the merge optimistically, assuming that everything fits.
            // If we fail, we reduce the target scale to make everything fit.

            mergeBucketsInto(negBucketsA, posBucketsA, negBucketsB, posBucketsB, targetScale, Long::sum, buffer);
            FixedCapacityExponentialHistogram temp = result;
            result = buffer;
            buffer = temp;
        } finally {
            factory.releaseBuffer(buffer);
        }
    }

    private void mergeBucketsInto(
        CopyableBucketIterator negBucketsA,
        CopyableBucketIterator posBucketsA,
        CopyableBucketIterator negBucketsB,
        CopyableBucketIterator posBucketsB,
        int targetScale,
        LongBinaryOperator countCombineFunction,
        FixedCapacityExponentialHistogram output
    ) {
        DownscaleStats downscaleStats = factory.downscaleStats;
        MergingBucketIterator positiveMerged = new MergingBucketIterator(
            posBucketsA.copy(),
            posBucketsB.copy(),
            targetScale,
            countCombineFunction
        );
        MergingBucketIterator negativeMerged = new MergingBucketIterator(
            negBucketsA.copy(),
            negBucketsB.copy(),
            targetScale,
            countCombineFunction
        );

        // We might exceed our limit for the total number of buckets for the targetScale.
        // We try the merge optimistically, assuming that everything fits.
        // If we fail, we reduce the target scale to make everything fit and redo the merge

        output.resetBuckets(targetScale);
        downscaleStats.reset();
        int overflowCount = putBuckets(output, negativeMerged, false, downscaleStats);
        overflowCount += putBuckets(output, positiveMerged, true, downscaleStats);

        if (overflowCount > 0) {
            // UDD-sketch approach: decrease the scale and retry.
            int reduction = downscaleStats.getRequiredScaleReductionToReduceBucketCountBy(overflowCount);
            targetScale -= reduction;
            output.resetBuckets(targetScale);
            positiveMerged = new MergingBucketIterator(posBucketsA, posBucketsB, targetScale, countCombineFunction);
            negativeMerged = new MergingBucketIterator(negBucketsA, negBucketsB, targetScale, countCombineFunction);
            overflowCount = putBuckets(output, negativeMerged, false, null);
            overflowCount += putBuckets(output, positiveMerged, true, null);

            assert overflowCount == 0 : "Should never happen, the histogram should have had enough space";
        }
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
            if (buckets.peekCount() == 0) {
                // skip empty buckets which might occur when subtracting histograms
                buckets.advance();
                continue;
            }

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

    /**
     * Clears this merger and sets it to the histogram {@code a} minus the histogram {@code b}.
     * <p>
     * This method is intended to compute the delta between two cumulative histograms from the same time series,
     * where {@code a} is a later snapshot and {@code b} is an earlier snapshot.
     * In cumulative histograms, bucket counts only increase over time, which means that subtracting {@code b}
     * from {@code a} will never result in negative bucket counts.
     * <p>
     * The algorithm provides the following guarantees:
     * Given two exponential histograms {@code b} and {@code c}.
     * The histogram {@code a} is the result of merging the histograms {@code b} and {@code c}.
     * Then {@code a - b} will yield the histogram {@code c}, with the limitation that
     * <ul>
     *     <li>{@code a - b} might have a smaller scale (=less precision) than {@code c} (but no lower than the scale of {@code a})</li>
     *     <li>{@code a - b} might have a greater zero threshold than {@code c} (but not greater than the zero threshold of {@code a})</li>
     *     <li>{@code a - b} might not preserve the exact minimum / maximum of {@code c}, but will provide an estimate in that case</li>
     * </ul>
     *
     * @param a the base histogram to subtract from
     * @param b the histogram to be subtracted
     */
    public void setToDifference(ExponentialHistogram a, ExponentialHistogram b) {
        if (result != null) {
            factory.releaseBuffer(result);
            result = null;
        }
        long bValueCount = b.valueCount();
        long aValueCount = a.valueCount();
        if (aValueCount == bValueCount) {
            // fast path, subtracting histograms with equal count will yield an empty histogram
            // We just assume here that the input is actually subtractable, e.g. we don't verify that all buckets are equal too
            return;
        }
        if (bValueCount == 0) {
            // fast path, subtracting an empty histogram does nothing.
            // This check is placed before the scale check because the ExponentialHistogram.empty() singleton
            // uses the maximum scale, so we never want to fail when subtracting an empty histogram.
            this.add(a);
            return;
        }
        if (aValueCount < bValueCount) {
            throw new IllegalArgumentException(
                "Cannot subtract histograms (a-b), where a.count < b.count: " + aValueCount + " < " + bValueCount
            );
        }
        if (a.scale() > b.scale()) {
            throw new IllegalArgumentException(
                "Cannot subtract histograms (a-b), where a.scale > b.scale: " + a.scale() + " > " + b.scale()
            );
        }
        if (a.min() > b.min()) {
            throw new IllegalArgumentException("Cannot subtract histograms (a-b), where a.min > b.min: " + a.min() + " > " + b.min());
        }
        if (a.max() < b.max()) {
            throw new IllegalArgumentException("Cannot subtract histograms (a-b), where a.max < b.max: " + a.max() + " < " + b.max());
        }
        if (a.zeroBucket().compareZeroThreshold(b.zeroBucket()) < 0) {
            throw new IllegalArgumentException(
                "Cannot subtract histograms (a-b), where a.zeroThreshold < b.zeroThreshold: "
                    + a.zeroBucket().zeroThreshold()
                    + " < "
                    + b.zeroBucket().zeroThreshold()
            );
        }

        CopyableBucketIterator negBucketsB = b.negativeBuckets().iterator();
        CopyableBucketIterator posBucketsB = b.positiveBuckets().iterator();

        // adjust the zeroThreshold of B to match A, collapsing overlapping buckets into the zero bucket
        ZeroBucket updatedZeroBucketB = a.zeroBucket()
            .withCount(b.zeroBucket().count())
            .collapseOverlappingBucketsForAll(negBucketsB, posBucketsB);

        // now we can subtract the zero buckets
        assert updatedZeroBucketB.compareZeroThreshold(a.zeroBucket()) == 0
            : "After collapsing overlapping buckets, the zero threshold of B should match that of A";
        if (a.zeroBucket().count() < updatedZeroBucketB.count()) {
            throw new IllegalArgumentException(
                "Cannot subtract histograms (a-b), where a.zeroCount < b.zeroCount after adjusting to the same zeroThreshold: "
                    + a.zeroBucket().count()
                    + " < "
                    + updatedZeroBucketB.count()
            );
        }

        ZeroBucket subtractedZeroBucket = a.zeroBucket().withCount(a.zeroBucket().count() - updatedZeroBucketB.count());

        LongBinaryOperator bucketDifferenceOperator = (aCount, bCount) -> {
            if (aCount < bCount) {
                throw new IllegalArgumentException(
                    "Cannot subtract histograms (a-b), where A has a smaller count for the same bucket than B: " + aCount + " < " + bCount
                );
            }
            return aCount - bCount;
        };

        FixedCapacityExponentialHistogram buffer = factory.acquireBuffer();
        try {
            mergeBucketsInto(
                a.negativeBuckets().iterator(),
                a.positiveBuckets().iterator(),
                negBucketsB,
                posBucketsB,
                a.scale(),
                bucketDifferenceOperator,
                buffer
            );

            buffer.setZeroBucket(subtractedZeroBucket);
            buffer.setSum(a.sum() - b.sum());

            if (a.min() < b.min()) {
                // A was generated by adding new values to B. These new values included a new, smallest value
                // That implies that this is also the smallest value of the difference
                buffer.setMin(a.min());
            } else {
                assert a.min() == b.min();
                // In this case we don't know the exact minimum anymore, we have to estimate it
                OptionalDouble estimatedMin = ExponentialHistogramUtils.estimateMin(
                    buffer.zeroBucket(),
                    buffer.negativeBuckets(),
                    buffer.positiveBuckets()
                );
                assert estimatedMin.isPresent()
                    : "The merged histogram should have at least one value, so the estimated minimum should be present";
                buffer.setMin(Math.max(a.min(), estimatedMin.getAsDouble()));
            }

            // Same logic as for min
            if (a.max() > b.max()) {
                buffer.setMax(a.max());
            } else {
                assert a.max() == b.max();
                OptionalDouble estimatedMax = ExponentialHistogramUtils.estimateMax(
                    buffer.zeroBucket(),
                    buffer.negativeBuckets(),
                    buffer.positiveBuckets()
                );
                assert estimatedMax.isPresent()
                    : "The merged histogram should have at least one value, so the estimated maximum should be present";
                buffer.setMax(Math.min(a.max(), estimatedMax.getAsDouble()));
            }

            result = buffer;
            buffer = null;
        } finally {
            factory.releaseBuffer(buffer);
        }
    }

}
