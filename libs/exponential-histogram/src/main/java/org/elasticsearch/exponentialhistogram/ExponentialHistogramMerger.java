/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import java.util.Arrays;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getMaximumScaleIncrease;

/**
 * Allows accumulating multiple {@link ExponentialHistogram} into a single one while keeping the bucket count in the result below a given limit.
 */
public class ExponentialHistogramMerger {

    // Our algorithm is not in-place, therefore we use two histograms and ping-pong between them
    private FixedCapacityExponentialHistogram result;
    private FixedCapacityExponentialHistogram buffer;

    private final DownscaleStats downscaleStats;

    private boolean isFinished;

    /**
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have
     */
    public ExponentialHistogramMerger(int bucketLimit) {
        downscaleStats = new DownscaleStats();
        result = new FixedCapacityExponentialHistogram(bucketLimit);
        buffer = new FixedCapacityExponentialHistogram(bucketLimit);
    }

    // Only intended for testing, using this in production means an unnecessary reduction of precision
    ExponentialHistogramMerger(int resultBucketCount, int minScale) {
        this(resultBucketCount);
        result.resetBuckets(minScale);
        buffer.resetBuckets(minScale);
    }

    public void add(ExponentialHistogram toAdd) {
        if (isFinished) {
            throw new IllegalStateException("get() has already been called");
        }
        merge(buffer, result, toAdd);
        FixedCapacityExponentialHistogram temp = result;
        result = buffer;
        buffer = temp;
    }

    public ExponentialHistogram get() {
        if (isFinished) {
            throw new IllegalStateException("get() has already been called");
        }
        isFinished = true;
        return result;
    }

    // TODO: this algorithm is very efficient if b has roughly as many buckets as a
    // However, if b is much smaller we still have to iterate over all buckets of a which is very wasteful
    // This can be optimized by buffering multiple histograms to accumulate first, then in O(log(b)) turn them into a single, merged histogram
    // (b is the number of buffered buckets)

    private void merge(FixedCapacityExponentialHistogram output, ExponentialHistogram a, ExponentialHistogram b) {
        ExponentialHistogram.CopyableBucketIterator posBucketsA = a.positiveBuckets();
        ExponentialHistogram.CopyableBucketIterator negBucketsA = a.negativeBuckets();
        ExponentialHistogram.CopyableBucketIterator posBucketsB = b.positiveBuckets();
        ExponentialHistogram.CopyableBucketIterator negBucketsB = b.negativeBuckets();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBuckets(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        output.setZeroBucket(zeroBucket);

        // we will attempt to bring everything to the scale of A
        // this might involve increasing the scale for B, which in turn would increase the indices
        // we need to make sure to not exceed MAX_INDEX / MIN_INDEX for those in this case
        int targetScale = a.scale();
        if (targetScale > b.scale()) {
            if (posBucketsB.hasNext()) {
                long smallestIndex = posBucketsB.peekIndex();
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease(smallestIndex));
            }
            if (negBucketsB.hasNext()) {
                long smallestIndex = negBucketsB.peekIndex();
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease(smallestIndex));
            }
            OptionalLong maxIndex = b.maximumBucketIndex();
            if (maxIndex.isPresent()) {
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease(maxIndex.getAsLong()));
            }
        }

        // Now we are sure that everything fits numerically into targetScale
        // however, we might exceed our limit for the total number of buckets
        // therefore we try the merging optimistically, and if we fail we reduce the target scale accordingly to make everything fit

        MergingBucketIterator positiveMerged = new MergingBucketIterator(posBucketsA.copy(), posBucketsB.copy(), targetScale);
        MergingBucketIterator negativeMerged = new MergingBucketIterator(negBucketsA.copy(), negBucketsB.copy(), targetScale);

        output.resetBuckets(targetScale);
        downscaleStats.reset();
        int overflowCount = putBuckets(output, negativeMerged, false, downscaleStats);
        overflowCount += putBuckets(output, positiveMerged, true, downscaleStats);

        if (overflowCount > 0) {
            // UDD-sketch approach: we decrease the scale and retry
            int reduction = downscaleStats.getRequiredScaleReductionToReduceBucketCountBy(overflowCount);
            targetScale -= reduction;
            output.resetBuckets(targetScale);
            positiveMerged = new MergingBucketIterator(posBucketsA, posBucketsB, targetScale);
            negativeMerged = new MergingBucketIterator(negBucketsA, negBucketsB, targetScale);
            overflowCount = putBuckets(output, negativeMerged, false, null);
            overflowCount += putBuckets(output, positiveMerged, true, null);

            if (overflowCount > 0) {
                throw new IllegalStateException("Should never happen, the histogram should have had enough space");
            }
        }
    }

    private static int putBuckets(
        FixedCapacityExponentialHistogram output,
        ExponentialHistogram.BucketIterator buckets,
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

}
