/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getMaximumScaleIncrease;

/**
 * Allows accumulating multiple {@link ExponentialHistogram} into a single one
 * while keeping the bucket count in the result below a given limit.
 */
public class ExponentialHistogramMerger {

    // Our algorithm is not in-place, therefore we use two histograms and ping-pong between them
    private FixedCapacityExponentialHistogram result;
    private FixedCapacityExponentialHistogram buffer;

    private final DownscaleStats downscaleStats;

    private boolean isFinished;

    /**
     * Creates a new instance with the specified bucket limit.
     *
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have
     */
    public ExponentialHistogramMerger(int bucketLimit) {
        downscaleStats = new DownscaleStats();
        result = new FixedCapacityExponentialHistogram(bucketLimit);
        buffer = new FixedCapacityExponentialHistogram(bucketLimit);
    }

    // Only intended for testing, using this in production means an unnecessary reduction of precision
    private ExponentialHistogramMerger(int bucketLimit, int minScale) {
        this(bucketLimit);
        result.resetBuckets(minScale);
        buffer.resetBuckets(minScale);
    }

    static ExponentialHistogramMerger createForTesting(int bucketLimit, int minScale) {
        return new ExponentialHistogramMerger(bucketLimit, minScale);
    }

    /**
     * Merges the given histogram into the current result.
     * Must not be called after {@link #get()} has been called.
     *
     * @param toAdd the histogram to merge
     */
    public void add(ExponentialHistogram toAdd) {
        if (isFinished) {
            throw new IllegalStateException("get() has already been called");
        }
        doMerge(toAdd);
    }

    /**
     * Returns the merged histogram.
     *
     * @return the merged histogram
     */
    public ExponentialHistogram get() {
        isFinished = true;
        return result;
    }

    // TODO: this algorithm is very efficient if b has roughly as many buckets as a
    // However, if b is much smaller we still have to iterate over all buckets of a which is very wasteful
    // This can be optimized by buffering multiple histograms to accumulate first,
    // then in O(log(b)) turn them into a single, merged histogram
    // (b is the number of buffered buckets)

    private void doMerge(ExponentialHistogram b) {

        ExponentialHistogram a = result;

        CopyableBucketIterator posBucketsA = a.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsA = a.negativeBuckets().iterator();
        CopyableBucketIterator posBucketsB = b.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsB = b.negativeBuckets().iterator();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBuckets(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        buffer.setZeroBucket(zeroBucket);

        // We attempt to bring everything to the scale of A.
        // This might involve increasing the scale for B, which would increase its indices.
        // We need to ensure that we do not exceed MAX_INDEX / MIN_INDEX in this case.
        int targetScale = a.scale();
        if (targetScale > b.scale()) {
            if (negBucketsB.hasNext()) {
                long smallestIndex = negBucketsB.peekIndex();
                long highestIndex = b.negativeBuckets().maxBucketIndex().getAsLong();
                int maxScaleIncrease = Math.min(getMaximumScaleIncrease(smallestIndex), getMaximumScaleIncrease(highestIndex));
                targetScale = Math.min(targetScale, b.scale() + maxScaleIncrease);
            }
            if (posBucketsB.hasNext()) {
                long smallestIndex = posBucketsB.peekIndex();
                long highestIndex = b.positiveBuckets().maxBucketIndex().getAsLong();
                int maxScaleIncrease = Math.min(getMaximumScaleIncrease(smallestIndex), getMaximumScaleIncrease(highestIndex));
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

            if (overflowCount > 0) {
                throw new IllegalStateException("Should never happen, the histogram should have had enough space");
            }
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

}
