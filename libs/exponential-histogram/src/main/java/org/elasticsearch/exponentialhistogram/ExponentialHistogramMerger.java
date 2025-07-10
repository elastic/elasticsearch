/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getMaximumScaleIncrease;

public class ExponentialHistogramMerger {

    FixedSizeExponentialHistogram result;
    FixedSizeExponentialHistogram buffer;

    private boolean isFinished;

    public ExponentialHistogramMerger(int resultBucketCount) {
        result = new FixedSizeExponentialHistogram(resultBucketCount);
        buffer = new FixedSizeExponentialHistogram(resultBucketCount);
    }

    // Only inteded for testing, using this in production means an unnecessary reduction of precision
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
        FixedSizeExponentialHistogram temp = result;
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

    public static ExponentialHistogram merge(int bucketCount, ExponentialHistogram... histograms) {
        return merge(bucketCount, Arrays.stream(histograms));
    }

    public static ExponentialHistogram merge(int bucketCount, Stream<ExponentialHistogram> histograms) {
        ExponentialHistogramMerger merger = new ExponentialHistogramMerger(bucketCount);
        histograms.forEach(merger::add);
        return merger.get();
    }


    // TODO: make this more efficient in case b is much smaller than a
    private static void merge(ExponentialHistogramBuilder output, ExponentialHistogram a, ExponentialHistogram b) {
        ExponentialHistogram.BucketIterator posBucketsA = a.positiveBuckets();
        ExponentialHistogram.BucketIterator negBucketsA = a.negativeBuckets();
        ExponentialHistogram.BucketIterator posBucketsB = b.positiveBuckets();
        ExponentialHistogram.BucketIterator negBucketsB = b.negativeBuckets();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBuckets(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        output.setZeroBucket(zeroBucket);

        // we will attempt to bring everything to the scale of A
        // this might involve increasing the scale for B, which in turn would increase the indices
        // we need to make sure to not exceed the numeric limits (64 bit) for those in this case
        int targetScale = a.scale();
        if (targetScale > b.scale()) {
            boolean isNonEmpty = false;
            if (posBucketsB.hasNext()) {
                isNonEmpty = true;
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease(posBucketsB.peekIndex()));
            }
            if (negBucketsB.hasNext()) {
                isNonEmpty = true;
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease(negBucketsB.peekIndex()));
            }
            if (isNonEmpty) {
                targetScale = Math.min(targetScale, b.scale() + getMaximumScaleIncrease( b.maximumBucketIndex()));
            }
        }

        // Now we are sure that everything fits numerically into targetScale
        // however, we might exceed our limit for the total number of buckets
        // therefore we try the merging optimistically, and if we fail we reduce the target scale accordingly to make everything fit

        MergingBucketIterator positiveMerged = new MergingBucketIterator(posBucketsA.copy(), posBucketsB.copy(), targetScale);
        MergingBucketIterator negativeMerged = new MergingBucketIterator(negBucketsA.copy(), negBucketsB.copy(), targetScale);

        output.resetBuckets(targetScale);
        DownscaleStats downscaleStats = new DownscaleStats();
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
        ExponentialHistogramBuilder output,
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
