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

import java.util.OptionalDouble;
import java.util.OptionalLong;

public class ExponentialHistogramUtils {

    /**
     * Estimates the sum of all values of a histogram just based on the populated buckets.
     * Will never return NaN, but might return +/-Infinity if the histogram is too big.
     *
     * @param negativeBuckets the negative buckets of the histogram
     * @param positiveBuckets the positive buckets of the histogram
     * @return the estimated sum of all values in the histogram, guaranteed to be zero if there are no buckets.
     */
    public static double estimateSum(BucketIterator negativeBuckets, BucketIterator positiveBuckets) {
        assert negativeBuckets.scale() == positiveBuckets.scale();

        // for each bucket index, sum up the counts, but account for the positive/negative sign
        BucketIterator it = new MergingBucketIterator(
            positiveBuckets,
            negativeBuckets,
            positiveBuckets.scale(),
            (positiveCount, negativeCount) -> positiveCount - negativeCount
        );
        double sum = 0.0;
        while (it.hasNext()) {
            long countWithSign = it.peekCount();
            double bucketMidPoint = ExponentialScaleUtils.getPointOfLeastRelativeError(it.peekIndex(), it.scale());
            if (countWithSign != 0) { // avoid 0 * INFINITY = NaN
                double toAdd = bucketMidPoint * countWithSign;
                if (Double.isFinite(toAdd)) {
                    sum += toAdd;
                } else {
                    // Avoid NaN in case we end up with e.g. -Infinity+Infinity
                    // we consider the bucket with the bigger index the winner for the sign
                    sum = toAdd;
                }
            }
            it.advance();
        }
        return sum;
    }

    /**
     * Estimates the minimum value of the histogram based on the populated buckets.
     * The returned value is guaranteed to be less than or equal to the exact minimum value of the histogram values.
     * If the histogram is empty, an empty Optional is returned.
     * <p>
     * Note that this method can return +-Infinity if the histogram bucket boundaries are not representable in a double.
     *
     * @param zeroBucket the zero bucket of the histogram
     * @param negativeBuckets the negative buckets of the histogram
     * @param positiveBuckets the positive buckets of the histogram
     * @return the estimated minimum
     */
    public static OptionalDouble estimateMin(
        ZeroBucket zeroBucket,
        ExponentialHistogram.Buckets negativeBuckets,
        ExponentialHistogram.Buckets positiveBuckets
    ) {
        int scale = negativeBuckets.iterator().scale();
        assert scale == positiveBuckets.iterator().scale();

        OptionalLong negativeMaxIndex = negativeBuckets.maxBucketIndex();
        if (negativeMaxIndex.isPresent()) {
            return OptionalDouble.of(-ExponentialScaleUtils.getUpperBucketBoundary(negativeMaxIndex.getAsLong(), scale));
        }

        if (zeroBucket.count() > 0) {
            if (zeroBucket.zeroThreshold() == 0.0) {
                // avoid negative zero
                return OptionalDouble.of(0.0);
            }
            return OptionalDouble.of(-zeroBucket.zeroThreshold());
        }

        BucketIterator positiveBucketsIt = positiveBuckets.iterator();
        if (positiveBucketsIt.hasNext()) {
            return OptionalDouble.of(ExponentialScaleUtils.getLowerBucketBoundary(positiveBucketsIt.peekIndex(), scale));
        }
        return OptionalDouble.empty();
    }

    /**
     * Estimates the maximum value of the histogram based on the populated buckets.
     * The returned value is guaranteed to be greater than or equal to the exact maximum value of the histogram values.
     * If the histogram is empty, an empty Optional is returned.
     * <p>
     * Note that this method can return +-Infinity if the histogram bucket boundaries are not representable in a double.
     *
     * @param zeroBucket the zero bucket of the histogram
     * @param negativeBuckets the negative buckets of the histogram
     * @param positiveBuckets the positive buckets of the histogram
     * @return the estimated minimum
     */
    public static OptionalDouble estimateMax(
        ZeroBucket zeroBucket,
        ExponentialHistogram.Buckets negativeBuckets,
        ExponentialHistogram.Buckets positiveBuckets
    ) {
        int scale = negativeBuckets.iterator().scale();
        assert scale == positiveBuckets.iterator().scale();

        OptionalLong positiveMaxIndex = positiveBuckets.maxBucketIndex();
        if (positiveMaxIndex.isPresent()) {
            return OptionalDouble.of(ExponentialScaleUtils.getUpperBucketBoundary(positiveMaxIndex.getAsLong(), scale));
        }

        if (zeroBucket.count() > 0) {
            return OptionalDouble.of(zeroBucket.zeroThreshold());
        }

        BucketIterator negativeBucketsIt = negativeBuckets.iterator();
        if (negativeBucketsIt.hasNext()) {
            return OptionalDouble.of(-ExponentialScaleUtils.getLowerBucketBoundary(negativeBucketsIt.peekIndex(), scale));
        }
        return OptionalDouble.empty();
    }

    /**
     * Makes sure that if both histograms were generated by merging the same set of histograms,
     * they are equal independent of the order in which the merge occurred.
     * This utility is intended for usage in tests.
     *
     * @param first the first histogram
     * @param second the second histogram
     * @return the adjusted histograms
     */
    public static HistogramPair removeMergeNoise(ExponentialHistogram first, ExponentialHistogram second) {
        int targetScale = Math.min(first.scale(), second.scale());
        ExponentialHistogram a = downscaleTo(first, targetScale);
        ExponentialHistogram b = downscaleTo(second, targetScale);

        double sumError;
        if (Math.abs(b.sum()) < 0.0001) {
            // close to zero, use absolute error
            sumError = Math.abs(a.sum() - b.sum());
        } else {
            // otherwise relative error
            sumError = Math.abs(1 - a.sum() / b.sum());
        }
        if (sumError < 0.01) {
            // if the sum only differs a little (e.g. due to numeric errors), unify it
            double averageSum = (a.sum() + b.sum()) / 2.0;
            a = ExponentialHistogram.builder(a, ExponentialHistogramCircuitBreaker.noop()).sum(averageSum).build();
            b = ExponentialHistogram.builder(b, ExponentialHistogramCircuitBreaker.noop()).sum(averageSum).build();
        }

        // if a zero bucket is involved, the zero-threshold will depend on the merge order
        // as it is dependent on the downscaling order
        // therefore, we have to increase the smaller zero bucket to match the bigger one
        if (a.zeroBucket().count() > 0 && b.zeroBucket().count() > 0) {
            // bring the zero-threshold of both histograms to the same value (the higher one)
            ZeroBucket targetZeroBucket;
            if (a.zeroBucket().compareZeroThreshold(b.zeroBucket()) >= 0) {
                targetZeroBucket = a.zeroBucket();
            } else {
                targetZeroBucket = b.zeroBucket();
            }
            a = increaseZeroThreshold(a, targetZeroBucket);
            b = increaseZeroThreshold(b, targetZeroBucket);
        }

        return new HistogramPair(a, b);
    }

    private static ExponentialHistogram downscaleTo(ExponentialHistogram histogram, int targetScale) {
        assert histogram.scale() >= targetScale;
        int bucketCount = Math.max(4, histogram.positiveBuckets().bucketCount() + histogram.negativeBuckets().bucketCount());
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.createWithMaxScale(
            bucketCount,
            targetScale,
            ExponentialHistogramCircuitBreaker.noop()
        );
        merger.addWithoutUpscaling(histogram);
        return merger.get();
    }

    private static ExponentialHistogram increaseZeroThreshold(ExponentialHistogram histo, ZeroBucket targetZeroBucket) {
        int bucketCount = Math.max(4, histo.positiveBuckets().bucketCount() + histo.negativeBuckets().bucketCount());
        ExponentialHistogramMerger merger = ExponentialHistogramMerger.create(bucketCount, ExponentialHistogramCircuitBreaker.noop());
        merger.addWithoutUpscaling(histo);
        // now add a histogram with only the zero-threshold with a count of 1 to trigger merging of overlapping buckets
        merger.add(
            ExponentialHistogram.builder(ExponentialHistogram.MAX_SCALE, ExponentialHistogramCircuitBreaker.noop())
                .zeroBucket(copyWithNewCount(targetZeroBucket, 1))
                .build()
        );
        // the merger now has the desired zero-threshold, but we need to subtract the fake zero count of 1
        ExponentialHistogram mergeResult = merger.get();
        return ExponentialHistogram.builder(mergeResult, ExponentialHistogramCircuitBreaker.noop())
            .zeroBucket(copyWithNewCount(mergeResult.zeroBucket(), mergeResult.zeroBucket().count() - 1))
            .build();
    }

    private static ZeroBucket copyWithNewCount(ZeroBucket zb, long newCount) {
        if (zb.isIndexBased()) {
            return ZeroBucket.create(zb.index(), zb.scale(), newCount);
        } else {
            return ZeroBucket.create(zb.zeroThreshold(), newCount);
        }
    }

    public record HistogramPair(ExponentialHistogram first, ExponentialHistogram second) {}
}
