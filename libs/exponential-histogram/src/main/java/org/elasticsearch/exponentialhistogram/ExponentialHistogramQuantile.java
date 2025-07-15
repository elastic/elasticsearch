/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

/**
 * Provides quantile estimation for {@link ExponentialHistogram} instances.
 */
public class ExponentialHistogramQuantile {

    /**
     * Estimates a quantile for the distribution represented by the given histogram.
     *
     * It returns the value of the element at rank {@code max(0, min(n - 1, (quantile * (n + 1)) - 1))}, where n is the total number of
     * values and rank starts at 0. If the rank is fractional, the result is linearly interpolated from the values of the two
     * neighboring ranks.
     *
     * @param histo    the histogram representing the distribution
     * @param quantile the quantile to query, in the range [0, 1]
     * @return the estimated quantile value, or {@link Double#NaN} if the histogram is empty
     */
    public static double getQuantile(ExponentialHistogram histo, double quantile) {
        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("quantile must be in range [0, 1]");
        }

        long zeroCount = histo.zeroBucket().count();
        long negCount = getTotalCount(histo.negativeBuckets());
        long posCount = getTotalCount(histo.positiveBuckets());

        long totalCount = zeroCount + negCount + posCount;
        if (totalCount == 0) {
            // Can't compute quantile on an empty histogram
            return Double.NaN;
        }

        double exactRank = Math.max(0, Math.min(totalCount - 1, (totalCount + 1) * quantile - 1));
        long lowerRank = (long) Math.floor(exactRank);
        long upperRank = (long) Math.ceil(exactRank);
        double upperFactor = exactRank - lowerRank;

        // TODO: This can be optimized to iterate over the buckets once instead of twice.
        return getElementAtRank(histo, lowerRank, negCount, zeroCount) * (1 - upperFactor) + getElementAtRank(
            histo,
            upperRank,
            negCount,
            zeroCount
        ) * upperFactor;
    }

    private static double getElementAtRank(ExponentialHistogram histo, long rank, long negCount, long zeroCount) {
        if (rank < negCount) {
            return -getBucketMidpointForRank(histo.negativeBuckets(), (negCount - 1) - rank);
        } else if (rank < (negCount + zeroCount)) {
            return 0.0;
        } else {
            return getBucketMidpointForRank(histo.positiveBuckets(), rank - (negCount + zeroCount));
        }
    }

    private static double getBucketMidpointForRank(ExponentialHistogram.BucketIterator buckets, long rank) {
        long seenCount = 0;
        while (buckets.hasNext()) {
            seenCount += buckets.peekCount();
            if (rank < seenCount) {
                return ExponentialScaleUtils.getPointOfLeastRelativeError(buckets.peekIndex(), buckets.scale());
            }
            buckets.advance();
        }
        throw new IllegalStateException("The total number of elements in the buckets is less than the desired rank.");
    }

    private static long getTotalCount(ExponentialHistogram.BucketIterator buckets) {
        long count = 0;
        while (buckets.hasNext()) {
            count += buckets.peekCount();
            buckets.advance();
        }
        return count;
    }
}
