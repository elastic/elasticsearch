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
 * Quantile estimation for {@link ExponentialHistogram}s.
 */
public class ExponentialHistogramQuantile {

    /**
     * Provides a quantile for the distribution represented by the given histogram.
     *
     * It returns the value of the element at rank {@code max(0, min( n-1, (quantile * (n+1))-1))}, where rank starts at 0.
     * If that value is fractional, we linearly interpolate based on the fraction the values of the two neighboring ranks.
     *
     * @param histo the histogram representing the distribution
     * @param quantile the quantile to query, in the range [0,1]
     * @return NaN if the histogram is empty, otherwise the quantile
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
            // Can't compute quantile on empty histogram
            return Double.NaN;
        }

        double exactRank = Math.max(0, Math.min(totalCount - 1, (totalCount + 1) * quantile - 1));
        long lowerRank = (long) Math.floor(exactRank);
        long upperRank = (long) Math.ceil(exactRank);
        double upperFactor = exactRank - lowerRank;

        // TODO: if we want more performance here, we could iterate the buckets once instead of twice
        return getElementAtRank(histo, lowerRank, negCount, zeroCount) * ( 1 - upperFactor)
            +getElementAtRank(histo, upperRank, negCount, zeroCount) * upperFactor;
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
        throw new IllegalStateException("buckets contain in total less elements than the desired rank");
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
