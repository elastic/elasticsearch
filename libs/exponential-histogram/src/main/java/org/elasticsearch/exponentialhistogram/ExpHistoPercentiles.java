/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

public class ExpHistoPercentiles {

    public static double getPercentile(ExponentialHistogram histo, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("percentile must be in range [0, 1]");
        }

        long zeroCount = histo.zeroBucket().count();
        long negCount = getTotalCount(histo.negativeBuckets());
        long posCount = getTotalCount(histo.positiveBuckets());

        long totalCount = zeroCount + negCount + posCount;
        if (totalCount == 0) {
            // Can't compute percentile on empty histogram
            return Double.NaN;
        }
        // TODO: Maybe not round, but interpolate between?
        long targetRank = Math.round((totalCount - 1) * percentile);
        if (targetRank < negCount) {
            return -getBucketMidpointForRank(histo.negativeBuckets(), (negCount - 1) - targetRank);
        } else if (targetRank < (negCount + zeroCount)) {
            return 0.0; // we are in the zero bucket
        } else {
            return getBucketMidpointForRank(histo.positiveBuckets(), targetRank - (negCount + zeroCount));
        }
    }

    private static double getBucketMidpointForRank(ExponentialHistogram.BucketIterator buckets, long rank) {
        long seenCount = 0;
        while (buckets.hasNext()) {
            seenCount += buckets.peekCount();
            if (rank < seenCount) {
                return ExponentialHistogramUtils.getPointOfLeastRelativeError(buckets.peekIndex(), buckets.scale());
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
