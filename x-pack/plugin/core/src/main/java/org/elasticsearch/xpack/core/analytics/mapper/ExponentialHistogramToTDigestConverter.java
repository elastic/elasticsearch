/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExponentialHistogramToTDigestConverter {

    public static EncodedTDigest.CentroidIterator convert(int scale, BucketIterator negative, ZeroBucket zeroBucket, BucketIterator positive) {
        List<Double> negativeBucketCenters;
        List<Long> negativeBucketCounts;
        if (negative.hasNext()) {
            negativeBucketCenters = new ArrayList<>();
            negativeBucketCounts = new ArrayList<>();
            while (negative.hasNext()) {
                negativeBucketCenters.add(-getBucketCenter(negative.peekIndex(), scale));
                negativeBucketCounts.add(negative.peekCount());
                negative.advance();
            }
            // negative buckets are now sorted from 0 to -INF, we want them to be sorted in ascending order instead
            Collections.reverse(negativeBucketCenters);
            Collections.reverse(negativeBucketCounts);
        } else {
            negativeBucketCenters = Collections.emptyList();
            negativeBucketCounts = Collections.emptyList();
        }

        return new EncodedTDigest.CentroidIterator() {

            double currentMean;
            long currentCount;

            int nextNegativeBucketIndex = 0;
            boolean zeroBucketConsumed = false;

            private void moveToNextCentroid() {
                currentCount = 0;
                while (nextNegativeBucketIndex < negativeBucketCenters.size()) {
                    if (currentCount > 0 && currentMean != negativeBucketCenters.get(nextNegativeBucketIndex)) {
                        break;
                    }
                    currentMean = negativeBucketCenters.get(nextNegativeBucketIndex);
                    currentCount += negativeBucketCounts.get(nextNegativeBucketIndex);
                    nextNegativeBucketIndex++;
                }
                if (zeroBucketConsumed == false && zeroBucket.count() > 0) {
                    if (currentCount == 0 || currentMean == 0.0d) {
                        currentMean = 0.0;
                        currentCount += zeroBucket.count();
                        zeroBucketConsumed = true;
                    }
                }
                while (positive.hasNext()) {
                    double center = getBucketCenter(positive.peekIndex(), scale);
                    if (currentCount > 0 && currentMean != center) {
                        break;
                    }
                    currentMean = center;
                    currentCount += positive.peekCount();
                    positive.advance();
                }
                if (currentMean == -0.0) {
                    // avoid negative zero
                    currentMean = 0.0;
                }
            }

            @Override
            public boolean next() {
                moveToNextCentroid();
                return currentCount > 0;
            }

            @Override
            public long currentCount() {
                assert currentCount > 0 : "next() must be called and return true before accessing current centroid";
                return currentCount;
            }

            @Override
            public double currentMean() {
                assert currentCount > 0 : "next() must be called and return true before accessing current centroid";
                return currentMean;
            }

            @Override
            public boolean hasNext() {
                return nextNegativeBucketIndex < negativeBucketCenters.size()
                    || (zeroBucketConsumed == false && zeroBucket.count() > 0)
                    || positive.hasNext();
            }
        };
    }

    private static double getBucketCenter(long index, int scale) {
        double lowerBound = ExponentialScaleUtils.getLowerBucketBoundary(index, scale);
        double upperBound = ExponentialScaleUtils.getUpperBucketBoundary(index, scale);
        return (lowerBound + upperBound) / 2.0;
    }

}
