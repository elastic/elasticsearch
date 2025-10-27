/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;

import java.util.ArrayList;
import java.util.List;

public class ParsedHistogramConverter {

    /**
     * Converts exponential histograms to t-digests using the very same algorithm as the built-in OTLP metrics endpoint.
     *
     * @param exponential
     * @return
     */
    public static HistogramParser.ParsedHistogram exponentialToTDigest(ExponentialHistogramParser.ParsedExponentialHistogram exponential) {
        // We don't want to reuse the code across the OTLP intake an the field mappers because they use different data models
        // and shuffling the data into a common format or interface would be more expensive and complex than just duplicating the logic.
        List<Double> centroids = new ArrayList<>(); // sorted from descending to ascending
        List<Long> counts = new ArrayList<>();

        List<IndexWithCount> neg = exponential.negativeBuckets();
        for (int i = neg.size() - 1; i >= 0; i--) {
            appendBucketCentroid(centroids, counts, neg.get(i), exponential.scale(), -1);
        }
        if (exponential.zeroCount() > 0) {
            centroids.add(0.0);
            counts.add(exponential.zeroCount());
        }
        for (IndexWithCount positiveBucket : exponential.positiveBuckets()) {
            appendBucketCentroid(centroids, counts, positiveBucket, exponential.scale(), 1);
        }
        assert centroids.size() == counts.size();
        assert centroids.stream().sorted().toList().equals(centroids);
        return new HistogramParser.ParsedHistogram(centroids, counts);
    }

    private static void appendBucketCentroid(
        List<Double> centroids,
        List<Long> counts,
        IndexWithCount expHistoBucket,
        int scale,
        int sign
    ) {
        double lowerBound = ExponentialScaleUtils.getLowerBucketBoundary(expHistoBucket.index(), scale);
        double upperBound = ExponentialScaleUtils.getUpperBucketBoundary(expHistoBucket.index(), scale);
        double center = sign * (lowerBound + upperBound) / 2.0;
        centroids.add(center);
        counts.add(expHistoBucket.count());
    }
}
