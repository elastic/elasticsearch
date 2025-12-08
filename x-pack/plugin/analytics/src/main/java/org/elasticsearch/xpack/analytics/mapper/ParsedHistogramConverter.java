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

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;

public class ParsedHistogramConverter {

    /**
     * Converts exponential histograms to t-digests using the very same algorithm as the built-in OTLP metrics endpoint.
     *
     * @param expHisto the exponential histogram to convert
     * @return the resulting t-digest histogram
     */
    public static HistogramParser.ParsedHistogram exponentialToTDigest(ExponentialHistogramParser.ParsedExponentialHistogram expHisto) {
        // We don't want to reuse the code across the OTLP intake an the field mappers because they use different data models
        // and shuffling the data into a common format or interface would be more expensive and complex than just duplicating the logic.
        List<Double> centroids = new ArrayList<>(); // sorted from descending to ascending
        List<Long> counts = new ArrayList<>();

        List<IndexWithCount> neg = expHisto.negativeBuckets();
        for (int i = neg.size() - 1; i >= 0; i--) {
            appendBucketCentroid(centroids, counts, neg.get(i), expHisto.scale(), -1);
        }
        if (expHisto.zeroCount() > 0) {
            centroids.add(0.0);
            counts.add(expHisto.zeroCount());
        }
        for (IndexWithCount positiveBucket : expHisto.positiveBuckets()) {
            appendBucketCentroid(centroids, counts, positiveBucket, expHisto.scale(), 1);
        }
        assert centroids.size() == counts.size();
        assert centroids.stream().sorted().toList().equals(centroids);
        return new HistogramParser.ParsedHistogram(centroids, counts);
    }

    /**
     * Converts t-digest histograms to exponential histograms, trying to do the inverse
     * of {@link #exponentialToTDigest(ExponentialHistogramParser.ParsedExponentialHistogram)}
     * as accurately as possible.
     * <br>
     * On a round-trip conversion from exponential histogram to T-Digest and back,
     * the bucket centers will be preserved, however the bucket widths are lost.
     * The conversion algorithm works by generating tiny buckets (scale set to MAX_SCALE)
     * containing the T-Digest centroids.
     *
     * @param tDigest the t-digest histogram to convert
     * @return the resulting exponential histogram
     */
    public static ExponentialHistogramParser.ParsedExponentialHistogram tDigestToExponential(HistogramParser.ParsedHistogram tDigest) {
        List<Double> centroids = tDigest.values();
        List<Long> counts = tDigest.counts();

        int numNegativeCentroids = 0;
        while (numNegativeCentroids < centroids.size() && centroids.get(numNegativeCentroids) < 0) {
            numNegativeCentroids++;
        }

        // iterate negative centroids from closest to zero to furthest away,
        // which corresponds to ascending exponential histogram bucket indices
        int scale = MAX_SCALE;
        List<IndexWithCount> negativeBuckets = new ArrayList<>();
        for (int i = numNegativeCentroids - 1; i >= 0; i--) {
            double centroid = centroids.get(i);
            long count = counts.get(i);
            assert centroid < 0;
            appendCentroidWithCountAsBucket(centroid, count, scale, negativeBuckets);
        }

        long zeroCount = 0;
        int firstPositiveIndex = numNegativeCentroids;
        if (firstPositiveIndex < centroids.size() && centroids.get(firstPositiveIndex) == 0) {
            // we have a zero-centroid, which we'll map to the zero bucket
            zeroCount = counts.get(firstPositiveIndex);
            firstPositiveIndex++;
        }

        List<IndexWithCount> positiveBuckets = new ArrayList<>();
        for (int i = firstPositiveIndex; i < centroids.size(); i++) {
            double centroid = centroids.get(i);
            long count = counts.get(i);
            assert centroid > 0;
            appendCentroidWithCountAsBucket(centroid, count, scale, positiveBuckets);
        }

        return new ExponentialHistogramParser.ParsedExponentialHistogram(
            scale,
            0.0,
            zeroCount,
            negativeBuckets,
            positiveBuckets,
            null, // sum, min, max will be estimated
            null,
            null
        );
    }

    private static void appendCentroidWithCountAsBucket(double centroid, long count, int scale, List<IndexWithCount> outputBuckets) {
        if (count == 0) {
            return; // zero counts are allowed in T-Digests but not in exponential histograms
        }
        long index = ExponentialScaleUtils.computeIndex(centroid, scale);
        assert outputBuckets.isEmpty() || outputBuckets.getLast().index() < index;
        outputBuckets.add(new IndexWithCount(index, count));
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
        // the index + scale representation is higher precision than the centroid representation,
        // so we can have multiple exp histogram buckets map to the same centroid.
        if (centroids.isEmpty() == false && centroids.getLast() == center) {
            counts.add(counts.removeLast() + expHistoBucket.count());
        } else {
            centroids.add(center);
            counts.add(expHistoBucket.count());
        }
    }
}
