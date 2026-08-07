/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.exponentialhistogram.TDigestToExponentialHistogramConverter;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.core.analytics.mapper.ExponentialHistogramToTDigestConverter;

import java.util.ArrayList;
import java.util.List;

public class ParsedHistogramConverter {

    /**
     * Converts exponential histograms to t-digests using the very same algorithm as the built-in OTLP metrics endpoint.
     *
     * @param expHisto the exponential histogram to convert
     * @return the resulting t-digest histogram
     */
    public static HistogramParser.ParsedHistogram exponentialToTDigest(ExponentialHistogramParser.ParsedExponentialHistogram expHisto) {
        EncodedTDigest.CentroidIterator centroidIterator = ExponentialHistogramToTDigestConverter.convert(
            IndexWithCount.asBuckets(expHisto.scale(), expHisto.negativeBuckets()),
            ZeroBucket.create(expHisto.zeroThreshold(), expHisto.zeroCount()),
            IndexWithCount.asBuckets(expHisto.scale(), expHisto.positiveBuckets())
        );

        List<Double> centroids = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        while (centroidIterator.next()) {
            centroids.add(centroidIterator.currentMean());
            counts.add(centroidIterator.currentCount());
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

        TDigestToExponentialHistogramConverter.LazyConversion converted = TDigestToExponentialHistogramConverter.convertLazy(
            new TDigestToExponentialHistogramConverter.ArrayBasedCentroidIterator(centroids, counts)
        );

        return new ExponentialHistogramParser.ParsedExponentialHistogram(
            converted.getScale(),
            0.0,
            converted.getZeroCount(),
            IndexWithCount.fromIterator(converted.negativeBuckets()),
            IndexWithCount.fromIterator(converted.positiveBuckets()),
            null, // sum, min, max will be estimated
            null,
            null
        );
    }

}
