/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramQuantile;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestCase;
import org.elasticsearch.exponentialhistogram.QuantileAccuracyTests;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.TDigest;
import org.elasticsearch.tdigest.arrays.TDigestArrays;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.DoubleFunction;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramConverterAccuracyTests extends ExponentialHistogramTestCase {
    public static final double[] QUANTILES_TO_TEST = { 0, 0.0000001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999999, 1.0 };

    private static final TDigestArrays arrays = new MemoryTrackingTDigestArrays(new NoopCircuitBreaker("default-wrapper-tdigest-arrays"));

    public void testExponentialHistogramConversionAccuracy() {
        double[] samples = QuantileAccuracyTests.generateSamples(
            new NormalDistribution(new Well19937c(randomInt()), 100, 15),
            between(1_000, 50_000)
        );
        ExponentialHistogram exponentialHistogram = createAutoReleasedHistogram(100, samples);
        TDigest rawTDigest = TDigest.createAvlTreeDigest(arrays, 100);
        for (double sample : samples) {
            rawTDigest.add(sample);
        }

        ExponentialHistogramDataPoint otlpHistogram = convertToOtlpHistogram(exponentialHistogram);
        TDigest convertedTDigest = convertToTDigest(otlpHistogram);

        Arrays.sort(samples);
        getMaxRelativeError(samples, q -> ExponentialHistogramQuantile.getQuantile(exponentialHistogram, q));
        double exponentialHistogramMaxError = getMaxRelativeError(
            samples,
            q -> ExponentialHistogramQuantile.getQuantile(exponentialHistogram, q)
        );
        double rawTDigestMaxError = getMaxRelativeError(samples, rawTDigest::quantile);
        double convertedTDigestMaxError = getMaxRelativeError(samples, convertedTDigest::quantile);
        double combinedRelativeError = rawTDigestMaxError + exponentialHistogramMaxError;
        assertThat(convertedTDigestMaxError, lessThanOrEqualTo(combinedRelativeError * 2));
    }

    private static TDigest convertToTDigest(ExponentialHistogramDataPoint otlpHistogram) {
        TDigest result = TDigest.createAvlTreeDigest(arrays, 100);
        List<Double> centroidValues = new ArrayList<>();
        HistogramConverter.centroidValues(otlpHistogram, centroidValues::add);
        List<Long> counts = new ArrayList<>();
        HistogramConverter.counts(otlpHistogram, counts::add);
        assertEquals(centroidValues.size(), counts.size());
        for (int i = 0; i < centroidValues.size(); i++) {
            if (counts.get(i) > 0) {
                result.add(centroidValues.get(i), counts.get(i));
            }
        }
        return result;
    }

    private ExponentialHistogramDataPoint convertToOtlpHistogram(ExponentialHistogram histogram) {
        ExponentialHistogramDataPoint.Builder builder = ExponentialHistogramDataPoint.newBuilder();
        builder.setScale(histogram.scale());
        builder.setZeroCount(histogram.zeroBucket().count());
        builder.setZeroThreshold(histogram.zeroBucket().zeroThreshold());
        builder.setPositive(convertBuckets(histogram.positiveBuckets()));
        builder.setNegative(convertBuckets(histogram.negativeBuckets()));
        return builder.build();
    }

    private static ExponentialHistogramDataPoint.Buckets.Builder convertBuckets(ExponentialHistogram.Buckets buckets) {
        ExponentialHistogramDataPoint.Buckets.Builder result = ExponentialHistogramDataPoint.Buckets.newBuilder();
        BucketIterator it = buckets.iterator();
        if (it.hasNext() == false) {
            return result;
        }
        result.setOffset((int) it.peekIndex());
        for (long index = it.peekIndex(); it.hasNext(); index++) {
            int missingBuckets = (int) (it.peekIndex() - index);
            for (int i = 0; i < missingBuckets; i++) {
                result.addBucketCounts(0);
                index++;
            }
            result.addBucketCounts(it.peekCount());
            it.advance();
        }
        return result;
    }

    private double getMaxRelativeError(double[] values, DoubleFunction<Double> quantileFunction) {
        double maxError = 0;
        // Compare histogram quantiles with exact quantiles
        for (double q : QUANTILES_TO_TEST) {
            double percentileRank = q * (values.length - 1);
            int lowerRank = (int) Math.floor(percentileRank);
            int upperRank = (int) Math.ceil(percentileRank);
            double upperFactor = percentileRank - lowerRank;

            if (values[lowerRank] < 0 && values[upperRank] > 0) {
                // the percentile lies directly between a sign change and we interpolate linearly in-between
                // in this case the relative error bound does not hold
                continue;
            }
            double exactValue = values[lowerRank] * (1 - upperFactor) + values[upperRank] * upperFactor;

            double histoValue = quantileFunction.apply(q);

            // Skip comparison if exact value is close to zero to avoid false-positives due to numerical imprecision
            if (Math.abs(exactValue) < 1e-100) {
                continue;
            }

            double relativeError = Math.abs(histoValue - exactValue) / Math.abs(exactValue);
            maxError = Math.max(maxError, relativeError);
        }
        return maxError;
    }

}
