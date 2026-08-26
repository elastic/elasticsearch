/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.TDigestToExponentialHistogramConverter;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramToTDigestConverterTests extends ESTestCase {

    public void testExponentialHistogramRoundTrip() {
        double[] values = randomDoubles().limit(randomIntBetween(1, 500))
            .map(val -> randomDouble() < 0.05 ? 0 : val)
            .map(val -> val * 10_000)
            .toArray();
        try (
            ReleasableExponentialHistogram input = ExponentialHistogram.create(
                randomIntBetween(20, 200),
                ExponentialHistogramCircuitBreaker.noop(),
                values
            )
        ) {
            EncodedTDigest.CentroidIterator tDigest = ExponentialHistogramToTDigestConverter.convert(
                input.negativeBuckets(),
                input.zeroBucket(),
                input.positiveBuckets()
            );

            List<Double> centroidValues = new ArrayList<>();
            List<Long> centroidCounts = new ArrayList<>();
            while (tDigest.next()) {
                centroidValues.add(tDigest.currentMean());
                centroidCounts.add(tDigest.currentCount());
            }

            try (
                ReleasableExponentialHistogram output = TDigestToExponentialHistogramConverter.convert(
                    new TDigestToExponentialHistogramConverter.ArrayBasedCentroidIterator(centroidValues, centroidCounts),
                    ExponentialHistogramCircuitBreaker.noop()
                )
            ) {
                assertThat(output.zeroBucket().count(), equalTo(input.zeroBucket().count()));
                assertBucketCentersClose(input.negativeBuckets().iterator(), output.negativeBuckets().iterator());
                assertBucketCentersClose(input.positiveBuckets().iterator(), output.positiveBuckets().iterator());
            }
        }
    }

    public void testToTDigestConversionMergesCentroids() {
        ExponentialHistogram input = ExponentialHistogram.builder(ExponentialHistogram.MAX_SCALE, ExponentialHistogramCircuitBreaker.noop())
            .setNegativeBucket(ExponentialHistogram.MIN_INDEX, 1)
            .setNegativeBucket(ExponentialHistogram.MIN_INDEX + 1, 2)
            .setPositiveBucket(ExponentialHistogram.MIN_INDEX, 3)
            .setPositiveBucket(ExponentialHistogram.MIN_INDEX + 1, 4)
            .zeroBucket(ZeroBucket.create(0.0, 5))
            .build();

        EncodedTDigest.CentroidIterator converted = ExponentialHistogramToTDigestConverter.convert(
            input.negativeBuckets(),
            input.zeroBucket(),
            input.positiveBuckets()
        );

        assertThat(converted.next(), equalTo(true));
        assertThat(converted.currentMean(), equalTo(0.0));
        assertThat(converted.currentCount(), equalTo(15L));
        assertThat(converted.next(), equalTo(false));
    }

    private static void assertBucketCentersClose(BucketIterator originalBuckets, BucketIterator convertedBuckets) {
        while (convertedBuckets.hasNext()) {
            assertThat(originalBuckets.hasNext(), equalTo(true));

            double originalCenter = (ExponentialScaleUtils.getLowerBucketBoundary(originalBuckets.peekIndex(), originalBuckets.scale())
                + ExponentialScaleUtils.getUpperBucketBoundary(originalBuckets.peekIndex(), originalBuckets.scale())) / 2.0;
            double convertedCenter = (ExponentialScaleUtils.getLowerBucketBoundary(convertedBuckets.peekIndex(), convertedBuckets.scale())
                + ExponentialScaleUtils.getUpperBucketBoundary(convertedBuckets.peekIndex(), convertedBuckets.scale())) / 2.0;
            double relativeError = Math.abs(convertedCenter - originalCenter) / Math.abs(originalCenter);

            assertThat(
                "original center=" + originalCenter + ", converted center=" + convertedCenter + ", relative error=" + relativeError,
                relativeError,
                closeTo(0, 0.0000001)
            );
            assertThat(convertedBuckets.peekCount(), equalTo(originalBuckets.peekCount()));

            originalBuckets.advance();
            convertedBuckets.advance();
        }
        assertThat(originalBuckets.hasNext(), equalTo(false));
    }

}
