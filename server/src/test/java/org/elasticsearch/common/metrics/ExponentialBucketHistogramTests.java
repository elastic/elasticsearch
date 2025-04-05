/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;

public class ExponentialBucketHistogramTests extends ESTestCase {

    public void testHistogram() {
        final ExponentialBucketHistogram histogram = new ExponentialBucketHistogram();

        assertArrayEquals(new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(0L);
        assertArrayEquals(new long[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(1L);
        assertArrayEquals(new long[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(2L);
        assertArrayEquals(new long[] { 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(3L);
        assertArrayEquals(new long[] { 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(4L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(127L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(128L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getHistogram());

        histogram.addObservation(65535L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0 }, histogram.getHistogram());

        histogram.addObservation(65536L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1 }, histogram.getHistogram());

        histogram.addObservation(Long.MAX_VALUE);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2 }, histogram.getHistogram());

        histogram.addObservation(randomLongBetween(65536L, Long.MAX_VALUE));
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, histogram.getHistogram());

        histogram.addObservation(randomLongBetween(Long.MIN_VALUE, 0L));
        assertArrayEquals(new long[] { 2, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, histogram.getHistogram());
    }

    public void testHistogramRandom() {
        final int[] upperBounds = ExponentialBucketHistogram.getBucketUpperBounds();
        final long[] expectedCounts = new long[upperBounds.length + 1];
        final ExponentialBucketHistogram histogram = new ExponentialBucketHistogram();
        for (int i = between(0, 1000); i > 0; i--) {
            final int bucket = between(0, expectedCounts.length - 1);
            expectedCounts[bucket] += 1;

            final int lowerBound = bucket == 0 ? 0 : upperBounds[bucket - 1];
            final int upperBound = bucket == upperBounds.length ? randomBoolean() ? 100000 : Integer.MAX_VALUE : upperBounds[bucket] - 1;
            histogram.addObservation(between(lowerBound, upperBound));
        }

        assertArrayEquals(expectedCounts, histogram.getHistogram());
    }

    public void testBoundsConsistency() {
        final int[] upperBounds = ExponentialBucketHistogram.getBucketUpperBounds();
        assertThat(upperBounds[0], greaterThan(0));
        for (int i = 1; i < upperBounds.length; i++) {
            assertThat(upperBounds[i], greaterThan(upperBounds[i - 1]));
        }
    }

    public void testPercentile() {
        ExponentialBucketHistogram histogram = new ExponentialBucketHistogram();
        int valueCount = randomIntBetween(100, 10_000);
        for (int i = 0; i < valueCount; i++) {
            histogram.addObservation(i);
        }
        for (int i = 0; i <= 100; i++) {
            final float percentile = i / 100.0f;
            final long actualPercentile = (long) Math.ceil(valueCount * percentile);
            final long histogramPercentile = histogram.getPercentile(percentile);
            final String message = Strings.format("%d percentile is %d (actual=%d)", i, histogramPercentile, actualPercentile);
            assertThat(message, histogramPercentile, greaterThan(actualPercentile));
        }
    }
}
