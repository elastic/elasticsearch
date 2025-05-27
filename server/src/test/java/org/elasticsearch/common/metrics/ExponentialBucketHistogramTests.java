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

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThrows;

public class ExponentialBucketHistogramTests extends ESTestCase {

    public void testSnapshot() {
        final ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(18);

        assertArrayEquals(new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(0L);
        assertArrayEquals(new long[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(1L);
        assertArrayEquals(new long[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(2L);
        assertArrayEquals(new long[] { 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(3L);
        assertArrayEquals(new long[] { 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(4L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(127L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(128L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, histogram.getSnapshot());

        histogram.addObservation(65535L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0 }, histogram.getSnapshot());

        histogram.addObservation(65536L);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1 }, histogram.getSnapshot());

        histogram.addObservation(Long.MAX_VALUE);
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2 }, histogram.getSnapshot());

        histogram.addObservation(randomLongBetween(65536L, Long.MAX_VALUE));
        assertArrayEquals(new long[] { 1, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, histogram.getSnapshot());

        histogram.addObservation(randomLongBetween(Long.MIN_VALUE, 0L));
        assertArrayEquals(new long[] { 2, 1, 2, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3 }, histogram.getSnapshot());
    }

    public void testHistogramRandom() {
        final int bucketCount = randomIntBetween(5, 20);
        final int[] upperBounds = ExponentialBucketHistogram.getBucketUpperBounds(bucketCount);
        final long[] expectedCounts = new long[upperBounds.length + 1];
        final ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(bucketCount);
        for (int i = between(0, 1000); i > 0; i--) {
            final int bucket = between(0, expectedCounts.length - 1);
            expectedCounts[bucket] += 1;

            final int lowerBound = bucket == 0 ? 0 : upperBounds[bucket - 1];
            final int upperBound = bucket == upperBounds.length
                ? randomBoolean() ? upperBounds[bucket - 1] * 2 : Integer.MAX_VALUE
                : upperBounds[bucket] - 1;
            histogram.addObservation(between(lowerBound, upperBound));
        }

        assertArrayEquals(expectedCounts, histogram.getSnapshot());
    }

    public void testBoundsConsistency() {
        final int bucketCount = randomIntBetween(5, 20);
        final int[] upperBounds = ExponentialBucketHistogram.getBucketUpperBounds(bucketCount);
        assertThat(upperBounds[0], greaterThan(0));
        for (int i = 1; i < upperBounds.length; i++) {
            assertThat(upperBounds[i], greaterThan(upperBounds[i - 1]));
        }
    }

    public void testPercentile() {
        final int bucketCount = randomIntBetween(5, 20);
        ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(bucketCount);
        int valueCount = randomIntBetween(100, 10_000);
        for (int i = 0; i < valueCount; i++) {
            histogram.addObservation(i);
        }
        final long[] snapshot = histogram.getSnapshot();
        final int[] bucketUpperBounds = histogram.calculateBucketUpperBounds();
        for (int i = 0; i <= 100; i++) {
            final float percentile = i / 100.0f;
            final long actualPercentile = (long) Math.ceil(valueCount * percentile);
            final long histogramPercentile = histogram.getPercentile(percentile, snapshot, bucketUpperBounds);
            final String message = Strings.format("%d percentile is %d (actual=%d)", i, histogramPercentile, actualPercentile);
            assertThat(message, histogramPercentile, greaterThanOrEqualTo(actualPercentile));
        }
    }

    public void testMaxPercentile() {
        final int bucketCount = randomIntBetween(5, 20);
        ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(bucketCount);
        int[] bucketUpperBounds = histogram.calculateBucketUpperBounds();
        int secondToLastBucketUpperBound = bucketUpperBounds[bucketUpperBounds.length - 1];
        histogram.addObservation(secondToLastBucketUpperBound + 1);
        assertThat(histogram.getPercentile(1.0f), equalTo(Long.MAX_VALUE));
    }

    public void testClear() {
        final int bucketCount = randomIntBetween(5, 20);
        ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(bucketCount);
        for (int i = 0; i < 100; i++) {
            histogram.addObservation(randomIntBetween(1, 100_000));
        }
        assertThat(Arrays.stream(histogram.getSnapshot()).sum(), greaterThan(0L));
        histogram.clear();
        assertThat(Arrays.stream(histogram.getSnapshot()).sum(), equalTo(0L));
    }

    public void testPercentileValidation() {
        final int bucketCount = randomIntBetween(5, 20);
        ExponentialBucketHistogram histogram = new ExponentialBucketHistogram(bucketCount);
        for (int i = 0; i < 100; i++) {
            histogram.addObservation(randomIntBetween(1, 100_000));
        }
        // valid values
        histogram.getPercentile(randomFloatBetween(0.0f, 1.0f, true));
        histogram.getPercentile(1.0f);
        // invalid values
        assertThrows(
            IllegalArgumentException.class,
            () -> histogram.getPercentile(randomFloatBetween(Float.NEGATIVE_INFINITY, 0.0f, false))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> histogram.getPercentile(randomFloatBetween(1.0f, Float.POSITIVE_INFINITY, false))
        );
    }

    public void testBucketCountValidation() {
        // Valid values
        for (int i = 2; i <= Integer.SIZE; i++) {
            new ExponentialBucketHistogram(i);
        }
        // Invalid values
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBucketHistogram(randomIntBetween(Integer.MIN_VALUE, 1)));
        assertThrows(
            IllegalArgumentException.class,
            () -> new ExponentialBucketHistogram(randomIntBetween(Integer.SIZE, Integer.MAX_VALUE))
        );
    }
}
