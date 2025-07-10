/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import java.util.Random;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.adjustScale;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.compareLowerBoundaries;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getLowerBucketBoundary;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getMaximumScaleIncrease;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getUpperBucketBoundary;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramUtilsTest extends ESTestCase {

    public void testMaxValue() {
        assertThat(getMaximumScaleIncrease(Long.MAX_VALUE), equalTo(0));
        assertThat(getMaximumScaleIncrease(Long.MAX_VALUE >> 1), equalTo(1));

        assertThat(adjustScale(Long.MAX_VALUE, 0), equalTo(Long.MAX_VALUE));
        assertThat(adjustScale(Long.MAX_VALUE >> 1, 1), equalTo(Long.MAX_VALUE - 1));
        assertThat(adjustScale(Long.MAX_VALUE >> 2, 2), equalTo((Long.MAX_VALUE & ~3) + 1));
        assertThat(adjustScale(Long.MAX_VALUE >> 4, 4), equalTo((Long.MAX_VALUE & ~15) + 6));
    }

    public void testMinValue() {
        assertThat(getMaximumScaleIncrease(Long.MIN_VALUE), equalTo(0));
        assertThat(getMaximumScaleIncrease(Long.MIN_VALUE >> 1), equalTo(1));

        assertThat(adjustScale(Long.MIN_VALUE, 0), equalTo(Long.MIN_VALUE));
        assertThat(adjustScale(Long.MIN_VALUE >> 1, 1), equalTo(Long.MIN_VALUE));
        assertThat(adjustScale(Long.MIN_VALUE >> 2, 2), equalTo((Long.MIN_VALUE & ~3) + 1));
        assertThat(adjustScale(Long.MIN_VALUE >> 4, 4), equalTo((Long.MIN_VALUE & ~15) + 6));
    }

    public void testRandom() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            long index = rnd.nextLong();
            int maxScale = getMaximumScaleIncrease(index);

            assertThat(adjustScale(adjustScale(index, maxScale), -maxScale), equalTo(index));
            assertThrows(ArithmeticException.class, () -> Math.multiplyExact(adjustScale(index, maxScale), 2));
        }

    }

    public void testRandomComparison() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            long indexA = rnd.nextLong();
            long indexB = rnd.nextLong();
            int scaleA = rnd.nextInt() % 40;
            int scaleB = rnd.nextInt() % 40;

            double lowerBoundA = getLowerBucketBoundary(indexA, scaleA);
            while (Double.isInfinite(lowerBoundA)) {
                indexA = indexA >> 1;
                lowerBoundA = getLowerBucketBoundary(indexA, scaleA);
            }
            double lowerBoundB = getLowerBucketBoundary(indexB, scaleB);
            while (Double.isInfinite(lowerBoundB)) {
                indexB = indexB >> 1;
                lowerBoundB = getLowerBucketBoundary(indexB, scaleB);
            }

            if (lowerBoundA != lowerBoundB) {
                System.out.println("Comparing " + lowerBoundA + " to " + lowerBoundB);
                assertThat(Double.compare(lowerBoundA, lowerBoundB), equalTo(compareLowerBoundaries(indexA, scaleA, indexB, scaleB)));
            }
        }

    }

    public void testScalingUpToMidpoint() {
        long midpointIndex = adjustScale(0, 64);
        double lowerBoundary = getLowerBucketBoundary(midpointIndex, 64);
        double upperBoundary = getUpperBucketBoundary(midpointIndex, 64);

        // due to limited double-float precision the results are actually exact
        assertThat(lowerBoundary, equalTo(4.0 / 3.0));
        assertThat(upperBoundary, equalTo(4.0 / 3.0));
    }

    public void testSaneBucketBoundaries() {
        assertThat(getLowerBucketBoundary(0, 42), equalTo(1.0));
        assertThat(getLowerBucketBoundary(1, 0), equalTo(2.0));
        assertThat(getLowerBucketBoundary(1, -1), equalTo(4.0));
        assertThat(getLowerBucketBoundary(1, -2), equalTo(16.0));

        double limit1 = getLowerBucketBoundary(Long.MAX_VALUE - 1, 56);
        double limit2 = getLowerBucketBoundary(Long.MAX_VALUE, 56);
        assertThat(limit1, lessThanOrEqualTo(limit2));
    }
}
