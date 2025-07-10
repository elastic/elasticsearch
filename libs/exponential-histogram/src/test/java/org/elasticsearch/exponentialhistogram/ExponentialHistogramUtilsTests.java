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

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.adjustScale;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.compareLowerBoundaries;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getLowerBucketBoundary;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getMaximumScaleIncrease;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getUpperBucketBoundary;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramUtilsTests extends ESTestCase {

    public void testMaxValue() {
        assertThat(getMaximumScaleIncrease(MAX_INDEX), equalTo(0));
        assertThat(getMaximumScaleIncrease(MAX_INDEX >> 1), equalTo(1));

        assertThrows(ArithmeticException.class, () -> Math.multiplyExact(MAX_INDEX, 4));
    }

    public void testMinValue() {
        assertThat(getMaximumScaleIncrease(MIN_INDEX), equalTo(0));
        assertThat(getMaximumScaleIncrease(MIN_INDEX >> 1), equalTo(1));
    }

    public void testRandomIndicesScaleAdjustement() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            long index = rnd.nextLong() % MAX_INDEX;
            int maxScale = getMaximumScaleIncrease(index);

            assertThat(adjustScale(adjustScale(index, maxScale), -maxScale), equalTo(index));
            if (index >0) {
                assertThat(adjustScale(index, maxScale) *2, greaterThan(MAX_INDEX));
            } else {
                assertThat(adjustScale(index, maxScale) *2, lessThan(MIN_INDEX));

            }
        }

    }

    public void testRandomBucketBoundaryComparison() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            long indexA = rnd.nextLong() % MAX_INDEX;
            long indexB = rnd.nextLong() % MAX_INDEX;
            int scaleA = rnd.nextInt() % MAX_SCALE;
            int scaleB = rnd.nextInt() % MAX_SCALE;

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

        double limit1 = getLowerBucketBoundary(MIN_INDEX, MAX_SCALE);
        double limit2 = getLowerBucketBoundary(MIN_INDEX, MAX_SCALE);
        assertThat(limit1, lessThanOrEqualTo(limit2));
    }
}
