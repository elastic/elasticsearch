/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import ch.obermuhlner.math.big.BigDecimalMath;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Random;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.SCALE_UP_CONSTANT_TABLE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.adjustScale;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareLowerBoundaries;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getLowerBucketBoundary;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getMaximumScaleIncrease;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getPointOfLeastRelativeError;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getUpperBucketBoundary;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialScaleUtilsTests extends ESTestCase {

    public void testMaxIndex() {
        assertThat(getMaximumScaleIncrease(MAX_INDEX), equalTo(0));
        assertThat(getMaximumScaleIncrease(MAX_INDEX >> 1), equalTo(1));
        assertThrows(ArithmeticException.class, () -> Math.multiplyExact(MAX_INDEX, 4));
    }

    public void testMinIndex() {
        assertThat(getMaximumScaleIncrease(MIN_INDEX), equalTo(0));
        assertThat(getMaximumScaleIncrease(MIN_INDEX >> 1), equalTo(1));
        assertThrows(ArithmeticException.class, () -> Math.multiplyExact(MIN_INDEX, 4));
    }

    public void testExtremeValueIndexing() {
        double leeway = Math.pow(10.0, 20);

        for (double testValue : new double[] { Double.MAX_VALUE / leeway, Double.MIN_VALUE * leeway }) {
            long idx = computeIndex(testValue, MAX_SCALE);
            double lowerBound = getLowerBucketBoundary(idx, MAX_SCALE);
            double upperBound = getUpperBucketBoundary(idx, MAX_SCALE);
            assertThat(lowerBound, lessThanOrEqualTo(testValue));
            assertThat(upperBound, greaterThanOrEqualTo(testValue));
            assertThat(lowerBound, lessThan(upperBound));
        }
    }

    public void testRandomValueIndexing() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            // generate values in the range 10^-100 to 10^100
            double exponent = rnd.nextDouble() * 200 - 100;
            double testValue = Math.pow(10, exponent);
            int scale = rnd.nextInt(MIN_SCALE/2, MAX_SCALE/2);
            long index = computeIndex(testValue, scale);

            double lowerBound = getLowerBucketBoundary(index, scale);
            double upperBound = getUpperBucketBoundary(index, scale);
            double pointOfLeastError = getPointOfLeastRelativeError(index, scale);

            String baseMsg = " for input value " + testValue + " and scale " + scale;

            assertThat("Expected lower bound to be less than input value", lowerBound, lessThanOrEqualTo(testValue));
            assertThat("Expected upper bound to be greater than input value", upperBound, greaterThanOrEqualTo(upperBound));
            assertThat("Expected lower bound to be less than upper bound" + baseMsg, lowerBound, lessThan(upperBound));

            // only do this check for ranges where we have enough numeric stability
            if (lowerBound > Math.pow(10, -250) && upperBound < Math.pow(10, 250)) {

                assertThat(
                    "Expected point of least error to be greater than lower bound" + baseMsg,
                    pointOfLeastError,
                    greaterThan(lowerBound)
                );
                assertThat("Expected point of least error to be less than upper bound" + baseMsg, pointOfLeastError, lessThan(upperBound));

                double errorLower = (pointOfLeastError - lowerBound) / lowerBound;
                double errorUpper = (upperBound - pointOfLeastError) / upperBound;
                assertThat(errorLower / errorUpper, closeTo(1, 0.1));
            }

        }
    }

    public void testRandomIndicesScaleAdjustement() {
        Random rnd = new Random(42);

        for (int i = 0; i < 100_000; i++) {
            long index = rnd.nextLong(MAX_INDEX);
            int currentScale = rnd.nextInt(MIN_SCALE, MAX_SCALE);
            int maxAdjustment = getMaximumScaleIncrease(index);

            assertThat(
                adjustScale(adjustScale(index, currentScale, maxAdjustment), currentScale + maxAdjustment, -maxAdjustment),
                equalTo(index)
            );
            if (index > 0) {
                assertThat(adjustScale(index, currentScale, maxAdjustment) * 2, greaterThan(MAX_INDEX));
            } else {
                assertThat(adjustScale(index, currentScale, maxAdjustment) * 2, lessThan(MIN_INDEX));

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

    public void testUpscalingAccuracy() {
        Random rnd = new Random(42);

        // Use slightly adjusted scales to not run into numeric trouble, because we don't use exact maths here
        int minScale = MIN_SCALE + 7;
        int maxScale = MAX_SCALE - 15;

        for (int i = 0; i < 10_000; i++) {

            int startScale = rnd.nextInt(minScale, maxScale);
            int scaleIncrease =  rnd.nextInt(1, maxScale-startScale + 1);

            long index = MAX_INDEX >> scaleIncrease >> (int) (rnd.nextDouble() * (MAX_INDEX_BITS - scaleIncrease));
            index = Math.max(1, index);
            index = (long) (rnd.nextDouble() * index) * (rnd.nextBoolean() ? 1 : -1);


            double midPoint = getPointOfLeastRelativeError(index, startScale);
            // limit the numeric range, otherwise we get rounding errors causing the test to fail
            while (midPoint > Math.pow(10, 10) || midPoint < Math.pow(10, -10)) {
                index /= 2;
                midPoint = getPointOfLeastRelativeError(index, startScale);
            }

            long scaledUpIndex = adjustScale(index, startScale, scaleIncrease);
            long correctIdx = computeIndex(midPoint, startScale + scaleIncrease);
            // Due to rounding problems in the tests, we can still be off by one for extreme scales
            assertThat(scaledUpIndex, equalTo(correctIdx));
        }
    }

    public void testScaleUpTableUpToDate() {

        MathContext mc = new MathContext(200);
        BigDecimal one = new BigDecimal(1, mc);
        BigDecimal two = new BigDecimal(2, mc);

        for (int scale = MIN_SCALE; scale <= MAX_SCALE; scale++) {
            BigDecimal base = BigDecimalMath.pow(two, two.pow(-scale, mc), mc);
            BigDecimal factor = one.add(two.pow(scale, mc).multiply(one.subtract(BigDecimalMath.log2(one.add(base), mc))));
            assertThat(SCALE_UP_CONSTANT_TABLE[scale - MIN_SCALE], equalTo(factor.doubleValue()));

        }
    }

}
