/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import ch.obermuhlner.math.big.BigDecimalMath;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.SCALE_UP_CONSTANT_TABLE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.adjustScale;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareExponentiallyScaledValues;
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
        assertThat(getMaximumScaleIncrease(MAX_INDEX - 1), equalTo(0));
        assertThat(getMaximumScaleIncrease(MAX_INDEX >> 1), equalTo(1));
        assertThrows(ArithmeticException.class, () -> Math.multiplyExact(MAX_INDEX, 4));
    }

    public void testMinIndex() {
        assertThat(getMaximumScaleIncrease(MIN_INDEX), equalTo(0));
        assertThat(getMaximumScaleIncrease(MIN_INDEX + 1), equalTo(0));
        assertThat(getMaximumScaleIncrease(MIN_INDEX >> 1), equalTo(0));
        assertThat(getMaximumScaleIncrease((MIN_INDEX + 1) >> 1), equalTo(1));
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

    public void testPointOfLeastErrorAtInfinity() {
        assertThat(getPointOfLeastRelativeError(0, MIN_SCALE), equalTo(Double.POSITIVE_INFINITY));
        assertThat(getPointOfLeastRelativeError(-1, MIN_SCALE), equalTo(0.0));
        assertThat(getPointOfLeastRelativeError(10, MIN_SCALE + 2), equalTo(Double.POSITIVE_INFINITY));
    }

    public void testRandomValueIndexing() {
        for (int i = 0; i < 100_000; i++) {
            // generate values in the range 10^-100 to 10^100
            double exponent = randomDouble() * 200 - 100;
            double testValue = Math.pow(10, exponent);
            int scale = randomIntBetween(MIN_SCALE / 2, MAX_SCALE / 2);
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

        for (int i = 0; i < 100_000; i++) {
            long index = randomLongBetween(MIN_INDEX, MAX_INDEX);
            int currentScale = randomIntBetween(MIN_SCALE, MAX_SCALE);
            int maxAdjustment = Math.min(MAX_SCALE - currentScale, getMaximumScaleIncrease(index));

            assertThat(
                adjustScale(adjustScale(index, currentScale, maxAdjustment), currentScale + maxAdjustment, -maxAdjustment),
                equalTo(index)
            );
            if (currentScale + maxAdjustment < MAX_SCALE) {
                if (index > 0) {
                    assertThat(adjustScale(index, currentScale, maxAdjustment) * 2, greaterThan(MAX_INDEX));
                } else if (index < 0) {
                    assertThat(adjustScale(index, currentScale, maxAdjustment) * 2, lessThan(MIN_INDEX));
                }
            }
        }

    }

    public void testRandomBucketBoundaryComparison() {

        for (int i = 0; i < 100_000; i++) {
            long indexA = randomLongBetween(MIN_INDEX, MAX_INDEX);
            long indexB = randomLongBetween(MIN_INDEX, MAX_INDEX);
            int scaleA = randomIntBetween(MIN_SCALE, MAX_SCALE);
            int scaleB = randomIntBetween(MIN_SCALE, MAX_SCALE);

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
                assertThat(
                    Double.compare(lowerBoundA, lowerBoundB),
                    equalTo(compareExponentiallyScaledValues(indexA, scaleA, indexB, scaleB))
                );
            }
        }
    }

    public void testUpscalingAccuracy() {
        // Use slightly adjusted scales to not run into numeric trouble, because we don't use exact maths here
        int minScale = MIN_SCALE + 7;
        int maxScale = MAX_SCALE - 15;

        for (int i = 0; i < 10_000; i++) {

            int startScale = randomIntBetween(minScale, maxScale - 1);
            int scaleIncrease = randomIntBetween(1, maxScale - startScale);

            long index = MAX_INDEX >> scaleIncrease >> (int) (randomDouble() * (MAX_INDEX_BITS - scaleIncrease));
            index = Math.max(1, index);
            index = (long) ((2 * randomDouble() - 1) * index);

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

        MathContext mc = new MathContext(1000);
        BigDecimal one = new BigDecimal(1, mc);
        BigDecimal two = new BigDecimal(2, mc);

        for (int scale = MIN_SCALE; scale <= MAX_SCALE; scale++) {
            BigDecimal base = BigDecimalMath.pow(two, two.pow(-scale, mc), mc);
            BigDecimal factor = one.add(two.pow(scale, mc).multiply(one.subtract(BigDecimalMath.log2(one.add(base), mc))));

            BigDecimal scaledFactor = factor.multiply(two.pow(63, mc)).setScale(0, RoundingMode.FLOOR);
            assertThat(SCALE_UP_CONSTANT_TABLE[scale - MIN_SCALE], equalTo(scaledFactor.longValue()));
        }
    }

}
