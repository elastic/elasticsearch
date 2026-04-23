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

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareExponentiallyScaledValues;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getLowerBucketBoundary;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getPointOfLeastRelativeError;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.getUpperBucketBoundary;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialScaleUtilsTests extends ESTestCase {

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
}
