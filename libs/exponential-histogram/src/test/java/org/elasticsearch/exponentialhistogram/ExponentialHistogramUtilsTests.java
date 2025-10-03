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

import java.util.OptionalDouble;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramUtilsTests extends ExponentialHistogramTestCase {

    public void testRandomDataSumEstimation() {
        for (int i = 0; i < 100; i++) {
            int valueCount = randomIntBetween(100, 10_000);
            int bucketCount = randomIntBetween(4, 500);

            double correctSum = 0;
            double sign = randomBoolean() ? 1 : -1;
            double[] values = new double[valueCount];
            for (int j = 0; j < valueCount; j++) {
                values[j] = sign * Math.pow(10, randomIntBetween(1, 9)) * randomDouble();
                correctSum += values[j];
            }

            ExponentialHistogram histo = createAutoReleasedHistogram(bucketCount, values);

            double estimatedSum = ExponentialHistogramUtils.estimateSum(
                histo.negativeBuckets().iterator(),
                histo.positiveBuckets().iterator()
            );

            double correctAverage = correctSum / valueCount;
            double estimatedAverage = estimatedSum / valueCount;

            // If the histogram does not contain mixed sign values, we have a guaranteed relative error bound of 2^(2^-scale) - 1
            double histogramBase = Math.pow(2, Math.pow(2, -histo.scale()));
            double allowedError = Math.abs(correctAverage * (histogramBase - 1));

            assertThat(estimatedAverage, closeTo(correctAverage, allowedError));
        }
    }

    public void testSumInfinityHandling() {
        ExponentialHistogram morePositiveValues = createAutoReleasedHistogram(
            b -> b.scale(0).setNegativeBucket(1999, 1).setNegativeBucket(2000, 2).setPositiveBucket(1999, 2).setPositiveBucket(2000, 2)
        );

        double sum = ExponentialHistogramUtils.estimateSum(
            morePositiveValues.negativeBuckets().iterator(),
            morePositiveValues.positiveBuckets().iterator()
        );
        assertThat(sum, equalTo(Double.POSITIVE_INFINITY));
        ExponentialHistogram moreNegativeValues = createAutoReleasedHistogram(
            b -> b.scale(0).setNegativeBucket(1999, 2).setNegativeBucket(2000, 2).setPositiveBucket(1999, 1).setPositiveBucket(2000, 2)
        );

        sum = ExponentialHistogramUtils.estimateSum(
            moreNegativeValues.negativeBuckets().iterator(),
            moreNegativeValues.positiveBuckets().iterator()
        );
        assertThat(sum, equalTo(Double.NEGATIVE_INFINITY));
    }

    public void testMinMaxEstimation() {
        for (int i = 0; i < 100; i++) {
            int positiveValueCount = randomBoolean() ? 0 : randomIntBetween(10, 10_000);
            int negativeValueCount = randomBoolean() ? 0 : randomIntBetween(10, 10_000);
            int zeroValueCount = randomBoolean() ? 0 : randomIntBetween(10, 100);
            int bucketCount = randomIntBetween(4, 500);

            double correctMin = Double.MAX_VALUE;
            double correctMax = -Double.MAX_VALUE;
            double zeroThreshold = Double.MAX_VALUE;
            double[] values = new double[positiveValueCount + negativeValueCount];
            for (int j = 0; j < values.length; j++) {
                double absValue = Math.pow(10, randomIntBetween(1, 9)) * randomDouble();
                if (j < positiveValueCount) {
                    values[j] = absValue;
                } else {
                    values[j] = -absValue;
                }
                zeroThreshold = Math.min(zeroThreshold, absValue / 2);
                correctMin = Math.min(correctMin, values[j]);
                correctMax = Math.max(correctMax, values[j]);
            }
            if (zeroValueCount > 0) {
                correctMin = Math.min(correctMin, -zeroThreshold);
                correctMax = Math.max(correctMax, zeroThreshold);
            }

            ExponentialHistogram histo = createAutoReleasedHistogram(bucketCount, values);

            ZeroBucket zeroBucket = ZeroBucket.create(zeroThreshold, zeroValueCount);
            OptionalDouble estimatedMin = ExponentialHistogramUtils.estimateMin(
                zeroBucket,
                histo.negativeBuckets(),
                histo.positiveBuckets()
            );
            OptionalDouble estimatedMax = ExponentialHistogramUtils.estimateMax(
                zeroBucket,
                histo.negativeBuckets(),
                histo.positiveBuckets()
            );
            if (correctMin == Double.MAX_VALUE) {
                assertThat(estimatedMin.isPresent(), equalTo(false));
                assertThat(estimatedMax.isPresent(), equalTo(false));
            } else {
                assertThat(estimatedMin.isPresent(), equalTo(true));
                assertThat(estimatedMax.isPresent(), equalTo(true));
                // If the histogram does not contain mixed sign values, we have a guaranteed relative error bound of 2^(2^-scale) - 1
                double histogramBase = Math.pow(2, Math.pow(2, -histo.scale()));
                double allowedErrorMin = Math.abs(correctMin * (histogramBase - 1));
                assertThat(estimatedMin.getAsDouble(), closeTo(correctMin, allowedErrorMin));
                double allowedErrorMax = Math.abs(correctMax * (histogramBase - 1));
                assertThat(estimatedMax.getAsDouble(), closeTo(correctMax, allowedErrorMax));
            }
        }
    }

    public void testMinMaxEstimationPositiveInfinityHandling() {
        ExponentialHistogram histo = createAutoReleasedHistogram(b -> b.scale(0).setPositiveBucket(2000, 1));

        OptionalDouble minEstimate = ExponentialHistogramUtils.estimateMin(
            ZeroBucket.minimalEmpty(),
            histo.negativeBuckets(),
            histo.positiveBuckets()
        );
        assertThat(minEstimate.isPresent(), equalTo(true));
        assertThat(minEstimate.getAsDouble(), equalTo(Double.POSITIVE_INFINITY));

        OptionalDouble maxEstimate = ExponentialHistogramUtils.estimateMax(
            ZeroBucket.minimalEmpty(),
            histo.negativeBuckets(),
            histo.positiveBuckets()
        );
        assertThat(maxEstimate.isPresent(), equalTo(true));
        assertThat(maxEstimate.getAsDouble(), equalTo(Double.POSITIVE_INFINITY));
    }

    public void testMinMaxEstimationNegativeInfinityHandling() {
        ExponentialHistogram histo = createAutoReleasedHistogram(b -> b.scale(0).setNegativeBucket(2000, 1));

        OptionalDouble minEstimate = ExponentialHistogramUtils.estimateMin(
            ZeroBucket.minimalEmpty(),
            histo.negativeBuckets(),
            histo.positiveBuckets()
        );
        assertThat(minEstimate.isPresent(), equalTo(true));
        assertThat(minEstimate.getAsDouble(), equalTo(Double.NEGATIVE_INFINITY));

        OptionalDouble maxEstimate = ExponentialHistogramUtils.estimateMax(
            ZeroBucket.minimalEmpty(),
            histo.negativeBuckets(),
            histo.positiveBuckets()
        );
        assertThat(maxEstimate.isPresent(), equalTo(true));
        assertThat(maxEstimate.getAsDouble(), equalTo(Double.NEGATIVE_INFINITY));
    }

    public void testMinimumEstimationSanitizedNegativeZero() {
        OptionalDouble estimate = ExponentialHistogramUtils.estimateMin(
            ZeroBucket.minimalWithCount(42),
            ExponentialHistogram.empty().negativeBuckets(),
            ExponentialHistogram.empty().positiveBuckets()
        );
        assertThat(estimate.isPresent(), equalTo(true));
        assertThat(estimate.getAsDouble(), equalTo(0.0));
    }
}
