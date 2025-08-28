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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramUtilsTests extends ExponentialHistogramTestCase {

    public void testRandomDataSumEstimation() {
        for (int i = 0; i < 100; i++) {
            int valueCount = randomIntBetween(100, 10_000);
            int bucketCount = randomIntBetween(2, 500);

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

    public void testInfinityHandling() {
        FixedCapacityExponentialHistogram morePositiveValues = createAutoReleasedHistogram(100);
        morePositiveValues.resetBuckets(0);
        morePositiveValues.tryAddBucket(1999, 1, false);
        morePositiveValues.tryAddBucket(2000, 2, false);
        morePositiveValues.tryAddBucket(1999, 2, true);
        morePositiveValues.tryAddBucket(2000, 2, true);

        double sum = ExponentialHistogramUtils.estimateSum(
            morePositiveValues.negativeBuckets().iterator(),
            morePositiveValues.positiveBuckets().iterator()
        );
        assertThat(sum, equalTo(Double.POSITIVE_INFINITY));
        FixedCapacityExponentialHistogram moreNegativeValues = createAutoReleasedHistogram(100);
        moreNegativeValues.resetBuckets(0);
        moreNegativeValues.tryAddBucket(1999, 2, false);
        moreNegativeValues.tryAddBucket(2000, 2, false);
        moreNegativeValues.tryAddBucket(1999, 1, true);
        moreNegativeValues.tryAddBucket(2000, 2, true);

        sum = ExponentialHistogramUtils.estimateSum(
            moreNegativeValues.negativeBuckets().iterator(),
            moreNegativeValues.positiveBuckets().iterator()
        );
        assertThat(sum, equalTo(Double.NEGATIVE_INFINITY));
    }
}
