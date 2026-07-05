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

import java.util.Arrays;
import java.util.stream.DoubleStream;

import static org.hamcrest.Matchers.equalTo;

public class RankAccuracyTests extends ExponentialHistogramTestCase {

    public void testRandomDistribution() {
        int numValues = randomIntBetween(10, 10_000);
        double[] values = new double[numValues];

        int valuesGenerated = 0;
        while (valuesGenerated < values.length) {
            double value;
            if (randomDouble() < 0.01) { // 1% chance of exact zero
                value = 0;
            } else {
                value = randomDouble() * 2_000_000 - 1_000_000;
            }
            // Add some duplicates
            for (int i = 0; i < randomIntBetween(1, 10) && valuesGenerated < values.length; i++) {
                values[valuesGenerated++] = value;
            }
        }

        int numBuckets = randomIntBetween(4, 400);
        ExponentialHistogram histo = createAutoReleasedHistogram(numBuckets, values);

        Arrays.sort(values);
        double min = values[0];
        double max = values[values.length - 1];

        double[] valuesRoundedToBucketCenters = DoubleStream.of(values).map(value -> {
            if (value == 0) {
                return 0;
            }
            long index = ExponentialScaleUtils.computeIndex(value, histo.scale());
            double bucketCenter = Math.signum(value) * ExponentialScaleUtils.getPointOfLeastRelativeError(index, histo.scale());
            return Math.clamp(bucketCenter, min, max);
        }).toArray();

        // Test the values at exactly the bucket center for exclusivity correctness
        for (double v : valuesRoundedToBucketCenters) {
            long inclusiveRank = getRank(v, valuesRoundedToBucketCenters, true);
            assertThat(ExponentialHistogramQuantile.estimateRank(histo, v, true), equalTo(inclusiveRank));
            long exclusiveRank = getRank(v, valuesRoundedToBucketCenters, false);
            assertThat(ExponentialHistogramQuantile.estimateRank(histo, v, false), equalTo(exclusiveRank));
        }
        // Test the original values to have values in between bucket centers
        for (double v : values) {
            long inclusiveRank = getRank(v, valuesRoundedToBucketCenters, true);
            assertThat(ExponentialHistogramQuantile.estimateRank(histo, v, true), equalTo(inclusiveRank));
            long exclusiveRank = getRank(v, valuesRoundedToBucketCenters, false);
            assertThat(ExponentialHistogramQuantile.estimateRank(histo, v, false), equalTo(exclusiveRank));
        }

    }

    private static long getRank(double value, double[] sortedValues, boolean inclusive) {
        for (int i = 0; i < sortedValues.length; i++) {
            if (sortedValues[i] > value || (inclusive == false && sortedValues[i] == value)) {
                return i;
            }
        }
        return sortedValues.length;
    }
}
