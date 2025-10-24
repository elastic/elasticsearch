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

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.random.Well19937c;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notANumber;

public class QuantileAccuracyTests extends ExponentialHistogramTestCase {

    public static final double[] QUANTILES_TO_TEST = { 0, 0.0000001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999999, 1.0 };

    private static int randomBucketCount() {
        // exponentially distribute the bucket count to test more for smaller sizes
        return (int) Math.round(5 + Math.pow(1995, randomDouble()));
    }

    public void testNoNegativeZeroReturned() {
        ExponentialHistogram histogram = createAutoReleasedHistogram(
            b -> b.scale(MAX_SCALE).setNegativeBucket(MIN_INDEX, 3) // add a single, negative bucket close to zero
        );
        double median = ExponentialHistogramQuantile.getQuantile(histogram, 0.5);
        assertThat(median, equalTo(0.0));
    }

    public void testPercentilesClampedToMinMax() {
        ExponentialHistogram histogram = createAutoReleasedHistogram(
            b -> b.scale(0).setNegativeBucket(1, 1).setPositiveBucket(1, 1).max(0.00001).min(-0.00002)
        );
        double p0 = ExponentialHistogramQuantile.getQuantile(histogram, 0.0);
        double p100 = ExponentialHistogramQuantile.getQuantile(histogram, 1.0);
        assertThat(p0, equalTo(-0.00002));
        assertThat(p100, equalTo(0.00001));
    }

    public void testMinMaxClampedPercentileAccuracy() {
        ExponentialHistogram histogram = createAutoReleasedHistogram(
            b -> b.scale(0)
                .setPositiveBucket(0, 1) // bucket 0 covers (1, 2]
                .setPositiveBucket(1, 1) // bucket 1 covers (2, 4]
                .min(1.1)
                .max(2.1)
        );

        // The 0.5 percentile linearly interpolates between the two buckets.
        // For the (1, 2] bucket, the point of least relative error will be used (1.33333)
        // For the (2, 4] bucket, the max of the histogram should be used instead (2.1)
        double expectedResult = (4.0 / 3 + 2.1) / 2;
        double p50 = ExponentialHistogramQuantile.getQuantile(histogram, 0.5);
        assertThat(p50, equalTo(expectedResult));

        // Test the same for min instead of max
        histogram = createAutoReleasedHistogram(
            b -> b.scale(0)
                .setNegativeBucket(0, 1) // bucket 0 covers (1, 2]
                .setNegativeBucket(1, 1) // bucket 1 covers (2, 4]
                .min(-2.1)
                .max(-1.1)
        );
        p50 = ExponentialHistogramQuantile.getQuantile(histogram, 0.5);
        assertThat(p50, equalTo(-expectedResult));
    }

    public void testUniformDistribution() {
        testDistributionQuantileAccuracy(new UniformRealDistribution(new Well19937c(randomInt()), 0, 100));
    }

    public void testNormalDistribution() {
        testDistributionQuantileAccuracy(new NormalDistribution(new Well19937c(randomInt()), 100, 15));
    }

    public void testExponentialDistribution() {
        testDistributionQuantileAccuracy(new ExponentialDistribution(new Well19937c(randomInt()), 10));
    }

    public void testLogNormalDistribution() {
        testDistributionQuantileAccuracy(new LogNormalDistribution(new Well19937c(randomInt()), 0, 1));
    }

    public void testGammaDistribution() {
        testDistributionQuantileAccuracy(new GammaDistribution(new Well19937c(randomInt()), 2, 5));
    }

    public void testBetaDistribution() {
        testDistributionQuantileAccuracy(new BetaDistribution(new Well19937c(randomInt()), 2, 5));
    }

    public void testWeibullDistribution() {
        testDistributionQuantileAccuracy(new WeibullDistribution(new Well19937c(randomInt()), 2, 5));
    }

    public void testBasicSmall() {
        DoubleStream values = IntStream.range(1, 10).mapToDouble(Double::valueOf);
        double maxError = testQuantileAccuracy(values.toArray(), 100);
        assertThat(maxError, lessThan(0.000001));
    }

    public void testPercentileOverlapsZeroBucket() {
        ExponentialHistogram histo = createAutoReleasedHistogram(9, -3.0, -2, -1, 0, 0, 0, 1, 2, 3);
        assertThat(ExponentialHistogramQuantile.getQuantile(histo, 8.0 / 16.0), equalTo(0.0));
        assertThat(ExponentialHistogramQuantile.getQuantile(histo, 7.0 / 16.0), equalTo(0.0));
        assertThat(ExponentialHistogramQuantile.getQuantile(histo, 9.0 / 16.0), equalTo(0.0));
        assertThat(ExponentialHistogramQuantile.getQuantile(histo, 5.0 / 16.0), closeTo(-0.5, 0.000001));
        assertThat(ExponentialHistogramQuantile.getQuantile(histo, 11.0 / 16.0), closeTo(0.5, 0.000001));
    }

    public void testBigJump() {
        double[] values = DoubleStream.concat(IntStream.range(0, 18).mapToDouble(Double::valueOf), DoubleStream.of(1_000_000.0)).toArray();

        double maxError = testQuantileAccuracy(values, 500);
        assertThat(maxError, lessThan(0.000001));
    }

    public void testExplicitSkewedData() {
        double[] data = new double[] {
            245,
            246,
            247.249,
            240,
            243,
            248,
            250,
            241,
            244,
            245,
            245,
            247,
            243,
            242,
            241,
            50100,
            51246,
            52247,
            52249,
            51240,
            53243,
            59248,
            59250,
            57241,
            56244,
            55245,
            56245,
            575247,
            58243,
            51242,
            54241 };

        double maxError = testQuantileAccuracy(data, data.length / 2);
        assertThat(maxError, lessThan(0.007));
    }

    public void testEmptyHistogram() {
        ExponentialHistogram histo = ExponentialHistogram.empty();
        for (double q : QUANTILES_TO_TEST) {
            assertThat(ExponentialHistogramQuantile.getQuantile(histo, q), notANumber());
        }
    }

    public void testSingleValueHistogram() {
        ExponentialHistogram histo = createAutoReleasedHistogram(4, 42.0);
        for (double q : QUANTILES_TO_TEST) {
            assertThat(ExponentialHistogramQuantile.getQuantile(histo, q), closeTo(42, 0.0000001));
        }
    }

    public void testBucketCountImpact() {
        RealDistribution distribution = new LogNormalDistribution(new Well19937c(randomInt()), 0, 1);
        int sampleSize = between(100, 50_000);
        double[] values = generateSamples(distribution, sampleSize);

        // Verify that more buckets generally means better accuracy
        double errorWithFewBuckets = testQuantileAccuracy(values, 20);
        double errorWithManyBuckets = testQuantileAccuracy(values, 200);
        assertThat("More buckets should improve accuracy", errorWithManyBuckets, lessThanOrEqualTo(errorWithFewBuckets));
    }

    public void testMixedSignValues() {
        double[] values = new double[between(100, 10_000)];
        for (int i = 0; i < values.length; i++) {
            values[i] = (randomDouble() * 200) - 100; // Range from -100 to 100
        }

        testQuantileAccuracy(values, 100);
    }

    public void testSkewedData() {
        // Create a highly skewed dataset
        double[] values = new double[10000];
        for (int i = 0; i < values.length; i++) {
            if (randomDouble() < 0.9) {
                // 90% of values are small
                values[i] = randomDouble() * 10;
            } else {
                // 10% are very large
                values[i] = randomDouble() * 10000 + 100;
            }
        }

        testQuantileAccuracy(values, 100);
    }

    public void testDataWithZeros() {
        double[] values = new double[10000];
        for (int i = 0; i < values.length; i++) {
            if (randomDouble() < 0.2) {
                // 20% zeros
                values[i] = 0;
            } else {
                values[i] = randomDouble() * 100;
            }
        }

        testQuantileAccuracy(values, 100);
    }

    private void testDistributionQuantileAccuracy(RealDistribution distribution) {
        double[] values = generateSamples(distribution, between(100, 50_000));
        int bucketCount = randomBucketCount();
        testQuantileAccuracy(values, bucketCount);
    }

    public static double[] generateSamples(RealDistribution distribution, int sampleSize) {
        double[] values = new double[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            values[i] = distribution.sample();
        }
        return values;
    }

    private double testQuantileAccuracy(double[] values, int bucketCount) {
        ExponentialHistogram histogram = createAutoReleasedHistogram(bucketCount, values);

        Arrays.sort(values);

        double maxError = 0;
        double allowedError = getMaximumRelativeError(values, bucketCount);

        // Compare histogram quantiles with exact quantiles
        for (double q : QUANTILES_TO_TEST) {
            double percentileRank = q * (values.length - 1);
            int lowerRank = (int) Math.floor(percentileRank);
            int upperRank = (int) Math.ceil(percentileRank);
            double upperFactor = percentileRank - lowerRank;

            if (values[lowerRank] < 0 && values[upperRank] > 0) {
                // the percentile lies directly between a sign change and we interpolate linearly in-between
                // in this case the relative error bound does not hold
                continue;
            }
            double exactValue = values[lowerRank] * (1 - upperFactor) + values[upperRank] * upperFactor;

            double histoValue = ExponentialHistogramQuantile.getQuantile(histogram, q);

            // Skip comparison if exact value is close to zero to avoid false-positives due to numerical imprecision
            if (Math.abs(exactValue) < 1e-100) {
                continue;
            }

            double relativeError = Math.abs(histoValue - exactValue) / Math.abs(exactValue);
            maxError = Math.max(maxError, relativeError);

            assertThat(
                String.format(Locale.ENGLISH, "Quantile %.2f should be accurate within %.6f%% relative error", q, allowedError * 100),
                histoValue,
                closeTo(exactValue, Math.abs(exactValue * allowedError))
            );

        }
        return maxError;
    }

    /**
     * Provides the upper bound of the relative error for any percentile estimate performed with the exponential histogram.
     * The error depends on the raw values put into the histogram and the number of buckets allowed.
     * This is an implementation of the error bound computation proven by Theorem 3 in the <a href="https://arxiv.org/pdf/2004.08604">UDDSketch paper</a>
     */
    public static double getMaximumRelativeError(double[] values, int bucketCount) {
        HashSet<Long> usedPositiveIndices = new HashSet<>();
        HashSet<Long> usedNegativeIndices = new HashSet<>();
        int bestPossibleScale = MAX_SCALE;
        for (double value : values) {
            if (value < 0) {
                usedPositiveIndices.add(computeIndex(value, bestPossibleScale));
            } else if (value > 0) {
                usedNegativeIndices.add(computeIndex(value, bestPossibleScale));
            }
            while ((usedNegativeIndices.size() + usedPositiveIndices.size()) > bucketCount) {
                usedNegativeIndices = rightShiftAll(usedNegativeIndices);
                usedPositiveIndices = rightShiftAll(usedPositiveIndices);
                bestPossibleScale--;
            }
        }
        // for the best possible scale, compute the worst-case error
        double base = Math.pow(2.0, Math.scalb(1.0, -bestPossibleScale));
        return 2 * base / (1 + base) - 1;
    }

    private static HashSet<Long> rightShiftAll(HashSet<Long> indices) {
        HashSet<Long> result = new HashSet<>();
        for (long index : indices) {
            result.add(index >> 1);
        }
        return result;
    }

}
