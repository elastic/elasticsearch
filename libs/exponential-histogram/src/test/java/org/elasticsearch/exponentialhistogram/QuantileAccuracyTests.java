/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notANumber;

public class QuantileAccuracyTests extends ESTestCase {

    public static final double[] QUANTILES_TO_TEST = { 0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0 };

    public void testUniformDistribution() {
        testDistributionQuantileAccuracy(new UniformRealDistribution(new Well19937c(42), 0, 100), 50000, 500);
    }

    public void testNormalDistribution() {
        testDistributionQuantileAccuracy(new NormalDistribution(new Well19937c(42), 100, 15), 50000, 500);
    }

    public void testExponentialDistribution() {
        testDistributionQuantileAccuracy(new ExponentialDistribution(new Well19937c(42), 10), 50000, 500);
    }

    public void testLogNormalDistribution() {
        testDistributionQuantileAccuracy(new LogNormalDistribution(new Well19937c(42), 0, 1), 50000, 500);
    }

    public void testGammaDistribution() {
        testDistributionQuantileAccuracy(new GammaDistribution(new Well19937c(42), 2, 5), 50000, 500);
    }

    public void testBetaDistribution() {
        testDistributionQuantileAccuracy(new BetaDistribution(new Well19937c(42), 2, 5), 50000, 500);
    }

    public void testWeibullDistribution() {
        testDistributionQuantileAccuracy(new WeibullDistribution(new Well19937c(42), 2, 5), 50000, 500);
    }

    public void testBasicSmall() {
        DoubleStream values = IntStream.range(1, 10).mapToDouble(Double::valueOf);
        double maxError = testQuantileAccuracy(values.toArray(), 100);
        assertThat(maxError, lessThan(0.000001));
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
        ExponentialHistogram histo = ExponentialHistogramGenerator.createFor();
        for (double q : QUANTILES_TO_TEST) {
            assertThat(ExponentialHistogramQuantile.getQuantile(histo, q), notANumber());
        }
    }

    public void testSingleValueHistogram() {
        ExponentialHistogram histo = ExponentialHistogramGenerator.createFor(42);
        for (double q : QUANTILES_TO_TEST) {
            assertThat(ExponentialHistogramQuantile.getQuantile(histo, q), closeTo(42, 0.0000001));
        }
    }

    public void testBucketCountImpact() {
        RealDistribution distribution = new LogNormalDistribution(new Well19937c(42), 0, 1);
        int sampleSize = 50000;
        double[] values = generateSamples(distribution, sampleSize);

        // Test with different bucket counts
        int[] bucketCounts = { 10, 50, 100, 200, 500 };
        for (int bucketCount : bucketCounts) {
            double maxError = testQuantileAccuracy(values, bucketCount);
            logger.info("Bucket count: " + bucketCount + ", Max relative error: " + maxError);
        }

        // Verify that more buckets generally means better accuracy
        double errorWithFewBuckets = testQuantileAccuracy(values, 20);
        double errorWithManyBuckets = testQuantileAccuracy(values, 200);
        assertThat("More buckets should improve accuracy", errorWithManyBuckets, lessThan(errorWithFewBuckets));
    }

    public void testMixedSignValues() {
        Random random = new Random(42);
        double[] values = new double[10000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (random.nextDouble() * 200) - 100; // Range from -100 to 100
        }

        testQuantileAccuracy(values, 100);
    }

    public void testSkewedData() {
        // Create a highly skewed dataset
        Random random = new Random(42);
        double[] values = new double[10000];
        for (int i = 0; i < values.length; i++) {
            if (random.nextDouble() < 0.9) {
                // 90% of values are small
                values[i] = random.nextDouble() * 10;
            } else {
                // 10% are very large
                values[i] = random.nextDouble() * 10000 + 100;
            }
        }

        testQuantileAccuracy(values, 100);
    }

    public void testDataWithZeros() {
        Random random = new Random(42);
        double[] values = new double[10000];
        for (int i = 0; i < values.length; i++) {
            if (random.nextDouble() < 0.2) {
                // 20% zeros
                values[i] = 0;
            } else {
                values[i] = random.nextDouble() * 100;
            }
        }

        testQuantileAccuracy(values, 100);
    }

    private void testDistributionQuantileAccuracy(RealDistribution distribution, int sampleSize, int bucketCount) {
        double[] values = generateSamples(distribution, sampleSize);
        testQuantileAccuracy(values, bucketCount);
    }

    private static double[] generateSamples(RealDistribution distribution, int sampleSize) {
        double[] values = new double[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            values[i] = distribution.sample();
        }
        return values;
    }

    private double testQuantileAccuracy(double[] values, int bucketCount) {
        // Create histogram
        ExponentialHistogram histogram = ExponentialHistogramGenerator.createFor(bucketCount, Arrays.stream(values));

        // Calculate exact percentiles
        Percentile exactPercentile = new Percentile();
        exactPercentile.setData(values);

        double allowedError = getMaximumRelativeError(values, bucketCount);
        double maxError = 0;

        // Compare histogram quantiles with exact quantiles
        for (double q : QUANTILES_TO_TEST) {
            double exactValue;
            if (q == 0) {
                exactValue = Arrays.stream(values).min().getAsDouble();
            } else if (q == 1) {
                exactValue = Arrays.stream(values).max().getAsDouble();
            } else {
                exactValue = exactPercentile.evaluate(q * 100);
            }
            double histoValue = ExponentialHistogramQuantile.getQuantile(histogram, q);

            // Skip comparison if exact value is zero to avoid division by zero
            if (Math.abs(exactValue) < 1e-10) {
                continue;
            }

            double relativeError = Math.abs(histoValue - exactValue) / Math.abs(exactValue);
            maxError = Math.max(maxError, relativeError);

            logger.info(
                String.format(
                    "Quantile %.2f: Exact=%.6f, Histogram=%.6f, Relative Error=%.8f, Allowed Relative Error=%.8f",
                    q,
                    exactValue,
                    histoValue,
                    relativeError,
                    allowedError
                )
            );

            assertThat(
                String.format("Quantile %.2f should be accurate within %.6f%% relative error", q, allowedError * 100),
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
    private static double getMaximumRelativeError(double[] values, int bucketCount) {
        double smallestAbsNegative = Double.MAX_VALUE;
        double largestAbsNegative = 0;
        double smallestPositive = Double.MAX_VALUE;
        double largestPositive = 0;

        for (double value : values) {
            if (value < 0) {
                smallestAbsNegative = Math.min(-value, smallestAbsNegative);
                largestAbsNegative = Math.max(-value, largestAbsNegative);
            } else if (value > 0) {
                smallestPositive = Math.min(value, smallestPositive);
                largestPositive = Math.max(value, largestPositive);
            }
        }

        // Our algorithm is designed to optimally distribute the bucket budget across the positive and negative range
        // therefore we simply try all variations here and assume the smallest possible error

        if (largestAbsNegative == 0) {
            // only positive values
            double gammaSquare = Math.pow(largestPositive / smallestPositive, 2.0 / (bucketCount));
            return (gammaSquare - 1) / (gammaSquare + 1);
        } else if (smallestAbsNegative == 0) {
            // only negative values
            double gammaSquare = Math.pow(largestAbsNegative / smallestAbsNegative, 2.0 / (bucketCount));
            return (gammaSquare - 1) / (gammaSquare + 1);
        } else {
            double smallestError = Double.MAX_VALUE;
            for (int positiveBuckets = 1; positiveBuckets < bucketCount - 1; positiveBuckets++) {
                int negativeBuckets = bucketCount - positiveBuckets;

                double gammaSquareNeg = Math.pow(largestAbsNegative / smallestAbsNegative, 2.0 / (negativeBuckets));
                double errorNeg = (gammaSquareNeg - 1) / (gammaSquareNeg + 1);

                double gammaSquarePos = Math.pow(largestAbsNegative / smallestAbsNegative, 2.0 / (positiveBuckets));
                double errorPos = (gammaSquarePos - 1) / (gammaSquarePos + 1);

                double error = Math.max(errorNeg, errorPos);
                smallestError = Math.min(smallestError, error);
            }
            return smallestError;
        }
    }

}
