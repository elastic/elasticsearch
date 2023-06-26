/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.util.FastMath;
import org.elasticsearch.common.Randomness;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.apache.commons.math3.stat.StatUtils.variance;

/**
 * Kernel Density Estimator
 */
final class KDE {
    private static final double SQRT2 = FastMath.sqrt(2.0);
    private static final double ESTIMATOR_EPS = 1e-10;

    /**
     * Fit KDE choosing bandwidth by maximum likelihood cross validation.
     * @param orderedValues the provided values, sorted
     * @return the maximum likelihood bandwidth
     */
    private static double maxLikelihoodBandwidth(double[] orderedValues) {
        int step = Math.max((int) (orderedValues.length / 10.0 + 0.5), 2);
        IntStream.Builder trainingIndicesBuilder = IntStream.builder();
        IntStream.Builder testIndicesBuilder = IntStream.builder();
        for (int i = 0; i < orderedValues.length; i += step) {
            int adjStep = Math.min(i + step, orderedValues.length) - i;
            List<Integer> indices = IntStream.range(i, i + adjStep).boxed().collect(Collectors.toList());
            Randomness.shuffle(indices);
            int n = Math.min(adjStep / 2, 4);
            indices.stream().limit(n).forEach(trainingIndicesBuilder::add);
            indices.stream().skip(n).forEach(testIndicesBuilder::add);
        }
        int[] trainingIndices = trainingIndicesBuilder.build().toArray();
        int[] testIndices = testIndicesBuilder.build().toArray();
        Arrays.sort(trainingIndices);
        Arrays.sort(testIndices);
        double[] xTrain = IntStream.of(trainingIndices).mapToDouble(i -> orderedValues[i]).toArray();
        double maxLogLikelihood = -Double.MAX_VALUE;
        double result = 0;
        for (int i = 0; i < 20; ++i) {
            double bandwidth = 0.02 * (i + 1) * (orderedValues[orderedValues.length - 1] - orderedValues[0]);
            double logBandwidth = Math.log(bandwidth);
            double logLikelihood = IntStream.of(testIndices)
                .mapToDouble(j -> logLikelihood(xTrain, bandwidth, logBandwidth, orderedValues[j]))
                .sum();
            if (logLikelihood >= maxLogLikelihood) {
                maxLogLikelihood = logLikelihood;
                result = bandwidth;
            }
        }
        return result;
    }

    private static int lowerBound(double[] xs, double x) {
        int retVal = Arrays.binarySearch(xs, x);
        if (retVal < 0) {
            retVal = -1 - retVal;
        }
        return retVal;
    }

    private static double logLikelihood(double[] xs, double bandwidth, double logBandwidth, double x) {
        int a = lowerBound(xs, x - 3.0 * bandwidth);
        int b = lowerBound(xs, x + 3.0 * bandwidth);
        double[] logPdfs = IntStream.range(Math.max(Math.min(a, b - 1), 0), Math.min(Math.max(b, a + 1), xs.length)).mapToDouble(i -> {
            double y = (x - xs[i]) / bandwidth;
            return -0.5 * y * y - logBandwidth;
        }).toArray();
        double maxLogPdf = DoubleStream.of(logPdfs).max().orElseThrow();
        double result = DoubleStream.of(logPdfs).map(logPdf -> Math.exp(logPdf - maxLogPdf)).sum();
        return Math.log(result) + maxLogPdf;
    }

    private final double[] orderedValues;
    private final double bandwidth;

    KDE(double[] values, int minIndex, int maxIndex) {
        int excluded = (int) (0.025 * ((double) values.length) + 0.5);
        double[] orderedValues = new double[values.length];
        int j = 0;
        for (int i = 0; i < values.length; i++) {
            if ((i >= minIndex - excluded && i <= minIndex + excluded) || (i >= maxIndex - excluded && i <= maxIndex + excluded)) {
                continue;
            }
            orderedValues[j++] = values[i];
        }
        this.orderedValues = Arrays.copyOf(orderedValues, j);
        Arrays.sort(this.orderedValues);
        double var = variance(this.orderedValues);
        this.bandwidth = var > 0 ? maxLikelihoodBandwidth(this.orderedValues) : 0.01 * (values[maxIndex] - values[minIndex]);
    }

    ValueAndMagnitude cdf(double x) {
        int a = lowerBound(orderedValues, x - 4.0 * bandwidth);
        int b = lowerBound(orderedValues, x + 4.0 * bandwidth);
        double cdf = 0.0;
        double diff = Double.MAX_VALUE;
        for (int i = a; i < Math.min(Math.max(b, a + 1), orderedValues.length); i++) {
            cdf += new NormalDistribution(orderedValues[i], bandwidth).cumulativeProbability(x);
            diff = Math.min(Math.abs(orderedValues[i] - x), diff);
        }
        cdf /= orderedValues.length;
        return new ValueAndMagnitude(cdf, diff);
    }

    ValueAndMagnitude sf(double x) {
        int a = lowerBound(orderedValues, x - 4.0 * bandwidth);
        int b = lowerBound(orderedValues, x + 4.0 * bandwidth);
        double sf = 0.0;
        double diff = Double.MAX_VALUE;
        for (int i = Math.max(Math.min(a, b - 1), 0); i < b; i++) {
            sf += normSf(orderedValues[i], bandwidth, x);
            diff = Math.min(Math.abs(orderedValues[i] - x), diff);
        }
        sf /= orderedValues.length;
        return new ValueAndMagnitude(sf, diff);
    }

    static double normSf(double mean, double standardDeviation, double x) {
        final double dev = x - mean;
        if (Math.abs(dev) > 40 * standardDeviation) {
            return dev > 0 ? 0.0d : 1.0d;
        }
        return 0.5 * Erf.erfc(dev / (standardDeviation * SQRT2));
    }

    record ValueAndMagnitude(double value, double magnitude) {
        boolean isMoreSignificant(ValueAndMagnitude o, int numberOfTestedValues) {
            int c = Double.compare(significance(numberOfTestedValues), o.significance(numberOfTestedValues));
            if (c != 0) {
                return c < 0;
            } else {
                return magnitude > o.magnitude;
            }
        }

        double significance(int numberOfTestedValues) {
            return value > ESTIMATOR_EPS ? 1 - Math.pow(1 - value, numberOfTestedValues) : numberOfTestedValues * value;
        }
    }

}
