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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.apache.commons.math3.stat.StatUtils.variance;

/**
 * Kernel Density Estimator
 */
final class KDE {
    private static final double SQRT2 = FastMath.sqrt(2.0);

    /**
     * Fit KDE choosing bandwidth by maximum likelihood cross validation.
     * @param orderedValues the provided values, sorted
     * @return the maximum likelihood bandwidth
     */
    private static double maxLikelihoodBandwidth(double[] orderedValues) {
        int step = Math.max((int) (orderedValues.length / 10.0 + 0.5), 2);
        List<Integer> trainingIndex = new ArrayList<>();
        for (int i = 0; i < orderedValues.length; i += step) {
            int adjStep = Math.min(i + step, orderedValues.length) - i;
            List<Integer> indices = IntStream.range(i, i + adjStep).boxed().collect(Collectors.toList());
            Randomness.shuffle(indices);
            indices.stream().limit(Math.min(adjStep / 2, 4)).forEach(trainingIndex::add);
        }
        Collections.sort(trainingIndex);
        Set<Integer> trainingSet = new HashSet<>(trainingIndex);
        int[] testIndices = IntStream.range(0, orderedValues.length).filter(i -> trainingSet.contains(i) == false).toArray();
        int testStep = (testIndices.length + 19) / 20;
        testIndices = IntStream.range(0, testIndices.length).filter(i -> i % testStep == 0).toArray();
        double[] xTrain = trainingIndex.stream().mapToDouble(i -> orderedValues[i]).toArray();
        double maxLogLikeliHood = -Double.MAX_VALUE;
        double result = 0;
        for (int i = 0; i < 20; ++i) {
            double bandwidth = 0.02 * (i + 1) * (orderedValues[orderedValues.length - 1] - orderedValues[0]);
            double logBandwidth = Math.log(bandwidth);
            double logLikelihood = IntStream.of(testIndices)
                .mapToDouble(j -> logLikelihood(xTrain, bandwidth, logBandwidth, orderedValues[j]))
                .sum();
            if (logLikelihood > maxLogLikeliHood) {
                maxLogLikeliHood = logLikelihood;
                result = bandwidth;
            } else if (logLikelihood == maxLogLikeliHood && bandwidth > result) {
                result = bandwidth;
            }
        }
        return result;
    }

    private static double logLikelihood(double[] xs, double bandwidth, double logBandwidth, double x) {
        int a = Arrays.binarySearch(xs, x - 3.0 * bandwidth);
        if (a < 0) {
            a = -1 - a;
        }
        int b = Arrays.binarySearch(xs, x + 3.0 * bandwidth);
        if (b < 0) {
            b = -1 - b;
        }
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
        List<Double> orderedValues = new ArrayList<>(values.length - excluded);
        for (int i = 0; i < values.length; i++) {
            if ((i >= minIndex - excluded && i <= minIndex + excluded) || (i >= maxIndex - excluded && i <= maxIndex + excluded)) {
                continue;
            }
            orderedValues.add(values[i]);
        }
        this.orderedValues = orderedValues.stream().sorted().mapToDouble(Double::doubleValue).toArray();
        double var = variance(this.orderedValues);
        this.bandwidth = var > 0 ? maxLikelihoodBandwidth(this.orderedValues) : 0.01 * (values[maxIndex] - values[minIndex]);
    }

    double cdf(double x) {
        int a = Arrays.binarySearch(orderedValues, x - 4.0 * bandwidth);
        if (a < 0) {
            a = -1 - a;
        }
        int b = Arrays.binarySearch(orderedValues, x + 4.0 * bandwidth);
        if (b < 0) {
            b = -1 - b;
        }
        return IntStream.range(a, Math.min(Math.max(b, a + 1), orderedValues.length))
            .mapToDouble(i -> new NormalDistribution(orderedValues[i], bandwidth).cumulativeProbability(x))
            .sum() / orderedValues.length;
    }

    double sf(double x) {
        int a = Arrays.binarySearch(orderedValues, x - 4.0 * bandwidth);
        if (a < 0) {
            a = -1 - a;
        }
        int b = Arrays.binarySearch(orderedValues, x + 4.0 * bandwidth);
        if (b < 0) {
            b = -1 - b;
        }
        return IntStream.range(Math.max(Math.min(a, b - 1), 0), b).mapToDouble(i -> normSf(orderedValues[i], bandwidth, x)).sum()
            / orderedValues.length;
    }

    static double normSf(double mean, double standardDeviation, double x) {
        final double dev = x - mean;
        if (Math.abs(dev) > 40 * standardDeviation) {
            return dev > 0 ? 0.0d : 1.0d;
        }
        return 0.5 * Erf.erfc(dev / (standardDeviation * SQRT2));
    }

}
