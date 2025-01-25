/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.commons.math3.special.Beta;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.function.IntToDoubleFunction;
import java.util.stream.IntStream;

/**
 * Detects whether a time series is stationary or changing
 * (either continuously or at a specific change point).
 */
public class ChangeDetector {

    private static final int MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST = 500;
    private static final int MAXIMUM_CANDIDATE_CHANGE_POINTS = 1000;

    private static final KolmogorovSmirnovTest KOLMOGOROV_SMIRNOV_TEST = new KolmogorovSmirnovTest();

    private static final Logger logger = LogManager.getLogger(ChangeDetector.class);

    private final MlAggsHelper.DoubleBucketValues bucketValues;
    private final double[] values;

    ChangeDetector(MlAggsHelper.DoubleBucketValues bucketValues) {
        this.bucketValues = bucketValues;
        this.values = bucketValues.getValues();
    }

    ChangeType detect(double minBucketsPValue) {
        // This was obtained by simulating the test power for a fixed size effect as a
        // function of the bucket value count.
        double pValueThreshold = minBucketsPValue * Math.exp(-0.04 * (values.length - 2 * (ChangePointDetector.MINIMUM_BUCKETS + 1)));
        return testForChange(pValueThreshold).changeType(bucketValues, slope(values));
    }

    private TestStats testForChange(double pValueThreshold) {

        int[] candidateChangePoints = computeCandidateChangePoints(values);
        logger.trace("candidatePoints: [{}]", Arrays.toString(candidateChangePoints));

        double[] valuesWeights = outlierWeights(values);
        logger.trace("values: [{}]", Arrays.toString(values));
        logger.trace("valuesWeights: [{}]", Arrays.toString(valuesWeights));
        RunningStats dataRunningStats = RunningStats.from(values, i -> valuesWeights[i]);
        DataStats dataStats = new DataStats(
            dataRunningStats.count(),
            dataRunningStats.mean(),
            dataRunningStats.variance(),
            candidateChangePoints.length
        );
        logger.trace("dataStats: [{}]", dataStats);
        TestStats stationary = new TestStats(Type.STATIONARY, 1.0, dataStats.var(), 1.0, dataStats);

        if (dataStats.varianceZeroToWorkingPrecision()) {
            return stationary;
        }

        TestStats trendVsStationary = testTrendVs(stationary, values, valuesWeights);
        logger.trace("trend vs stationary: [{}]", trendVsStationary);

        TestStats best = stationary;
        Set<Integer> discoveredChangePoints = Sets.newHashSetWithExpectedSize(4);
        if (trendVsStationary.accept(pValueThreshold)) {
            // Check if there is a change in the trend.
            TestStats trendChangeVsTrend = testTrendChangeVs(trendVsStationary, values, valuesWeights, candidateChangePoints);
            discoveredChangePoints.add(trendChangeVsTrend.changePoint());
            logger.trace("trend change vs trend: [{}]", trendChangeVsTrend);

            if (trendChangeVsTrend.accept(pValueThreshold)) {
                // Check if modeling a trend change adds much over modeling a step change.
                best = testVsStepChange(trendChangeVsTrend, values, valuesWeights, candidateChangePoints, pValueThreshold);
            } else {
                best = trendVsStationary;
            }

        } else {
            // Check if there is a step change.
            TestStats stepChangeVsStationary = testStepChangeVs(stationary, values, valuesWeights, candidateChangePoints);
            discoveredChangePoints.add(stepChangeVsStationary.changePoint());
            logger.trace("step change vs stationary: [{}]", stepChangeVsStationary);

            if (stepChangeVsStationary.accept(pValueThreshold)) {
                // Check if modeling a trend change adds much over modeling a step change.
                TestStats trendChangeVsStepChange = testTrendChangeVs(stepChangeVsStationary, values, valuesWeights, candidateChangePoints);
                discoveredChangePoints.add(stepChangeVsStationary.changePoint());
                logger.trace("trend change vs step change: [{}]", trendChangeVsStepChange);
                if (trendChangeVsStepChange.accept(pValueThreshold)) {
                    best = trendChangeVsStepChange;
                } else {
                    best = stepChangeVsStationary;
                }

            } else {
                // Check if there is a trend change.
                TestStats trendChangeVsStationary = testTrendChangeVs(stationary, values, valuesWeights, candidateChangePoints);
                discoveredChangePoints.add(stepChangeVsStationary.changePoint());
                logger.trace("trend change vs stationary: [{}]", trendChangeVsStationary);
                if (trendChangeVsStationary.accept(pValueThreshold)) {
                    best = trendChangeVsStationary;
                }
            }
        }

        logger.trace("best: [{}]", best.pValueVsStationary());

        // We're not very confident in the change point, so check if a distribution change
        // fits the data better.
        if (best.pValueVsStationary() > 1e-5) {
            TestStats distChange = testDistributionChange(dataStats, values, valuesWeights, candidateChangePoints, discoveredChangePoints);
            logger.trace("distribution change: [{}]", distChange);
            if (distChange.pValue() < Math.min(pValueThreshold, 0.1 * best.pValueVsStationary())) {
                best = distChange;
            }
        }

        return best;
    }

    private int[] computeCandidateChangePoints(double[] values) {
        int minValues = Math.max((int) (0.1 * values.length + 0.5), ChangePointDetector.MINIMUM_BUCKETS);
        if (values.length - 2 * minValues <= MAXIMUM_CANDIDATE_CHANGE_POINTS) {
            return IntStream.range(minValues, values.length - minValues).toArray();
        } else {
            int step = (int) Math.ceil((double) (values.length - 2 * minValues) / MAXIMUM_CANDIDATE_CHANGE_POINTS);
            return IntStream.range(minValues, values.length - minValues).filter(i -> i % step == 0).toArray();
        }
    }

    private double[] outlierWeights(double[] values) {
        int i = (int) Math.ceil(0.025 * values.length);
        double[] weights = Arrays.copyOf(values, values.length);
        Arrays.sort(weights);
        // We have to be careful here if we have a lot of duplicate values. To avoid marking
        // runs of duplicates as outliers we define outliers to be the smallest (largest)
        // value strictly less (greater) than the value at i (values.length - i - 1). This
        // means if i lands in a run of duplicates the entire run will be marked as inliers.
        double a = weights[i];
        double b = weights[values.length - i - 1];
        for (int j = 0; j < values.length; j++) {
            if (values[j] <= b && values[j] >= a) {
                weights[j] = 1.0;
            } else {
                weights[j] = 0.01;
            }
        }
        return weights;
    }

    private double slope(double[] values) {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < values.length; i++) {
            regression.addData(i, values[i]);
        }
        return regression.getSlope();
    }

    private static double independentTrialsPValue(double pValue, int nTrials) {
        return pValue > 1e-10 ? 1.0 - Math.pow(1.0 - pValue, nTrials) : nTrials * pValue;
    }

    private TestStats testTrendVs(TestStats H0, double[] values, double[] weights) {
        LeastSquaresOnlineRegression allLeastSquares = new LeastSquaresOnlineRegression(2);
        for (int i = 0; i < values.length; i++) {
            allLeastSquares.add(i, values[i], weights[i]);
        }
        double vTrend = H0.dataStats().var() * (1.0 - allLeastSquares.rSquared());
        double pValue = fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vTrend, 3.0);
        return new TestStats(Type.NON_STATIONARY, pValue, vTrend, 3.0, H0.dataStats());
    }

    private TestStats testStepChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints) {

        double vStep = Double.MAX_VALUE;
        int changePoint = ChangeType.NO_CHANGE_POINT;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);
        double mean = H0.dataStats().mean();
        int last = candidateChangePoints[0];
        for (int cp : candidateChangePoints) {
            lowerRange.addValues(values, i -> weights[i], last, cp);
            upperRange.removeValues(values, i -> weights[i], last, cp);
            last = cp;
            double nl = lowerRange.count();
            double nu = upperRange.count();
            double ml = lowerRange.mean();
            double mu = upperRange.mean();
            double vl = lowerRange.variance();
            double vu = upperRange.variance();
            double v = (nl * vl + nu * vu) / (nl + nu);
            if (v < vStep) {
                vStep = v;
                changePoint = cp;
            }
        }

        double pValue = independentTrialsPValue(
            fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vStep, 2.0),
            candidateChangePoints.length
        );

        return new TestStats(Type.STEP_CHANGE, pValue, vStep, 2.0, changePoint, H0.dataStats());
    }

    private TestStats testTrendChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints) {

        double vChange = Double.MAX_VALUE;
        int changePoint = ChangeType.NO_CHANGE_POINT;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        LeastSquaresOnlineRegression lowerLeastSquares = new LeastSquaresOnlineRegression(2);
        LeastSquaresOnlineRegression upperLeastSquares = new LeastSquaresOnlineRegression(2);
        int first = candidateChangePoints[0];
        int last = candidateChangePoints[0];
        for (int i = 0; i < candidateChangePoints[0]; i++) {
            lowerLeastSquares.add(i, values[i], weights[i]);
        }
        for (int i = candidateChangePoints[0]; i < values.length; i++) {
            upperLeastSquares.add(i - first, values[i], weights[i]);
        }
        for (int cp : candidateChangePoints) {
            for (int i = last; i < cp; i++) {
                lowerRange.addValue(values[i], weights[i]);
                upperRange.removeValue(values[i], weights[i]);
                lowerLeastSquares.add(i, values[i], weights[i]);
                upperLeastSquares.remove(i - first, values[i], weights[i]);
            }
            last = cp;
            double nl = lowerRange.count();
            double nu = upperRange.count();
            double rl = lowerLeastSquares.rSquared();
            double ru = upperLeastSquares.rSquared();
            double vl = lowerRange.variance() * (1.0 - rl);
            double vu = upperRange.variance() * (1.0 - ru);
            double v = (nl * vl + nu * vu) / (nl + nu);
            if (v < vChange) {
                vChange = v;
                changePoint = cp;
            }
        }

        double pValue = independentTrialsPValue(
            fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vChange, 6.0),
            candidateChangePoints.length
        );

        return new TestStats(Type.TREND_CHANGE, pValue, vChange, 6.0, changePoint, H0.dataStats());
    }

    private TestStats testVsStepChange(
        TestStats trendChange,
        double[] values,
        double[] weights,
        int[] candidateChangePoints,
        double pValueThreshold
    ) {
        DataStats dataStats = trendChange.dataStats();
        TestStats stationary = new TestStats(Type.STATIONARY, 1.0, dataStats.var(), 1.0, dataStats);
        TestStats stepChange = testStepChangeVs(stationary, values, weights, candidateChangePoints);
        double n = dataStats.nValues();
        double pValue = fTestNestedPValue(n, stepChange.var(), 2.0, trendChange.var(), 6.0);
        return pValue < pValueThreshold ? trendChange : stepChange;
    }

    private static double fTestNestedPValue(double n, double vNull, double pNull, double vAlt, double pAlt) {
        if (vAlt == vNull) {
            return 1.0;
        }
        if (vAlt == 0.0) {
            return 0.0;
        }
        double F = (vNull - vAlt) / (pAlt - pNull) * (n - pAlt) / vAlt;
        double sf = fDistribSf(pAlt - pNull, n - pAlt, F);
        return Math.min(2 * sf, 1.0);
    }

    private static int lowerBound(int[] x, int start, int end, int xs) {
        int retVal = Arrays.binarySearch(x, start, end, xs);
        if (retVal < 0) {
            retVal = -1 - retVal;
        }
        return retVal;
    }

    private SampleData sample(double[] values, double[] weights, Set<Integer> changePoints) {
        Integer[] adjChangePoints = changePoints.toArray(new Integer[changePoints.size()]);
        if (values.length <= MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST) {
            return new SampleData(values, weights, adjChangePoints);
        }

        // Just want repeatable random numbers.
        Random rng = new Random(126832678);
        UniformRealDistribution uniform = new UniformRealDistribution(RandomGeneratorFactory.createRandomGenerator(rng), 0.0, 0.99999);

        // Fisher–Yates shuffle (why isn't this in Arrays?).
        int[] choice = IntStream.range(0, values.length).toArray();
        for (int i = 0; i < MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST; ++i) {
            int index = i + (int) Math.floor(uniform.sample() * (values.length - i));
            int tmp = choice[i];
            choice[i] = choice[index];
            choice[index] = tmp;
        }

        double[] sample = new double[MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST];
        double[] sampleWeights = new double[MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST];
        Arrays.sort(choice, 0, MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST);
        for (int i = 0; i < MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST; ++i) {
            sample[i] = values[choice[i]];
            sampleWeights[i] = weights[choice[i]];
        }
        for (int i = 0; i < adjChangePoints.length; ++i) {
            adjChangePoints[i] = lowerBound(choice, 0, MAXIMUM_SAMPLE_SIZE_FOR_KS_TEST, adjChangePoints[i].intValue());
        }

        return new SampleData(sample, sampleWeights, adjChangePoints);
    }

    private TestStats testDistributionChange(
        DataStats stats,
        double[] values,
        double[] weights,
        int[] candidateChangePoints,
        Set<Integer> discoveredChangePoints
    ) {

        double maxDiff = 0.0;
        int changePoint = ChangeType.NO_CHANGE_POINT;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);
        int last = candidateChangePoints[0];
        for (int cp : candidateChangePoints) {
            lowerRange.addValues(values, i -> weights[i], last, cp);
            upperRange.removeValues(values, i -> weights[i], last, cp);
            last = cp;
            double scale = Math.min(cp, values.length - cp);
            double meanDiff = Math.abs(lowerRange.mean() - upperRange.mean());
            double stdDiff = Math.abs(lowerRange.std() - upperRange.std());
            double diff = scale * (meanDiff + stdDiff);
            if (diff >= maxDiff) {
                maxDiff = diff;
                changePoint = cp;
            }
        }
        discoveredChangePoints.add(changePoint);

        // Note that statistical tests become increasingly powerful as the number of samples
        // increases. We are not interested in detecting visually small distribution changes
        // in splits of long windows so we randomly downsample the data if it is too large
        // before we run the tests.
        SampleData sampleData = sample(values, weights, discoveredChangePoints);
        final double[] sampleValues = sampleData.values();

        double pValue = 1;
        for (int cp : sampleData.changePoints()) {
            if (cp == ChangeType.NO_CHANGE_POINT) {
                continue;
            }
            double[] x = Arrays.copyOfRange(sampleValues, 0, cp);
            double[] y = Arrays.copyOfRange(sampleValues, cp, sampleValues.length);
            double statistic = KOLMOGOROV_SMIRNOV_TEST.kolmogorovSmirnovStatistic(x, y);
            double ksTestPValue = KOLMOGOROV_SMIRNOV_TEST.exactP(statistic, x.length, y.length, false);
            if (ksTestPValue < pValue) {
                changePoint = cp;
                pValue = ksTestPValue;
            }
        }

        // We start to get false positives if we have too many candidate change points. This
        // is the classic p-value hacking problem. However, the Sidak style correction we use
        // elsewhere is too conservative because test statistics for different split positions
        // are strongly correlated. We assume that we have some effective number of independent
        // trials equal to f * n for f < 1. Simulation shows the f = 1/50 yields low Type I
        // error rates.
        pValue = independentTrialsPValue(pValue, (sampleValues.length + 49) / 50);
        logger.trace("distribution change p-value: [{}]", pValue);

        return new TestStats(Type.DISTRIBUTION_CHANGE, pValue, changePoint, stats);
    }

    private static double fDistribSf(double numeratorDegreesOfFreedom, double denominatorDegreesOfFreedom, double x) {
        if (x <= 0) {
            return 1;
        }
        if (Double.isInfinite(x) || Double.isNaN(x)) {
            return 0;
        }

        return Beta.regularizedBeta(
            denominatorDegreesOfFreedom / (denominatorDegreesOfFreedom + numeratorDegreesOfFreedom * x),
            0.5 * denominatorDegreesOfFreedom,
            0.5 * numeratorDegreesOfFreedom
        );
    }

    private enum Type {
        STATIONARY,
        NON_STATIONARY,
        STEP_CHANGE,
        TREND_CHANGE,
        DISTRIBUTION_CHANGE
    }

    private record SampleData(double[] values, double[] weights, Integer[] changePoints) {}

    private record DataStats(double nValues, double mean, double var, int nCandidateChangePoints) {
        boolean varianceZeroToWorkingPrecision() {
            // Our variance calculation is only accurate to ulp(length * mean)^(1/2),
            // i.e. we compute it using the difference of squares method and don't use
            // the Kahan correction. We treat anything as zero to working precision as
            // zero. We should at some point switch to a more numerically stable approach
            // for computing data statistics.
            return var < Math.sqrt(Math.ulp(2.0 * nValues * mean));
        }

        @Override
        public String toString() {
            return "DataStats{nValues=" + nValues + ", mean=" + mean + ", var=" + var + ", nCandidates=" + nCandidateChangePoints + "}";
        }
    }

    private record TestStats(Type type, double pValue, double var, double nParams, int changePoint, DataStats dataStats) {
        TestStats(Type type, double pValue, int changePoint, DataStats dataStats) {
            this(type, pValue, 0.0, 0.0, changePoint, dataStats);
        }

        TestStats(Type type, double pValue, double var, double nParams, DataStats dataStats) {
            this(type, pValue, var, nParams, ChangeType.NO_CHANGE_POINT, dataStats);
        }

        boolean accept(double pValueThreshold) {
            // Check the change is:
            // 1. Statistically significant.
            // 2. That we explain enough of the data variance overall.
            return pValue < pValueThreshold && rSquared() >= 0.5;
        }

        double rSquared() {
            return 1.0 - var / dataStats.var();
        }

        double pValueVsStationary() {
            return independentTrialsPValue(
                fTestNestedPValue(dataStats.nValues(), dataStats.var(), 1.0, var, nParams),
                dataStats.nCandidateChangePoints()
            );
        }

        ChangeType changeType(MlAggsHelper.DoubleBucketValues bucketValues, double slope) {
            switch (type) {
                case STATIONARY:
                    return new ChangeType.Stationary();
                case NON_STATIONARY:
                    return new ChangeType.NonStationary(pValueVsStationary(), rSquared(), slope < 0.0 ? "decreasing" : "increasing");
                case STEP_CHANGE:
                    return new ChangeType.StepChange(pValueVsStationary(), bucketValues.getBucketIndex(changePoint));
                case TREND_CHANGE:
                    return new ChangeType.TrendChange(pValueVsStationary(), rSquared(), bucketValues.getBucketIndex(changePoint));
                case DISTRIBUTION_CHANGE:
                    return new ChangeType.DistributionChange(pValue, bucketValues.getBucketIndex(changePoint));
            }
            throw new RuntimeException("Unknown change type [" + type + "].");
        }

        @Override
        public String toString() {
            return "TestStats{"
                + ("type=" + type)
                + (", dataStats=" + dataStats)
                + (", var=" + var)
                + (", rSquared=" + rSquared())
                + (", pValue=" + pValue)
                + (", nParams=" + nParams)
                + (", changePoint=" + changePoint)
                + '}';
        }
    }

    private static class RunningStats {
        double sumOfSqrs;
        double sum;
        double count;

        static RunningStats from(double[] values, IntToDoubleFunction weightFunction) {
            return new RunningStats().addValues(values, weightFunction, 0, values.length);
        }

        RunningStats() {}

        double count() {
            return count;
        }

        double mean() {
            return sum / count;
        }

        double variance() {
            return Math.max((sumOfSqrs - ((sum * sum) / count)) / count, 0.0);
        }

        double std() {
            return Math.sqrt(variance());
        }

        RunningStats addValues(double[] value, IntToDoubleFunction weightFunction, int start, int end) {
            for (int i = start; i < value.length && i < end; i++) {
                addValue(value[i], weightFunction.applyAsDouble(i));
            }
            return this;
        }

        RunningStats addValue(double value, double weight) {
            sumOfSqrs += (value * value * weight);
            count += weight;
            sum += (value * weight);
            return this;
        }

        RunningStats removeValue(double value, double weight) {
            sumOfSqrs = Math.max(sumOfSqrs - value * value * weight, 0);
            count = Math.max(count - weight, 0);
            sum -= (value * weight);
            return this;
        }

        RunningStats removeValues(double[] value, IntToDoubleFunction weightFunction, int start, int end) {
            for (int i = start; i < value.length && i < end; i++) {
                removeValue(value[i], weightFunction.applyAsDouble(i));
            }
            return this;
        }
    }
}
