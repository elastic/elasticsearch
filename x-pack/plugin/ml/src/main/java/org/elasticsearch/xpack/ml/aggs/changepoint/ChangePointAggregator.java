/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.commons.math3.special.Beta;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.IntToDoubleFunction;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractBucket;
import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractDoubleBucketedValues;

public class ChangePointAggregator extends SiblingPipelineAggregator {

    private static final Logger logger = LogManager.getLogger(ChangePointAggregator.class);

    static final double P_VALUE_THRESHOLD = 0.025;
    private static final int MINIMUM_BUCKETS = 10;
    private static final int MAXIMUM_CANDIDATE_CHANGE_POINTS = 1000;
    private static final KolmogorovSmirnovTest KOLMOGOROV_SMIRNOV_TEST = new KolmogorovSmirnovTest();

    private record DataStats(double nValues, double mean, double var, int nCandidateChangePoints) {
        @Override
        public String toString() {
            return "DataStats{nValues=" + nValues + ", mean=" + mean + ", var=" + var + ", nCandidates=" + nCandidateChangePoints + "}";
        }
    }

    static int[] candidateChangePoints(double[] values) {
        int minValues = Math.max((int) (0.1 * values.length + 0.5), MINIMUM_BUCKETS);
        if (values.length - 2 * minValues <= MAXIMUM_CANDIDATE_CHANGE_POINTS) {
            return IntStream.range(minValues, values.length - minValues).toArray();
        } else {
            int step = (int) Math.ceil((double) (values.length - 2 * minValues) / MAXIMUM_CANDIDATE_CHANGE_POINTS);
            return IntStream.range(minValues, values.length - minValues).filter(i -> i % step == 0).toArray();
        }
    }

    public ChangePointAggregator(String name, String bucketsPath, Map<String, Object> metadata) {
        super(name, new String[] { bucketsPath }, metadata);
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, AggregationReduceContext context) {
        Optional<MlAggsHelper.DoubleBucketValues> maybeBucketsValue = extractDoubleBucketedValues(
            bucketsPaths()[0],
            aggregations,
            BucketHelpers.GapPolicy.SKIP,
            true
        );
        if (maybeBucketsValue.isEmpty()) {
            return new InternalChangePointAggregation(
                name(),
                metadata(),
                null,
                new ChangeType.Indeterminable("unable to find valid bucket values in bucket path [" + bucketsPaths()[0] + "]")
            );
        }
        MlAggsHelper.DoubleBucketValues bucketValues = maybeBucketsValue.get();
        if (bucketValues.getValues().length < (2 * MINIMUM_BUCKETS) + 2) {
            return new InternalChangePointAggregation(
                name(),
                metadata(),
                null,
                new ChangeType.Indeterminable(
                    "not enough buckets to calculate change_point. Requires at least ["
                        + ((2 * MINIMUM_BUCKETS) + 2)
                        + "]; found ["
                        + bucketValues.getValues().length
                        + "]"
                )
            );
        }
        int[] candidatePoints = candidateChangePoints(bucketValues.getValues());
        ChangeType changeType = testForChange(bucketValues, candidatePoints, P_VALUE_THRESHOLD);
        if (changeType.pValue() > P_VALUE_THRESHOLD) {
            try {
                SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues.getValues());
                changeType = detect.at(P_VALUE_THRESHOLD);
            } catch (NotStrictlyPositiveException nspe) {
                logger.debug("failure calculating spikes", nspe);
            }
        }
        ChangePointBucket changePointBucket = null;
        if (changeType.changePoint() >= 0) {
            changePointBucket = extractBucket(bucketsPaths()[0], aggregations, changeType.changePoint()).map(
                b -> new ChangePointBucket(b.getKey(), b.getDocCount(), (InternalAggregations) b.getAggregations())
            ).orElse(null);
        }

        return new InternalChangePointAggregation(name(), metadata(), changePointBucket, changeType);
    }

    static ChangeType testForChange(MlAggsHelper.DoubleBucketValues bucketValues, int[] candidateChangePoints, double pValueThreshold) {
        double[] timeWindow = bucketValues.getValues();
        double totalUnweightedVariance = RunningStats.from(timeWindow, i -> 1.0).variance();
        if (totalUnweightedVariance == 0.0) {
            return new ChangeType.Stationary();
        }
        return testForChange(timeWindow, candidateChangePoints, pValueThreshold).changeType(bucketValues, slope(timeWindow));
    }

    static TestStats testForChange(double[] timeWindow, int[] candidateChangePoints, double pValueThreshold) {

        logger.debug("timeWindow: [{}]", Arrays.toString(timeWindow));

        double[] timeWindowWeights = outlierWeights(timeWindow);
        RunningStats dataRunningStats = RunningStats.from(timeWindow, i -> timeWindowWeights[i]);
        DataStats dataStats = new DataStats(
            dataRunningStats.count(),
            dataRunningStats.mean(),
            dataRunningStats.variance(),
            candidateChangePoints.length
        );
        TestStats stationary = new TestStats(Type.STATIONARY, 1.0, dataStats.var(), 1.0, dataStats);

        // Should never happen but just in case.
        if (dataRunningStats.variance() == 0.0) {
            return stationary;
        }

        TestStats trendVsStationary = testTrendVs(stationary, timeWindow, timeWindowWeights);
        logger.debug("trend vs stationary: [{}]", trendVsStationary);

        TestStats best = stationary;
        HashSet<Integer> discoveredChangePoints = new HashSet<>(4, 1.0f);
        if (trendVsStationary.accept(pValueThreshold)) {
            // Check if there is a change in the trend.
            TestStats trendChangeVsTrend = testTrendChangeVs(trendVsStationary, timeWindow, timeWindowWeights, candidateChangePoints);
            discoveredChangePoints.add(trendChangeVsTrend.changePoint());
            logger.debug("trend change vs trend: [{}]", trendChangeVsTrend);

            if (trendChangeVsTrend.accept(pValueThreshold)) {
                // Check if modeling a trend change adds much over modeling a step change.
                best = testVsStepChange(trendChangeVsTrend, timeWindow, timeWindowWeights, candidateChangePoints, pValueThreshold);
            } else {
                best = trendVsStationary;
            }

        } else {
            // Check if there is a step change.
            TestStats stepChangeVsStationary = testStepChangeVs(stationary, timeWindow, timeWindowWeights, candidateChangePoints);
            discoveredChangePoints.add(stepChangeVsStationary.changePoint());
            logger.debug("step change vs stationary: [{}]", stepChangeVsStationary);

            if (stepChangeVsStationary.accept(pValueThreshold)) {
                // Check if modeling a trend change adds much over modeling a step change.
                TestStats trendChangeVsStepChange = testTrendChangeVs(
                    stepChangeVsStationary,
                    timeWindow,
                    timeWindowWeights,
                    candidateChangePoints
                );
                discoveredChangePoints.add(stepChangeVsStationary.changePoint());
                logger.debug("trend change vs step change: [{}]", trendChangeVsStepChange);
                if (trendChangeVsStepChange.accept(pValueThreshold)) {
                    best = trendChangeVsStepChange;
                } else {
                    best = stepChangeVsStationary;
                }

            } else {
                // Check if there is a trend change.
                TestStats trendChangeVsStationary = testTrendChangeVs(stationary, timeWindow, timeWindowWeights, candidateChangePoints);
                discoveredChangePoints.add(stepChangeVsStationary.changePoint());
                logger.debug("trend change vs stationary: [{}]", trendChangeVsStationary);
                if (trendChangeVsStationary.accept(pValueThreshold)) {
                    best = trendChangeVsStationary;
                }
            }
        }

        logger.debug("best: [{}]", best.pValueVsStationary());

        // We're not very confident in the change point, so check if a distribution change fits the data better.
        if (best.pValueVsStationary() > 1e-5) {
            TestStats distChange = testDistributionChange(
                dataStats,
                timeWindow,
                timeWindowWeights,
                candidateChangePoints,
                discoveredChangePoints
            );
            logger.debug("distribution change: [{}]", distChange);
            if (distChange.pValue() < Math.min(pValueThreshold, 0.1 * best.pValueVsStationary())) {
                best = distChange;
            }
        }

        return best;
    }

    static double[] outlierWeights(double[] values) {
        int i = (int) Math.ceil(0.025 * values.length);
        double[] weights = Arrays.copyOf(values, values.length);
        Arrays.sort(weights);
        double a = weights[i];
        double b = weights[values.length - i];
        for (int j = 0; j < values.length; j++) {
            if (values[j] < b && values[j] >= a) {
                weights[j] = 1.0;
            } else {
                weights[j] = 0.01;
            }
        }
        return weights;
    }

    static double slope(double[] values) {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < values.length; i++) {
            regression.addData(i, values[i]);
        }
        return regression.getSlope();
    }

    static double independentTrialsPValue(double pValue, int nTrials) {
        return pValue > 1e-10 ? 1.0 - Math.pow(1.0 - pValue, nTrials) : nTrials * pValue;
    }

    static TestStats testTrendVs(TestStats H0, double[] values, double[] weights) {
        LeastSquaresOnlineRegression allLeastSquares = new LeastSquaresOnlineRegression(2);
        for (int i = 0; i < values.length; i++) {
            allLeastSquares.add(i, values[i], weights[i]);
        }
        double vTrend = H0.dataStats().var() * (1.0 - allLeastSquares.rSquared());
        double pValue = fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vTrend, 3.0);
        return new TestStats(Type.NON_STATIONARY, pValue, vTrend, 3.0, H0.dataStats());
    }

    static TestStats testStepChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints) {

        double vStep = Double.MAX_VALUE;
        int changePoint = -1;

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

    static TestStats testTrendChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints) {

        double vChange = Double.MAX_VALUE;
        int changePoint = -1;

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

    static TestStats testVsStepChange(
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

    static double fTestNestedPValue(double n, double vNull, double pNull, double vAlt, double pAlt) {
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

    static Tuple<double[], double[]> sample(double[] values, double[] weights) {
        if (values.length <= 500) {
            return Tuple.tuple(values, weights);
        }

        // Just want repeatable random numbers.
        Random rng = new Random(126832678);
        UniformRealDistribution uniform = new UniformRealDistribution(RandomGeneratorFactory.createRandomGenerator(rng), 0.0, 0.99999);

        // Fisher–Yates shuffle (why isn't this in Arrays?).
        int[] choice = IntStream.range(0, values.length).toArray();
        for (int i = 0; i < 500; ++i) {
            int index = i + (int) Math.floor(uniform.sample() * (values.length - i));
            int tmp = choice[i];
            choice[i] = choice[index];
            choice[index] = tmp;
        }

        double[] sample = new double[500];
        double[] sampleWeights = new double[500];
        Arrays.sort(choice, 0, 500);
        for (int i = 0; i < 500; ++i) {
            sample[i] = values[choice[i]];
            sampleWeights[i] = weights[choice[i]];
        }
        return Tuple.tuple(sample, sampleWeights);
    }

    static TestStats testDistributionChange(
        DataStats stats,
        double[] values,
        double[] weights,
        int[] candidateChangePoints,
        HashSet<Integer> discoveredChangePoints
    ) {

        double maxDiff = 0.0;
        int changePoint = -1;

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
        // in splits of long windows so we randomly downsample the data to at most 500 samples
        // before running the tests.
        Tuple<double[], double[]> sample = sample(values, weights);
        final double[] sampleValues = sample.v1();
        final double[] sampleWeights = sample.v2();

        double pValue = 1;
        for (int cp : discoveredChangePoints) {
            double[] x = Arrays.copyOfRange(sampleValues, 0, cp);
            double[] y = Arrays.copyOfRange(sampleValues, cp, sampleValues.length);
            double statistic = KOLMOGOROV_SMIRNOV_TEST.kolmogorovSmirnovStatistic(x, y);
            double ksTestPValue = KOLMOGOROV_SMIRNOV_TEST.exactP(statistic, x.length, y.length, false);
            if (ksTestPValue < pValue) {
                changePoint = cp;
                pValue = ksTestPValue;
            }
        }

        return new TestStats(Type.DISTRIBUTION_CHANGE, pValue, changePoint, stats);
    }

    static double fDistribSf(double numeratorDegreesOfFreedom, double denominatorDegreesOfFreedom, double x) {
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

    enum Type {
        STATIONARY,
        NON_STATIONARY,
        STEP_CHANGE,
        TREND_CHANGE,
        DISTRIBUTION_CHANGE
    }

    record TestStats(Type type, double pValue, double var, double nParams, int changePoint, DataStats dataStats) {
        TestStats(Type type, double pValue, int changePoint, DataStats dataStats) {
            this(type, pValue, 0.0, 0.0, changePoint, dataStats);
        }

        TestStats(Type type, double pValue, double var, double nParams, DataStats dataStats) {
            this(type, pValue, var, nParams, -1, dataStats);
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
                    return new ChangeType.DistributionChange(pValue, changePoint);
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

    static class RunningStats {
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
