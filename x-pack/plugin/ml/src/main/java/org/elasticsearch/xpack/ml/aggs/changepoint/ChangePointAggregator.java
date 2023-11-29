/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.exception.NotStrictlyPositiveException;
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

    private record DataStats(double nValues, double var, int nCandidateChangePoints) {
        @Override
        public String toString() {
            return "DataStats{nValues=" + nValues + ", var=" + var + ",nCandidateChangePoints=" + nCandidateChangePoints + "}";
        }
    }

    private record TestStats(DataStats dataStats, double var, double pValue, double nParams, int changePoint) {
        TestStats(DataStats dataStats, double pValue, int changePoint) {
            this(dataStats, -1.0, pValue, -1.0, changePoint);
        }

        TestStats(DataStats dataStats, double var, double pValue, double nParams) {
            this(dataStats, var, pValue, nParams, -1);
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

        @Override
        public String toString() {
            return "TestStats{"
                + ("dataStats=" + dataStats)
                + (", var=" + var)
                + (", rSquared=" + rSquared())
                + (", pValue=" + pValue)
                + (", nParams=" + nParams)
                + (", changePoint=" + changePoint)
                + '}';
        }
    }

    static Tuple<int[], Integer> candidateChangePoints(double[] values) {
        int minValues = Math.max((int) (0.1 * values.length + 0.5), MINIMUM_BUCKETS);
        if (values.length - 2 * minValues <= MAXIMUM_CANDIDATE_CHANGE_POINTS) {
            return Tuple.tuple(IntStream.range(minValues, values.length - minValues).toArray(), 1);
        } else {
            int step = (int) Math.ceil((double) (values.length - 2 * minValues) / MAXIMUM_CANDIDATE_CHANGE_POINTS);
            return Tuple.tuple(IntStream.range(minValues, values.length - minValues).filter(i -> i % step == 0).toArray(), step);
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
        Tuple<int[], Integer> candidatePoints = candidateChangePoints(bucketValues.getValues());
        ChangeType changeType = changePValue(bucketValues, candidatePoints, P_VALUE_THRESHOLD);
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

    static ChangeType changePValue(
        MlAggsHelper.DoubleBucketValues bucketValues,
        Tuple<int[], Integer> candidateChangePointsAndStep,
        double pValueThreshold
    ) {
        ChangeType changeType = new ChangeType.Stationary();

        double[] timeWindow = bucketValues.getValues();
        double totalUnweightedVariance = RunningStats.from(timeWindow, i -> 1.0).variance();
        if (totalUnweightedVariance == 0.0) {
            return changeType;
        }

        double[] timeWindowWeights = outlierWeights(timeWindow);
        RunningStats dataRunningStats = RunningStats.from(timeWindow, i -> timeWindowWeights[i]);
        if (dataRunningStats.variance() == 0.0) {
            return changeType;
        }

        int[] candidateChangePoints = candidateChangePointsAndStep.v1();
        int step = candidateChangePointsAndStep.v2();
        HashSet<Integer> discoveredChangePoints = new HashSet<>(3, 1.0f);

        DataStats dataStats = new DataStats(dataRunningStats.count(), dataRunningStats.variance(), candidateChangePoints.length);
        TestStats stationary = new TestStats(dataStats, dataStats.var(), 1.0, 1.0);
        logger.debug("total variance: [{}]", dataStats);

        TestStats trendVsStationary = testTrendVs(stationary, timeWindow, timeWindowWeights);
        logger.debug("trend vs stationary: [{}]", trendVsStationary);

        if (trendVsStationary.pValue() < pValueThreshold && trendVsStationary.rSquared() >= 0.5) {
            // Check if there is a change in the trend.
            TestStats trendChangeVsTrend = testTrendChangeVs(trendVsStationary, timeWindow, timeWindowWeights, candidateChangePoints, step);
            logger.debug("trend change vs trend: [{}]", trendChangeVsTrend);

            if (trendChangeVsTrend.pValue() < pValueThreshold && trendChangeVsTrend.rSquared() >= 0.5) {
                discoveredChangePoints.add(trendChangeVsTrend.changePoint());

                // Check if modeling a trend change adds much over modeling a step change.
                TestStats trendChangeVsStepChange = testVsStepChange(trendChangeVsTrend, timeWindow, timeWindowWeights);
                logger.debug("trend change vs step change: [{}]", trendChangeVsStepChange);

                if (trendChangeVsStepChange.pValue() < pValueThreshold) {
                    changeType = new ChangeType.TrendChange(
                        trendChangeVsTrend.pValueVsStationary(),
                        trendChangeVsTrend.rSquared(),
                        bucketValues.getBucketIndex(trendChangeVsStepChange.changePoint())
                    );
                } else {
                    changeType = new ChangeType.StepChange(
                        trendChangeVsTrend.pValue(),
                        bucketValues.getBucketIndex(trendChangeVsTrend.changePoint())
                    );
                }

            } else {
                changeType = new ChangeType.NonStationary(
                    trendVsStationary.pValue(),
                    trendVsStationary.rSquared(),
                    slope(timeWindow, timeWindowWeights) < 0 ? "decreasing" : "increasing"
                );
            }

        } else {
            // Check if there is a step change.
            TestStats stepChangeVsStationary = testStepChangeVs(
                stationary,
                timeWindow,
                timeWindowWeights,
                candidateChangePoints,
                step
            );
            logger.debug("step change vs stationary: [{}]", stepChangeVsStationary);

            if (stepChangeVsStationary.pValue() < pValueThreshold) {
                discoveredChangePoints.add(stepChangeVsStationary.changePoint());

                // Check if modeling a trend change adds much over modeling a step change.
                TestStats trendChangeVsStepChange = testTrendChangeVs(
                    stepChangeVsStationary,
                    timeWindow,
                    timeWindowWeights,
                    candidateChangePoints,
                    step
                );
                logger.debug("trend change vs step change: [{}]", trendChangeVsStepChange);

                if (trendChangeVsStepChange.pValue() < pValueThreshold && trendChangeVsStepChange.rSquared() >= 0.5) {
                    discoveredChangePoints.add(stepChangeVsStationary.changePoint());
                    changeType = new ChangeType.TrendChange(
                        trendChangeVsStepChange.pValueVsStationary(),
                        trendChangeVsStepChange.rSquared(),
                        bucketValues.getBucketIndex(trendChangeVsStepChange.changePoint())
                    );
                } else {
                    changeType = new ChangeType.StepChange(
                        stepChangeVsStationary.pValue(),
                        bucketValues.getBucketIndex(stepChangeVsStationary.changePoint())
                    );
                }

            } else {
                // Check if there is a trend change.
                TestStats trendChangeVsStationary = testTrendChangeVs(
                    stationary,
                    timeWindow,
                    timeWindowWeights,
                    candidateChangePoints,
                    step
                );
                logger.debug("trend change vs stationary: [{}]", trendChangeVsStationary);

                if (trendChangeVsStationary.pValue() < pValueThreshold && trendChangeVsStationary.rSquared() >= 0.5) {
                    changeType = new ChangeType.TrendChange(
                        trendChangeVsStationary.pValue(),
                        trendChangeVsStationary.rSquared(),
                        bucketValues.getBucketIndex(trendChangeVsStationary.changePoint())
                    );
                }
            }
        }

        // We're not very confident in the change point, so check if a distribution change fits the data better.
        if (changeType.pValue() > 1e-5) {
            TestStats distChange = testDistributionChange(
                dataStats,
                timeWindow,
                timeWindowWeights,
                candidateChangePoints,
                step,
                discoveredChangePoints
            );
            logger.debug("distribution change: [{}]", distChange);

            if (distChange.pValue() < Math.min(pValueThreshold, 0.1 * changeType.pValue())) {
                changeType = new ChangeType.DistributionChange(distChange.pValue(), bucketValues.getBucketIndex(distChange.changePoint()));
            }
        }
        return changeType;
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

    static double slope(double[] values, double[] weights) {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < values.length; i++) {
            // Just omit outliers for slope estimation.
            if (weights[i] == 1.0) {
                regression.addData(i, values[i]);
            }
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
        return new TestStats(H0.dataStats(), vTrend, pValue, 3.0);
    }

    static TestStats testStepChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints, int step) {

        double vStep = Double.MAX_VALUE;
        int changePoint = -1;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);
        for (int cp : candidateChangePoints) {
            double v = lowerRange.totalVariance(upperRange);
            if (v < vStep) {
                vStep = v;
                changePoint = cp;
            }
            lowerRange.addValues(values, i -> weights[i], cp, cp + step);
            upperRange.removeValues(values, i -> weights[i], cp, cp + step);
        }

        double pValue = independentTrialsPValue(
            fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vStep, 2.0),
            candidateChangePoints.length
        );

        return new TestStats(H0.dataStats(), vStep, pValue, 2.0, changePoint);
    }

    static TestStats testTrendChangeVs(TestStats H0, double[] values, double[] weights, int[] candidateChangePoints, int step) {

        double vChange = Double.MAX_VALUE;
        int changePoint = -1;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        LeastSquaresOnlineRegression lowerLeastSquares = new LeastSquaresOnlineRegression(2);
        LeastSquaresOnlineRegression upperLeastSquares = new LeastSquaresOnlineRegression(2);
        for (int i = 0; i < candidateChangePoints[0]; i++) {
            lowerLeastSquares.add(i, values[i], weights[i]);
        }
        for (int i = candidateChangePoints[0], x = 0; i < values.length; i++, x++) {
            upperLeastSquares.add(x, values[i], weights[i]);
        }
        for (int cp : candidateChangePoints) {
            double nl = lowerRange.count();
            double nu = upperRange.count();
            double vl = lowerRange.variance() * (1.0 - lowerLeastSquares.rSquared());
            double vu = upperRange.variance() * (1.0 - upperLeastSquares.rSquared());
            double v = (nl * vl + nu * vu) / (nl + nu);
            if (v < vChange) {
                vChange = v;
                changePoint = cp;
            }
            for (int i = cp; i < cp + step; i++) {
                lowerRange.addValue(values[i], weights[i]);
                upperRange.removeValue(values[i], weights[i]);
                lowerLeastSquares.add(i, values[i], weights[i]);
                upperLeastSquares.remove(i, values[i], weights[i]);
            }
        }

        double pValue = independentTrialsPValue(
            fTestNestedPValue(H0.dataStats().nValues(), H0.var(), H0.nParams(), vChange, 6.0),
            candidateChangePoints.length
        );

        return new TestStats(H0.dataStats(), vChange, pValue, 6.0, changePoint);
    }

    static TestStats testVsStepChange(TestStats trendChange, double[] values, double[] weights) {
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        upperRange.addValues(values, i -> weights[i], trendChange.changePoint(), values.length);
        lowerRange.addValues(values, i -> weights[i], 0, trendChange.changePoint());
        double n = trendChange.dataStats().nValues();
        double v = lowerRange.totalVariance(upperRange);
        double pValue = fTestNestedPValue(n, v, 2.0, trendChange.var(), 6.0);
        return new TestStats(trendChange.dataStats(), trendChange.var(), pValue, 6.0, trendChange.changePoint());
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

    static TestStats testDistributionChange(
        DataStats stats,
        double[] values,
        double[] weights,
        int[] candidateChangePoints,
        int step,
        HashSet<Integer> discoveredChangePoints
    ) {
        double maxDiff = 0.0;
        int changePoint = -1;

        // Initialize running stats so that they are only missing the individual changepoint values
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        upperRange.addValues(values, i -> weights[i], candidateChangePoints[0], values.length);
        lowerRange.addValues(values, i -> weights[i], 0, candidateChangePoints[0]);

        for (int cp : candidateChangePoints) {
            double scale = Math.min(cp, values.length - cp);
            double diff = scale * (0.9 * Math.abs(lowerRange.mean() - upperRange.mean()) + 0.1 * Math.abs(
                lowerRange.std() - upperRange.std()
            ));
            if (diff >= maxDiff) {
                maxDiff = diff;
                changePoint = cp;
            }
            lowerRange.addValues(values, i -> weights[i], cp, cp + step);
            upperRange.removeValues(values, i -> weights[i], cp, cp + step);
        }
        discoveredChangePoints.add(changePoint);

        double pValue = 1;
        for (int cp : discoveredChangePoints) {
            double[] x = Arrays.copyOfRange(values, 0, cp);
            double[] y = Arrays.copyOfRange(values, cp, values.length);
            double statistic = KOLMOGOROV_SMIRNOV_TEST.kolmogorovSmirnovStatistic(x, y);
            double ksTestPValue = x.length > 10_000
                ? KOLMOGOROV_SMIRNOV_TEST.approximateP(statistic, x.length, y.length)
                : KOLMOGOROV_SMIRNOV_TEST.exactP(statistic, x.length, y.length, false);
            if (ksTestPValue < pValue) {
                changePoint = cp;
                pValue = ksTestPValue;
            }
        }
        pValue = independentTrialsPValue(pValue, candidateChangePoints.length);

        return new TestStats(stats, pValue, changePoint);
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

        double totalVariance(RunningStats other) {
            return (count() * variance() + other.count() * other.variance()) / (count() + other.count());
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
