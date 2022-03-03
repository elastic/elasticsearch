/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.special.Beta;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
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
import java.util.Set;
import java.util.function.IntToDoubleFunction;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractBucket;
import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractDoubleBucketedValues;

public class ChangePointAggregator extends SiblingPipelineAggregator {

    static final double P_VALUE_THRESHOLD = 0.025;
    private static final int MINIMUM_BUCKETS = 10;
    private static final int MAXIMUM_CANDIDATE_CHANGE_POINTS = 1000;
    private static final KolmogorovSmirnovTest KOLMOGOROV_SMIRNOV_TEST = new KolmogorovSmirnovTest();

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
        MlAggsHelper.DoubleBucketValues maybeBucketsValue = extractDoubleBucketedValues(
            bucketsPaths()[0],
            aggregations,
            BucketHelpers.GapPolicy.SKIP,
            true
        ).orElseThrow(
            () -> new AggregationExecutionException(
                "unable to find valid bucket values in bucket path [" + bucketsPaths()[0] + "] for agg [" + name() + "]"
            )
        );
        if (maybeBucketsValue.getValues().length < (2 * MINIMUM_BUCKETS) + 2) {
            throw new AggregationExecutionException(
                "not enough buckets to calculate change_point. Requires at least [" + ((2 * MINIMUM_BUCKETS) + 2) + "]"
            );
        }
        Tuple<int[], Integer> candidatePoints = candidateChangePoints(maybeBucketsValue.getValues());
        ChangeType changeType = changePValue(maybeBucketsValue, candidatePoints, P_VALUE_THRESHOLD);
        if (changeType.pValue() > P_VALUE_THRESHOLD) {
            changeType = maxDeviationKdePValue(maybeBucketsValue, P_VALUE_THRESHOLD);
        }
        ChangePointBucket changePointBucket = null;
        if (changeType.changePoint() >= 0) {
            changePointBucket = extractBucket(bucketsPaths()[0], aggregations, changeType.changePoint()).map(
                b -> new ChangePointBucket(b.getKey(), b.getDocCount(), (InternalAggregations) b.getAggregations())
            ).orElse(null);
        }

        return new InternalChangePointAggregation(name(), metadata(), changePointBucket, changeType);
    }

    static ChangeType maxDeviationKdePValue(MlAggsHelper.DoubleBucketValues bucketValues, double pValueThreshold) {
        double[] timeWindow = bucketValues.getValues();
        double variance = RunningStats.from(timeWindow, i -> 1.0).variance();
        if (variance == 0.0) {
            return new ChangeType.Stationary();
        }
        int minIndex = 0;
        double minValue = Double.MAX_VALUE;
        int maxIndex = 0;
        double maxValue = -Double.MAX_VALUE;
        for (int i = 0; i < timeWindow.length; i++) {
            if (timeWindow[i] < minValue) {
                minValue = timeWindow[i];
                minIndex = i;
            }
            if (timeWindow[i] > maxValue) {
                maxValue = timeWindow[i];
                maxIndex = i;
            } else if (timeWindow[i] == maxValue) {
                maxIndex = i;
            }
        }
        KDE dist = new KDE(timeWindow, minIndex, maxIndex);
        KDE.ValueAndMagnitude cdf = dist.cdf(minValue);
        KDE.ValueAndMagnitude sf = dist.sf(maxValue);

        if (cdf.isMoreSignificant(sf, timeWindow.length) && cdf.significance(timeWindow.length) * 2 < pValueThreshold) {
            return new ChangeType.Dip(cdf.significance(timeWindow.length) * 2, bucketValues.getBucketIndex(minIndex));
        }
        if (sf.significance(timeWindow.length) * 2 < pValueThreshold) {
            return new ChangeType.Spike(sf.significance(timeWindow.length) * 2, bucketValues.getBucketIndex(maxIndex));
        }
        return new ChangeType.Stationary();

    }

    static ChangeType changePValue(
        MlAggsHelper.DoubleBucketValues bucketValues,
        Tuple<int[], Integer> candidateChangePointsAndStep,
        double pValueThreshold
    ) {
        double[] timeWindow = bucketValues.getValues();
        double totalUnweightedVariance = RunningStats.from(timeWindow, i -> 1.0).variance();
        ChangeType changeType = new ChangeType.Stationary();
        if (totalUnweightedVariance == 0.0) {
            return changeType;
        }
        double[] timeWindowWeights = outlierWeights(timeWindow);
        int[] candidateChangePoints = candidateChangePointsAndStep.v1();
        int step = candidateChangePointsAndStep.v2();
        double totalVariance = RunningStats.from(timeWindow, i -> timeWindowWeights[i]).variance();
        double vNull = totalVariance;
        if (totalVariance == 0.0) {
            return changeType;
        }
        double n = timeWindow.length;
        double dfNull = n - 1;
        LeastSquaresOnlineRegression allLeastSquares = new LeastSquaresOnlineRegression(2);
        for (int i = 0; i < timeWindow.length; i++) {
            allLeastSquares.add(i, timeWindow[i], timeWindowWeights[i]);
        }
        double rValue = allLeastSquares.rSquared();

        double vAlt = totalVariance * (1 - Math.abs(rValue));
        double dfAlt = n - 3;
        double pValueVsNull = fTestPValue(vNull, dfNull, vAlt, dfAlt);
        if (pValueVsNull < pValueThreshold && Math.abs(rValue) >= 0.5) {
            double pValueVsStationary = fTestPValue(totalVariance, n - 1, vAlt, dfAlt);
            SimpleRegression regression = new SimpleRegression();
            for (int i = 0; i < timeWindow.length; i++) {
                regression.addData(i, timeWindow[i]);
            }
            double slope = regression.getSlope();
            changeType = new ChangeType.NonStationary(pValueVsStationary, rValue, slope < 0 ? "decreasing" : "increasing");
            vNull = vAlt;
            dfNull = dfAlt;
        }
        RunningStats lowerRange = new RunningStats();
        RunningStats upperRange = new RunningStats();
        // Initialize running stats so that they are only missing the individual changepoint values
        upperRange.addValues(timeWindow, i -> timeWindowWeights[i], candidateChangePoints[0], timeWindow.length);
        lowerRange.addValues(timeWindow, i -> timeWindowWeights[i], 0, candidateChangePoints[0]);
        vAlt = Double.MAX_VALUE;
        Set<Integer> discoveredChangePoints = new HashSet<>(3, 1.0f);
        int changePoint = candidateChangePoints[candidateChangePoints.length - 1] + 1;
        for (int cp : candidateChangePoints) {
            double maybeVAlt = (cp * lowerRange.variance() + (n - cp) * upperRange.variance()) / n;
            if (maybeVAlt < vAlt) {
                vAlt = maybeVAlt;
                changePoint = cp;
            }
            lowerRange.addValues(timeWindow, i -> timeWindowWeights[i], cp, cp + step);
            upperRange.removeValues(timeWindow, i -> timeWindowWeights[i], cp, cp + step);
        }
        discoveredChangePoints.add(changePoint);
        dfAlt = n - 2;

        pValueVsNull = independentTrialsPValue(fTestPValue(vNull, dfNull, vAlt, dfAlt), candidateChangePoints.length);
        if (pValueVsNull < pValueThreshold) {
            changeType = new ChangeType.StepChange(pValueVsNull, bucketValues.getBucketIndex(changePoint));
            vNull = vAlt;
            dfNull = dfAlt;
        }

        VarianceAndRValue vAndR = new VarianceAndRValue(Double.MAX_VALUE, Double.MAX_VALUE);
        changePoint = candidateChangePoints[candidateChangePoints.length - 1] + 1;
        lowerRange = new RunningStats();
        upperRange = new RunningStats();
        // Initialize running stats so that they are only missing the individual changepoint values
        upperRange.addValues(timeWindow, i -> timeWindowWeights[i], candidateChangePoints[0], timeWindow.length);
        lowerRange.addValues(timeWindow, i -> timeWindowWeights[i], 0, candidateChangePoints[0]);
        LeastSquaresOnlineRegression lowerLeastSquares = new LeastSquaresOnlineRegression(2);
        LeastSquaresOnlineRegression upperLeastSquares = new LeastSquaresOnlineRegression(2);
        for (int i = 0; i < candidateChangePoints[0]; i++) {
            lowerLeastSquares.add(i, timeWindow[i], timeWindowWeights[i]);
        }
        for (int i = candidateChangePoints[0], x = 0; i < timeWindow.length; i++, x++) {
            upperLeastSquares.add(x, timeWindow[i], timeWindowWeights[i]);
        }
        int upperMovingWindow = 0;
        for (int cp : candidateChangePoints) {
            double lowerRangeVar = lowerRange.variance();
            double upperRangeVar = upperRange.variance();
            double rv1 = lowerLeastSquares.rSquared();
            double rv2 = upperLeastSquares.rSquared();
            double v1 = lowerRangeVar * (1 - Math.abs(rv1));
            double v2 = upperRangeVar * (1 - Math.abs(rv2));
            VarianceAndRValue varianceAndRValue = new VarianceAndRValue((cp * v1 + (n - cp) * v2) / n, (cp * rv1 + (n - cp) * rv2) / n);
            if (varianceAndRValue.compareTo(vAndR) < 0) {
                vAndR = varianceAndRValue;
                changePoint = cp;
            }
            for (int i = 0; i < step; i++) {
                lowerRange.addValue(timeWindow[i + cp], timeWindowWeights[i + cp]);
                upperRange.removeValue(timeWindow[i + cp], timeWindowWeights[i + cp]);
                lowerLeastSquares.add(i + cp, timeWindow[i + cp], timeWindowWeights[i + cp]);
                upperLeastSquares.remove(i + upperMovingWindow, timeWindow[i + cp], timeWindowWeights[i + cp]);
                upperMovingWindow++;
            }
        }
        discoveredChangePoints.add(changePoint);

        dfAlt = n - 6;
        pValueVsNull = independentTrialsPValue(fTestPValue(vNull, dfNull, vAndR.variance, dfAlt), candidateChangePoints.length);
        if (pValueVsNull < pValueThreshold && Math.abs(vAndR.rValue) >= 0.5) {
            double pValueVsStationary = independentTrialsPValue(
                fTestPValue(totalVariance, n - 1, vAndR.variance, dfAlt),
                candidateChangePoints.length
            );
            changeType = new ChangeType.TrendChange(pValueVsStationary, vAndR.rValue, bucketValues.getBucketIndex(changePoint));
        }

        if (changeType.pValue() > 1e-5) {
            double diff = 0.0;
            changePoint = -1;
            lowerRange = new RunningStats();
            upperRange = new RunningStats();
            // Initialize running stats so that they are only missing the individual changepoint values
            upperRange.addValues(timeWindow, i -> timeWindowWeights[i], candidateChangePoints[0], timeWindow.length);
            lowerRange.addValues(timeWindow, i -> timeWindowWeights[i], 0, candidateChangePoints[0]);
            for (int cp : candidateChangePoints) {
                double otherDiff = Math.min(cp, timeWindow.length - cp) * (0.9 * Math.abs(lowerRange.mean() - upperRange.mean())) + 0.1
                    * Math.abs(lowerRange.std() - upperRange.std());
                if (otherDiff >= diff) {
                    changePoint = cp;
                    diff = otherDiff;
                }
                lowerRange.addValues(timeWindow, i -> timeWindowWeights[i], cp, cp + step);
                upperRange.removeValues(timeWindow, i -> timeWindowWeights[i], cp, cp + step);
            }
            discoveredChangePoints.add(changePoint);
            double pValue = 1;
            for (int i : discoveredChangePoints) {
                double ksTestPValue = KOLMOGOROV_SMIRNOV_TEST.kolmogorovSmirnovTest(
                    Arrays.copyOfRange(timeWindow, 0, i),
                    Arrays.copyOfRange(timeWindow, i, timeWindow.length)
                );
                if (ksTestPValue < pValue) {
                    changePoint = i;
                    pValue = ksTestPValue;
                }
            }
            pValue = independentTrialsPValue(pValue, candidateChangePoints.length);
            if (pValue < Math.min(pValueThreshold, 0.1 * changeType.pValue())) {
                changeType = new ChangeType.DistributionChange(pValue, bucketValues.getBucketIndex(changePoint));
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

    static double independentTrialsPValue(double pValue, int nTrials) {
        return pValue > 1e-10 ? 1.0 - Math.pow(1.0 - pValue, nTrials) : nTrials * pValue;
    }

    static double fTestPValue(double vNull, double dfNull, double varianceAlt, double dfAlt) {
        if (varianceAlt == vNull) {
            return 1.0;
        }
        if (varianceAlt == 0.0) {
            return 0.0;
        }
        double F = dfAlt / dfNull * vNull / varianceAlt;
        double sf = fDistribSf(dfNull, dfAlt, F);
        return Math.min(2 * sf, 1.0);
    }

    static class RunningStats {
        double sumOfSqrs;
        double sum;
        double count;

        static RunningStats from(double[] values, IntToDoubleFunction weightFunction) {
            return new RunningStats().addValues(values, weightFunction, 0, values.length);
        }

        RunningStats() {}

        double variance() {
            return Math.max((sumOfSqrs - ((sum * sum) / count)) / count, 0.0);
        }

        double mean() {
            return sum / count;
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

    static record VarianceAndRValue(double variance, double rValue) implements Comparable<VarianceAndRValue> {
        @Override
        public int compareTo(VarianceAndRValue o) {
            int v = Double.compare(variance, o.variance);
            if (v == 0) {
                return Double.compare(rValue, o.rValue);
            }
            return v;
        }

        public VarianceAndRValue min(VarianceAndRValue other) {
            if (this.compareTo(other) <= 0) {
                return this;
            }
            return other;
        }
    }

    static double fDistribSf(double numeratorDegreesOfFreedom, double denominatorDegreesOfFreedom, double x) {
        if (x <= 0) {
            return 1;
        } else if (Double.isInfinite(x) || Double.isNaN(x)) {
            return 0;
        }

        return Beta.regularizedBeta(
            denominatorDegreesOfFreedom / (denominatorDegreesOfFreedom + numeratorDegreesOfFreedom * x),
            0.5 * denominatorDegreesOfFreedom,
            0.5 * numeratorDegreesOfFreedom
        );
    }

}
