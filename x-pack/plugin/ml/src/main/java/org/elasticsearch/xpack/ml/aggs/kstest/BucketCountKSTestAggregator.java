/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.xpack.ml.aggs.DoubleArray;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ml.aggs.MlAggsHelper.extractDoubleBucketedValues;

public class BucketCountKSTestAggregator extends SiblingPipelineAggregator {

    private static final int NUM_ITERATIONS = 20;
    // 23 is chosen as that is ~ half of the typical number of CDF points (55)
    private static final int MINIMUM_NUMBER_OF_DOCS = 23;
    private static final KolmogorovSmirnovTest KOLMOGOROV_SMIRNOV_TEST = new KolmogorovSmirnovTest();

    private final double[] fractions;
    private final EnumSet<Alternative> alternatives;
    private final SamplingMethod samplingMethod;

    public BucketCountKSTestAggregator(
        String name,
        @Nullable double[] fractions,
        EnumSet<Alternative> alternatives,
        String bucketsPath,
        SamplingMethod samplingMethod,
        Map<String, Object> metadata
    ) {
        super(name, new String[] { bucketsPath }, metadata);
        this.fractions = fractions;
        this.alternatives = alternatives;
        this.samplingMethod = samplingMethod;
    }

    static Map<String, Double> ksTest(
        double[] fractions,
        MlAggsHelper.DoubleBucketValues bucketsValue,
        EnumSet<Alternative> alternatives,
        SamplingMethod samplingMethod
    ) {
        long bucketsCountSum = LongStream.of(bucketsValue.getDocCounts()).sum();
        int nSamples = Math.min(
            bucketsCountSum > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) bucketsCountSum,
            samplingMethod.cdfPoints().length
        );
        double[] fX = DoubleArray.cumulativeSum(bucketsValue.getValues());
        if (fX[fX.length - 1] <= 0) {
            return alternatives.stream().map(Alternative::toString).collect(Collectors.toMap(Function.identity(), a -> Double.NaN));
        }
        DoubleArray.divMut(fX, fX[fX.length - 1]);
        double[] fY = DoubleArray.cumulativeSum(fractions);
        if (fY[fY.length - 1] <= 0) {
            return alternatives.stream().map(Alternative::toString).collect(Collectors.toMap(Function.identity(), a -> Double.NaN));
        }
        // Yes, this is "magic" but in testing it seems that sampling on exceptionally sparse data
        // gives us unreliable statistics.
        // TODO bootstrap to support sparse data
        if (nSamples < MINIMUM_NUMBER_OF_DOCS) {
            return alternatives.stream().map(Alternative::toString).collect(Collectors.toMap(Function.identity(), a -> Double.NaN));
        }
        DoubleArray.divMut(fY, fY[fY.length - 1]);
        final double[] monotonic = LongStream.range(1, fY.length + 1).mapToDouble(Double::valueOf).toArray();

        // fast path, since our sample is the entire cdf space, don't worry about multiple passes on sampling
        if (nSamples >= samplingMethod.cdfPoints().length) {
            return ksTest(nSamples, fX, fY, monotonic, samplingMethod.cdfPoints(), alternatives);
        }

        Map<String, Double> result = Stream.generate(() -> ksTest(nSamples, fX, fY, monotonic, samplingMethod.cdfPoints(), alternatives))
            .limit(NUM_ITERATIONS)
            .reduce(new HashMap<>(), (memo, v) -> {
                v.forEach(
                    (alternative, ksTestValue) -> memo.merge(alternative, ksTestValue, (v1, v2) -> v1 + (v2 == 0.0 ? 0.0 : Math.log(v2)))
                );
                return memo;
            });
        alternatives.stream()
            .map(Alternative::toString)
            .forEach(a -> result.put(a, Math.min(1.0, Math.max(Math.exp(result.get(a) / NUM_ITERATIONS), 0.0))));
        return result;
    }

    private static Map<String, Double> ksTest(
        int nSamples,
        double[] fX,
        double[] fY,
        double[] monotonic,
        double[] cdfPoints,
        EnumSet<Alternative> alternatives
    ) {
        int[] samples = sampleOf(cdfPoints.length, nSamples);
        double[] x = new double[samples.length];
        double[] y = new double[samples.length];
        int index = 0;
        for (int i : samples) {
            double f = cdfPoints[i];
            x[index] = interpolate(fX, monotonic, f);
            y[index] = interpolate(fY, monotonic, f);
            ++index;
        }
        // These arrays should typically already be sorted
        // But, depending on the sampling methodology, they could be monotonically DECREASING instead of INCREASING
        // calling `sort` to capture this situation reliably
        Arrays.sort(x);
        Arrays.sort(y);

        Map<String, Double> results = new HashMap<>();
        // The following sided approximation is the same as eq 5.3 from:
        // J. L. Hodges
        // "The significance probability of the smirnov two-sample test," Arkiv för Matematik, Ark. Mat. 3(5), 469-486, (10 Jan. 1958)
        //
        // If calculating `two-sided`, simply use the built in ks-test in apache math.
        final double zConstant = (((double) x.length * y.length) / (x.length + y.length));
        final double continuityConstant = (x.length + 2 * y.length) / Math.sqrt(x.length * y.length * (x.length + y.length));
        for (Alternative alternative : alternatives) {
            double statistic = sidedStatistic(x, y, alternative);
            switch (alternative) {
                case GREATER:
                case LESS:
                    double z = Math.sqrt(zConstant) * statistic;
                    double unBounded = Math.exp(-2 * Math.pow(z, 2) - 2 * z * continuityConstant / 3.0);
                    results.put(alternative.toString(), Math.min(1.0, Math.max(unBounded, 0.0)));
                    break;
                case TWO_SIDED:
                    results.put(alternative.toString(), KOLMOGOROV_SMIRNOV_TEST.exactP(statistic, x.length, y.length, false));
                    break;
                default:
                    throw new AggregationExecutionException("unexpected alternative [" + alternative + "]");
            }

        }
        return results;
    }

    private static int[] sampleOf(int i, int n) {
        if (i <= 0) {
            throw new IllegalArgumentException("cannot create a range from a non-positive number");
        }
        if (n >= i) {
            return IntStream.range(0, i).toArray();
        }
        List<Integer> toSample = IntStream.range(0, i).boxed().collect(Collectors.toList());
        Collections.shuffle(toSample, Randomness.get());
        return toSample.subList(0, n).stream().mapToInt(Integer::intValue).toArray();
    }

    private static double interpolate(double[] xs, double[] fx, double x) {
        int i = Math.min(bisectRight(xs, x), xs.length - 1);
        return ((x - xs[i - 1]) * fx[i] + (xs[i] - x) * fx[i - 1]) / (xs[i] - xs[i - 1]);
    }

    private static int bisectRight(double[] xs, double x) {
        int pos = Arrays.binarySearch(xs, x);
        if (pos < 0) {
            pos = nonNegative(pos) - 1;
        }
        if (pos <= 0) {
            return 1;
        }
        // binarySearch gives no guarantees around duplicates
        while (pos < xs.length && xs[pos] <= x) {
            pos++;
        }
        return pos;
    }

    @SuppressForbidden(reason = "Math#abs(int) is safe here as we protect against MIN_VALUE")
    private static int nonNegative(int x) {
        if (x == Integer.MIN_VALUE) {
            throw new AggregationExecutionException("unexpected value while interpolating sampled values");
        }
        return Math.abs(x);
    }

    private static double sidedStatistic(double[] xa, double[] xb, Alternative alternative) {
        int ia = xa[0] < xb[0] ? 1 : 0;
        int ib = xa[0] < xb[0] ? 0 : 1;
        double t = 0;
        while (ia < xa.length && ib < xb.length) {
            t = Math.max(t, sidedKSStat((double) ia / xa.length, (double) ib / xb.length, alternative));
            if (xa[ia] < xb[ib]) {
                ia++;
            } else if (xb[ib] < xa[ia]) {
                ib++;
            } else {
                ia++;
                ib++;
            }
        }
        t = Math.max(t, sidedKSStat((double) ia / xa.length, (double) ib / xb.length, alternative));
        return alternative == Alternative.LESS ? Math.min(Math.max(t, 0.0), 1.0) : t;
    }

    private static double sidedKSStat(double a, double b, Alternative alternative) {
        switch (alternative) {
            case LESS:
                return Math.max(b - a, 0);
            case GREATER:
                return Math.max(a - b, 0);
            default:
                return Math.abs(b - a);
        }
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {
        Optional<MlAggsHelper.DoubleBucketValues> maybeBucketsValue = extractDoubleBucketedValues(bucketsPaths()[0], aggregations).map(
            bucketValue -> {
                double[] values = new double[bucketValue.getValues().length + 1];
                long[] counts = new long[bucketValue.getDocCounts().length + 1];
                values[0] = 0;
                counts[0] = 0;
                System.arraycopy(bucketValue.getValues(), 0, values, 1, values.length - 1);
                System.arraycopy(bucketValue.getDocCounts(), 0, counts, 1, counts.length - 1);
                return new MlAggsHelper.DoubleBucketValues(counts, values);
            }
        );
        if (maybeBucketsValue.isPresent() == false) {
            throw new AggregationExecutionException(
                "unable to find valid bucket values in bucket path [" + bucketsPaths()[0] + "] for agg [" + name() + "]"
            );
        }
        final MlAggsHelper.DoubleBucketValues bucketsValue = maybeBucketsValue.get();
        double[] fractions = this.fractions == null
            ? DoubleStream.concat(
                DoubleStream.of(0.0),
                Stream.generate(() -> 1.0 / (bucketsValue.getDocCounts().length - 1))
                    .limit(bucketsValue.getDocCounts().length - 1)
                    .mapToDouble(Double::valueOf)
            ).toArray()
            // We prepend zero to the fractions as we prepend 0 to the doc counts and we want them to be the same length when
            // we create the monotonically increasing values for distribution comparison.
            : DoubleStream.concat(DoubleStream.of(0.0), Arrays.stream(this.fractions)).toArray();
        return new InternalKSTestAggregation(name(), metadata(), ksTest(fractions, bucketsValue, alternatives, samplingMethod));
    }
}
