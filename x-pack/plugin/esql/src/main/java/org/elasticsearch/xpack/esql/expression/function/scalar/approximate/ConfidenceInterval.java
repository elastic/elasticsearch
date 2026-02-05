/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIFTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * This function is used internally by {@link Approximation}, and is not exposed
 * to users via the {@link EsqlFunctionRegistry}.
 */
public class ConfidenceInterval extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ConfidenceInterval",
        ConfidenceInterval::new
    );

    private static final NormalDistribution normal = new NormalDistribution();

    private final Expression bestEstimate;
    private final Expression estimates;
    private final Expression trialCount;
    private final Expression bucketCount;
    private final Expression confidenceLevel;

    @FunctionInfo(
        returnType = { "double", },
        description = "Computes the confidence interval and its reliability for the given best estimate and bootstrap estimates. The "
            + "output usually is an array with three values: lower bound, upper bound, and the fraction of trials that give a reliable "
            + "interval. If no sensible interval is found, the function returns null instead. "
            + "For example: CONFIDENCE_INTERVAL(10.0, [9.8, 9.9, 10.0, 10.1, 9, 9, 11, 11], 2, 4, 0.9) = [9.54, 10.46, 0.5]"
            + "Explanation: the best estimate (based on all data) is 10.0, and there are 2 trials with 4 buckets each. "
            + "The first trial has estimates [9.8, 9.9, 10.0, 10.1] and the second trial has estimates [9, 9, 11, 11]. "
            + "The computed 90% confidence interval is [9.54, 10.46]. Only the first trial is considered reliable, because "
            + "its values are nicely distributed around the best estimate. The second trial has very high kurtosis and is therefore"
            + "considered unreliable. This leads to a reliability of 0.5 (1 reliable trial out of 2)."
    )
    public ConfidenceInterval(
        Source source,
        @Param(name = "bestEstimate", type = { "double" }, description = "Best estimate of the parameter") Expression bestEstimate,
        @Param(
            name = "estimates",
            type = { "double" },
            description = "Bootstrap estimates of the parameter. This contains a concatenation of trialCount trials with bucketCount "
                + "estimates each."
        ) Expression estimates,
        @Param(name = "trialCount", type = { "integer" }, description = "Number of trials in the estimates data.") Expression trialCount,
        @Param(
            name = "bucketCount",
            type = { "integer" },
            description = "Number of buckets in each trial of the estimates data."
        ) Expression bucketCount,
        @Param(
            name = "confidenceLevel",
            type = { "double" },
            description = "The desired confidence level of the interval."
        ) Expression confidenceLevel
    ) {
        super(source, List.of(bestEstimate, estimates, trialCount, bucketCount, confidenceLevel));
        this.bestEstimate = bestEstimate;
        this.estimates = estimates;
        this.trialCount = trialCount;
        this.bucketCount = bucketCount;
        this.confidenceLevel = confidenceLevel;
    }

    private ConfidenceInterval(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(bestEstimate);
        out.writeNamedWriteable(estimates);
        out.writeNamedWriteable(trialCount);
        out.writeNamedWriteable(bucketCount);
        out.writeNamedWriteable(confidenceLevel);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(bestEstimate, t -> t == DataType.DOUBLE, sourceText(), FIRST, "double").and(
            isType(estimates, t -> t == DataType.DOUBLE, sourceText(), SECOND, "double")
        )
            .and(isType(trialCount, t -> t == DataType.INTEGER, sourceText(), THIRD, "integer"))
            .and(isType(bucketCount, t -> t == DataType.INTEGER, sourceText(), FOURTH, "integer"))
            .and(isType(confidenceLevel, t -> t == DataType.DOUBLE, sourceText(), FIFTH, "double"));
    }

    @Override
    public boolean foldable() {
        return bestEstimate.foldable()
            && estimates.foldable()
            && trialCount.foldable()
            && bucketCount.foldable()
            && confidenceLevel.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ConfidenceIntervalEvaluator.Factory(
            source(),
            toEvaluator.apply(bestEstimate),
            toEvaluator.apply(estimates),
            toEvaluator.apply(trialCount),
            toEvaluator.apply(bucketCount),
            toEvaluator.apply(confidenceLevel)
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ConfidenceInterval(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ConfidenceInterval::new, bestEstimate, estimates, trialCount, bucketCount, confidenceLevel);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bestEstimate, estimates, trialCount, bucketCount, confidenceLevel);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ConfidenceInterval other = (ConfidenceInterval) obj;
        return Objects.equals(other.bestEstimate, bestEstimate)
            && Objects.equals(other.estimates, estimates)
            && Objects.equals(other.trialCount, trialCount)
            && Objects.equals(other.bucketCount, bucketCount)
            && Objects.equals(other.confidenceLevel, confidenceLevel);
    }

    @Evaluator
    static void process(
        DoubleBlock.Builder resultBuilder,
        @Position int position,
        DoubleBlock bestEstimateBlock,
        DoubleBlock estimatesBlock,
        IntBlock trialCountBlock,
        IntBlock bucketCountBlock,
        DoubleBlock confidenceLevelBlock
    ) {
        // This is based on BCa methodlogy described in the following paper:
        // Efron, B. (1987). Better bootstrap confidence intervals.
        //
        // Journal of the American Statistical Association, 82(397), 171-185 https://www.jstor.org/stable/2289144.
        //
        // This is a good exposition of the underlying ideas: https://pages.stat.wisc.edu/~shao/stat710/stat710-24.pdf.
        //
        // In brief, one assumes that after an appropriate monotonic transformation \phi, the distribution of the
        // statistic \theta is normal. Specifically,
        //
        // P( (\hat{\phi} - \phi) / (1 - a \phi) + z_0 <= z ) = \Psi(z)
        //
        // where \hat{\phi} is the transform of the estimate, \phi is the transform true value of the statistic,
        // a is an acceleration parameter, z_0 is a bias correction parameter and \Psi is the CDF of the standard
        // normal distribution.
        //
        // Because \phi is assumed to be monotonic we can compute confidence intervals of the original statistic
        // using the normal distribution without ever needing to know the unknown function \phi. The result is
        // that one just has to transform the percentile points from which one reads off the confidence interval
        // as follows:
        //
        // \Psi(z_0 + (z_0 + z_{\alpha}) / (1 - a (z_0 + z_{\alpha}))) (1)
        //
        // where z_{\alpha} is the \alpha percentile of the standard normal distribution. Normally to compute the
        // various quantities one would:
        // 1. Use the standard normal percentile at the statistic's CDF value for the bootstrap to estimate z_0,
        // 2. Use the empirical distribution of the bootstrap to convert (1) to confidence intervals, and
        // 3. Use a jackknife of the bootstrap samples to estimate the skewness.
        //
        // Here, we make some modifications based on our specific scenario and
        // 1. Estimate the confidence level directly from a normal approximation of the distribution samples,
        // 2. Use a normal approximation of the distribution samples to convert (1) to confidence intervals,
        // 3. Compute skewness using the standard central moments definition from the distribution samples, and
        // 4. Correct the variance and skew to account for mismatch in sample size between the statistic and
        // distribution samples when approximating the statistic distribution.
        //
        // We performed extensive simulations to validate this approach, which show that it produces better
        // calibrated confidence intervals than alternatives.

        // Validate input.
        if (bestEstimateBlock.getValueCount(position) != 1
            || trialCountBlock.getValueCount(position) != 1
            || bucketCountBlock.getValueCount(position) != 1
            || confidenceLevelBlock.getValueCount(position) != 1) {
            resultBuilder.appendNull();
            return;
        }
        double bestEstimate = bestEstimateBlock.getDouble(bestEstimateBlock.getFirstValueIndex(position));
        int trialCount = trialCountBlock.getInt(trialCountBlock.getFirstValueIndex(position));
        int bucketCount = bucketCountBlock.getInt(bucketCountBlock.getFirstValueIndex(position));
        int estimatesCount = estimatesBlock.getValueCount(position);
        if (estimatesCount != trialCount * bucketCount) {
            resultBuilder.appendNull();
            return;
        }
        double confidenceLevel = confidenceLevelBlock.getDouble(confidenceLevelBlock.getFirstValueIndex(position));

        // Collect estimates into an array.
        double[] estimates = new double[estimatesCount];
        int offset = estimatesBlock.getFirstValueIndex(position);
        for (int i = 0; i < estimatesCount; i++) {
            estimates[i] = estimatesBlock.getDouble(offset + i);
        }

        // When a bucket is empty (indicated by a NaN value), it's not clear how to use it to
        // compute the confidence interval. For example:
        // - for a mean/percentile, empty buckets are best ignored;
        // - for a count/sum, it's best to treat them as zero;
        // - for a derived expression (like count()+10), you need a non-zero value;
        // - for a mixed quantity (like sum+count), it's not clear what to do at all.
        //
        // We try two strategies (ignoring and replacing by zero), and pick the one that gives
        // an estimate closest to the best estimate. While not perfect, this heuristic works
        // well in practice for many common cases (means, percentiles, counts, sums).
        //
        // If there are NaNs present in any trial, the interval is marked as unreliable. If the
        // interval is not consistent with the best estimate, it's dropped (null is returned).
        Mean meansIgnoreNaN = new Mean();
        Mean meansZeroNaN = new Mean();
        for (int trial = 0; trial < trialCount; trial++) {
            Mean meanIgnoreNaN = new Mean();
            Mean meanZeroNaN = new Mean();
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                double estimate = estimates[trial * bucketCount + bucket];
                if (Double.isNaN(estimate) == false) {
                    meanIgnoreNaN.increment(estimate);
                    meanZeroNaN.increment(estimate);
                } else {
                    meanZeroNaN.increment(0.0);
                }
            }
            double mean = meanIgnoreNaN.getResult();
            if (Double.isNaN(mean) == false) {
                meansIgnoreNaN.increment(mean);
            }
            mean = meanZeroNaN.getResult();
            if (Double.isNaN(mean) == false) {
                meansZeroNaN.increment(mean);
            }
        }
        if (Double.isNaN(meansIgnoreNaN.getResult()) || Double.isNaN(meansZeroNaN.getResult())) {
            resultBuilder.appendNull();
            return;
        }

        double meanIgnoreNan = meansIgnoreNaN.getResult();
        double meanZeroNan = meansZeroNaN.getResult();

        // Pick the NaN strategy that gives the mean closest to the best estimate.
        boolean ignoreNaNs = Math.abs(meanIgnoreNan - bestEstimate) < Math.abs(meanZeroNan - bestEstimate);
        double mm = ignoreNaNs ? meanIgnoreNan : meanZeroNan;

        // To compute the reliability of each trial's estimate, we use the skewness and kurtosis
        // of the bucket estimates. Under the null hypothesis these should be zero. If these are
        // too large, the trial is considered unreliable. This is a two-tailed p-value at the
        // 95% significance level.
        double maxSkew = 1.96 * Math.sqrt(
            6.0 * bucketCount * (bucketCount - 1) / (bucketCount - 2) / (bucketCount + 1) / (bucketCount + 3)
        );
        double maxKurtosis = 1.96 * Math.sqrt(
            24.0 * bucketCount * (bucketCount - 1) * (bucketCount - 1) / (bucketCount - 3) / (bucketCount - 2) / (bucketCount + 3)
                / (bucketCount + 5)
        );

        // Compute the stddev, skewness, and kurtosis for each of the trials.
        Mean stddevs = new Mean();
        Mean skews = new Mean();
        Mean kurtoses = new Mean();
        int reliableCount = 0;
        for (int trial = 0; trial < trialCount; trial++) {
            StandardDeviation stddev = new StandardDeviation(false);
            Skewness skew = new Skewness();
            Kurtosis kurtosis = new Kurtosis();
            boolean hasNans = false;
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                double estimate = estimates[trial * bucketCount + bucket];
                if (Double.isNaN(estimate)) {
                    hasNans = true;
                    if (ignoreNaNs) {
                        continue;
                    } else {
                        estimate = 0.0;
                    }
                }
                stddev.increment(estimate);
                skew.increment(estimate);
                kurtosis.increment(estimate);
            }
            double stddevResult = stddev.getResult();
            if (Double.isNaN(stddevResult) == false) {
                stddevs.increment(stddevResult);
            }
            double skewResult = skew.getResult();
            if (Double.isNaN(skewResult) == false) {
                skews.increment(skewResult);
            }
            double kurtosisResult = kurtosis.getResult();
            if (Double.isNaN(kurtosisResult) == false) {
                kurtoses.increment(kurtosisResult);
            }
            // A trial is considered reliable if it has no empty buckets (no NaNs; indicating
            // enough data), and its skewness and kurtosis are within acceptable limits.
            if (hasNans == false
                && Double.isNaN(skewResult) == false
                && Math.abs(skewResult) < maxSkew
                && Double.isNaN(kurtosisResult) == false
                && Math.abs(kurtosisResult) < maxKurtosis) {
                reliableCount++;
            }
        }

        double sm = stddevs.getResult();
        double skew = skews.getResult();
        if (Double.isNaN(sm) || Double.isNaN(skew)) {
            resultBuilder.appendNull();
            return;
        }
        if (sm == 0.0) {
            resultBuilder.beginPositionEntry();
            resultBuilder.appendDouble(bestEstimate);
            resultBuilder.appendDouble(bestEstimate);
            resultBuilder.appendDouble((double) reliableCount / trialCount);
            resultBuilder.endPositionEntry();
            return;
        }

        // Scale the acceleration to account for the dependence of skewness on sample size.
        double scale = 1 / Math.sqrt(bucketCount);

        // Use adjusted bootstrap confidence interval (BCa) method to compute the confidence interval.
        double a = scale * skew / 6.0;
        double z0 = (bestEstimate - mm) / sm;
        double dz = normal.inverseCumulativeProbability((1.0 + confidenceLevel) / 2.0);
        double zl = z0 + (z0 - dz) / (1.0 - Math.min(a * (z0 - dz), 0.9));
        double zu = z0 + (z0 + dz) / (1.0 - Math.min(a * (z0 + dz), 0.9));
        double lower = mm + scale * sm * zl;
        double upper = mm + scale * sm * zu;

        if (lower <= bestEstimate && bestEstimate <= upper) {
            resultBuilder.beginPositionEntry();
            resultBuilder.appendDouble(lower);
            resultBuilder.appendDouble(upper);
            resultBuilder.appendDouble((double) reliableCount / trialCount);
            resultBuilder.endPositionEntry();
        } else {
            // If the bestEstimate is outside the confidence interval, it is not a sensible interval,
            // so return null instead. TODO: this criterion is not ideal, and should be revisited.
            resultBuilder.appendNull();
        }
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
