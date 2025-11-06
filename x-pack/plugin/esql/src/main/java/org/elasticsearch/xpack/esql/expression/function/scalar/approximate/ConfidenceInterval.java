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
import org.elasticsearch.xpack.esql.approximate.Approximate;
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
 * This function is used internally by {@link Approximate}, and is not exposed
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

    @FunctionInfo(returnType = { "double", }, description = "...")
    public ConfidenceInterval(
        Source source,
        @Param(name = "bestEstimate", type = { "double" }) Expression bestEstimate,
        @Param(name = "estimates", type = { "double" }) Expression estimates,
        @Param(name = "trialCount", type = { "integer" }) Expression trialCount,
        @Param(name = "bucketCount", type = { "integer" }) Expression bucketCount,
        @Param(name = "confidenceLevel", type = { "double" }) Expression confidenceLevel
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
        DoubleBlock.Builder builder,
        @Position int position,
        DoubleBlock bestEstimateBlock,
        DoubleBlock estimatesBlock,
        IntBlock trialCountBlock,
        IntBlock bucketCountBlock,
        DoubleBlock confidenceLevelBlock
    ) {
        if (bestEstimateBlock.getValueCount(position) != 1
            || trialCountBlock.getValueCount(position) != 1
            || bucketCountBlock.getValueCount(position) != 1
            || confidenceLevelBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        double bestEstimate = bestEstimateBlock.getDouble(bestEstimateBlock.getFirstValueIndex(position));
        int trialCount = trialCountBlock.getInt(trialCountBlock.getFirstValueIndex(position));
        int bucketCount = bucketCountBlock.getInt(bucketCountBlock.getFirstValueIndex(position));
        if (estimatesBlock.getValueCount(position) != trialCount * bucketCount) {
            builder.appendNull();
            return;
        }
        double[] estimates = new double[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getDouble(estimatesBlock.getFirstValueIndex(position) + i);
        }
        double confidenceLevel = confidenceLevelBlock.getDouble(confidenceLevelBlock.getFirstValueIndex(position));
        double[] confidenceInterval = computeConfidenceInterval(bestEstimate, estimates, trialCount, bucketCount, confidenceLevel);
        if (confidenceInterval == null) {
            builder.appendNull();
        } else {
            builder.beginPositionEntry();
            for (double v : confidenceInterval) {
                builder.appendDouble(v);
            }
            builder.endPositionEntry();
        }
    }

    static double[] computeConfidenceInterval(
        double bestEstimate,
        double[] estimates,
        int trialCount,
        int bucketCount,
        double confidenceLevel
    ) {
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
            double value;
            if (Double.isNaN(value = meanIgnoreNaN.getResult()) == false) {
                meansIgnoreNaN.increment(value);
            }
            if (Double.isNaN(value = meanZeroNaN.getResult()) == false) {
                meansZeroNaN.increment(value);
            }
        }
        if (Double.isNaN(meansIgnoreNaN.getResult()) || Double.isNaN(meansZeroNaN.getResult())) {
            return null;
        }

        double meanIgnoreNan = meansIgnoreNaN.getResult();
        double meanZeroNan = meansZeroNaN.getResult();

        boolean ignoreNaNs = Math.abs(meanIgnoreNan - bestEstimate) < Math.abs(meanZeroNan - bestEstimate);
        double mm = ignoreNaNs ? meanIgnoreNan : meanZeroNan;

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
            if (hasNans == false && computeReliable(skewResult, kurtosisResult, bucketCount)) {
                reliableCount++;
            }
        }

        double sm = stddevs.getResult();
        double skew = skews.getResult();
        if (Double.isNaN(sm) || Double.isNaN(skew)) {
            return null;
        }
        if (sm == 0.0) {
            return new double[] { bestEstimate, bestEstimate, 1.0 };
        }

        // Scale the acceleration to account for the dependence of skewness on sample size.
        double scale = 1 / Math.sqrt(bucketCount);
        double a = scale * skew / 6.0;
        double z0 = (bestEstimate - mm) / sm;
        double dz = normal.inverseCumulativeProbability((1.0 + confidenceLevel) / 2.0);
        double zl = z0 + (z0 - dz) / (1.0 - Math.min(a * (z0 - dz), 0.9));
        double zu = z0 + (z0 + dz) / (1.0 - Math.min(a * (z0 + dz), 0.9));
        double lower = mm + scale * sm * zl;
        double upper = mm + scale * sm * zu;

        return lower <= bestEstimate && bestEstimate <= upper ? new double[] { lower, upper, (double) reliableCount / trialCount } : null;
    }

    static boolean computeReliable(double skew, double kurtosis, int B) {
        if (Double.isNaN(skew) || Double.isNaN(kurtosis) || B < 4) {
            return false;
        }
        double maxSkew = Math.sqrt(6.0 * B * (B - 1) / ((B - 2) * (B + 1) * (B + 3))) * 1.96;
        double maxKurtosis = Math.sqrt(24.0 * B * (B - 1) * (B - 1) / ((B - 3) * (B - 2) * (B + 3) * (B + 5))) * 1.96;
        return Math.abs(skew) < maxSkew && Math.abs(kurtosis) < maxKurtosis;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
