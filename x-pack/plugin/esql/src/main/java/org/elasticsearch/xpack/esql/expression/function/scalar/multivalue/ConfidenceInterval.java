/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.commons.math3.distribution.NormalDistribution;
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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;

public class ConfidenceInterval extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ConfidenceInterval",
        ConfidenceInterval::new
    );

    private final Expression bestEstimate;
    private final Expression estimates;

    @FunctionInfo(returnType = { "double", }, description = "...")
    public ConfidenceInterval(
        Source source,
        @Param(name = "bestEstimate", type = { "double", "int", "long" }) Expression bestEstimate,
        @Param(name = "estimates", type = { "double", "int", "long" }) Expression estimates
    ) {
        super(source, Arrays.asList(bestEstimate, estimates));
        this.bestEstimate = bestEstimate;
        this.estimates = estimates;
    }

    private ConfidenceInterval(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(bestEstimate);
        out.writeNamedWriteable(estimates);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(bestEstimate, t -> t.isNumeric() && isRepresentable(t), sourceText(), FIRST, "numeric").and(
            isType(estimates, t -> t.isNumeric() && isRepresentable(t), sourceText(), SECOND, "numeric")
        );
    }

    @Override
    public boolean foldable() {
        return bestEstimate.foldable() && estimates.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(bestEstimate.dataType())) {
            case DOUBLE -> new ConfidenceIntervalDoubleEvaluator.Factory(
                source(),
                toEvaluator.apply(bestEstimate),
                toEvaluator.apply(estimates)
            );
            case INT -> new ConfidenceIntervalIntEvaluator.Factory(source(), toEvaluator.apply(bestEstimate), toEvaluator.apply(estimates));
            case LONG -> new ConfidenceIntervalLongEvaluator.Factory(
                source(),
                toEvaluator.apply(bestEstimate),
                toEvaluator.apply(estimates)
            );
            default -> throw EsqlIllegalArgumentException.illegalDataType(bestEstimate.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ConfidenceInterval(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ConfidenceInterval::new, bestEstimate, estimates);
    }

    @Override
    public DataType dataType() {
        return bestEstimate.dataType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(bestEstimate, estimates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ConfidenceInterval other = (ConfidenceInterval) obj;
        return Objects.equals(other.bestEstimate, bestEstimate) && Objects.equals(other.estimates, estimates);
    }

    @Evaluator(extraName = "Double")
    static void process(DoubleBlock.Builder builder, @Position int position, DoubleBlock bestEstimateBlock, DoubleBlock estimatesBlock) {
        if (bestEstimateBlock.getValueCount(position) != 1) {
            builder.appendNull();
        }
        Number bestEstimate = bestEstimateBlock.getDouble(bestEstimateBlock.getFirstValueIndex(position));

        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getDouble(estimatesBlock.getFirstValueIndex(position) + i);
        }

        Number[] confidenceInterval = computeConfidenceInterval(bestEstimate, estimates);
        builder.beginPositionEntry();
        for (Number v : confidenceInterval) {
            builder.appendDouble(v.doubleValue());
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Int")
    static void process(IntBlock.Builder builder, @Position int position, IntBlock bestEstimateBlock, IntBlock estimatesBlock) {
        if (bestEstimateBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        Number bestEstimate = bestEstimateBlock.getInt(bestEstimateBlock.getFirstValueIndex(position));

        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getInt(estimatesBlock.getFirstValueIndex(position) + i);
        }

        Number[] confidenceInterval = computeConfidenceInterval(bestEstimate, estimates);
        builder.beginPositionEntry();
        for (Number v : confidenceInterval) {
            builder.appendInt(v.intValue());
        }
        builder.endPositionEntry();
    }

    @Evaluator(extraName = "Long")
    static void process(LongBlock.Builder builder, @Position int position, LongBlock bestEstimateBlock, LongBlock estimatesBlock) {
        if (bestEstimateBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        Number bestEstimate = bestEstimateBlock.getLong(bestEstimateBlock.getFirstValueIndex(position));

        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getLong(estimatesBlock.getFirstValueIndex(position) + i);
        }

        Number[] confidenceInterval = computeConfidenceInterval(bestEstimate, estimates);
        builder.beginPositionEntry();
        for (Number v : confidenceInterval) {
            builder.appendLong(v.longValue());
        }
        builder.endPositionEntry();
    }

    private static Number[] computeConfidenceInterval(Number bestEstimate, Number[] estimates) {
        System.out.println("@@@ computeConfidenceInterval: bestEstimate = " + bestEstimate + ", estimates = " + Arrays.toString(estimates));
        Mean estimatesMean = new Mean();
        StandardDeviation estimatesStdDev = new StandardDeviation(false);
        Skewness estimatesSkew = new Skewness();
        for (Number estimate : estimates) {
            estimatesMean.increment(estimate.doubleValue());
            estimatesStdDev.increment(estimate.doubleValue());
            estimatesSkew.increment(estimate.doubleValue());
        }

        double mm = estimatesMean.getResult();
        double sm = estimatesStdDev.getResult();

        if (sm == 0.0) {
            return new Number[] { bestEstimate, bestEstimate, bestEstimate };
        }

        double a = estimatesSkew.getResult() / 6;

        NormalDistribution norm = new NormalDistribution(0, 1);

        double z0 = (bestEstimate.doubleValue() - mm) / sm;
        double dz = norm.inverseCumulativeProbability((1 + 0.95) / 2);  // for 95% confidence interval
        double zl = z0 - dz;
        double zu = z0 + dz;

        sm /= Math.sqrt(estimatesMean.getN());

        return new Number[] { mm + sm * (z0 + zl / (1 - Math.min(0.8, a * zl))), mm + sm * (z0 + zu / (1 - Math.min(0.8, a * zu))), };
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
