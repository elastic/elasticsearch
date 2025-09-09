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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;

public class ConfidenceInterval extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY =
        new NamedWriteableRegistry.Entry(Expression.class, "ConfidenceInterval", ConfidenceInterval::new);

    private final Expression bestEstimate;
    private final Expression estimates;

    @FunctionInfo(returnType = { "double", }, description = "...")
    public ConfidenceInterval(
        Source source,
        @Param(name = "bestEstimate", type = { "double", }) Expression bestEstimate,
        @Param(name = "estimates", type = { "double", }) Expression estimates
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
        return isType(bestEstimate, t -> t.isNumeric() && isRepresentable(t), sourceText(), FIRST, "numeric")
            .and(isType(estimates, t -> t.isNumeric() && isRepresentable(t), sourceText(), SECOND, "numeric"));
    }

    @Override
    public boolean foldable() {
        return bestEstimate.foldable() && estimates.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new ConfidenceIntervalEvaluator.Factory(source(), toEvaluator.apply(bestEstimate), toEvaluator.apply(estimates));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ConfidenceInterval(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvAppend::new, bestEstimate, estimates);
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
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

    @Evaluator
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock bestEstimateBlock, DoubleBlock estimates) {
        assert bestEstimateBlock.getValueCount(position) == 1 : "expected 1 element, got " + bestEstimateBlock.getValueCount(position);
        double bestEstimate = bestEstimateBlock.getDouble(bestEstimateBlock.getFirstValueIndex(position));

        Mean estimatesMean = new Mean();
        StandardDeviation estimatesStdDev = new StandardDeviation(false);
        Skewness estimatesSkew = new Skewness();
        int first = estimates.getFirstValueIndex(position);
        for (int i = 0; i < 25; i++) {
            double estimate = i < estimates.getValueCount(position) ? estimates.getDouble(first + i) : 0.0;
            estimatesMean.increment(estimate);
            estimatesStdDev.increment(estimate);
            estimatesSkew.increment(estimate);
        }
        double mm = estimatesMean.getResult();
        double sm = estimatesStdDev.getResult();

        if (sm == 0.0) {
            builder.beginPositionEntry();
            builder.appendDouble(bestEstimate);
            builder.appendDouble(bestEstimate);
            builder.appendDouble(bestEstimate);
            builder.endPositionEntry();
            return;
        }

        double a = estimatesSkew.getResult() / 6;

        NormalDistribution norm = new NormalDistribution(0, 1);

        double z0 = (bestEstimate - mm) / sm;
        double dz = norm.inverseCumulativeProbability((1 + 0.95) / 2);  // for 95% confidence interval
        double zl = z0 - dz;
        double zu = z0 + dz;

        sm /= Math.sqrt(estimates.getValueCount(position));

        builder.beginPositionEntry();
        builder.appendDouble(mm + sm * (z0 + zl / (1 - Math.min(0.8, a * zl))));
        builder.appendDouble(bestEstimate);
        builder.appendDouble(mm + sm * (z0 + zu / (1 - Math.min(0.8, a * zu))));
        builder.endPositionEntry();
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
