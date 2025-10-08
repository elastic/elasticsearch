/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
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
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;

/**
 * This function is used internally by {@link Approximate}, and is not exposed
 * to users via the {@link EsqlFunctionRegistry}.
 */
public class Reliable extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Reliable", Reliable::new);

    private final Expression estimates;

    @FunctionInfo(returnType = { "boolean", }, description = "...")
    public Reliable(Source source, @Param(name = "estimates", type = { "double", "int", "long" }) Expression estimates) {
        super(source, List.of(estimates));
        this.estimates = estimates;
    }

    private Reliable(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(estimates);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(estimates, t -> t.isNumeric() && isRepresentable(t), sourceText(), SECOND, "numeric");
    }

    @Override
    public boolean foldable() {
        return estimates.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(estimates.dataType())) {
            case DOUBLE -> new ReliableDoubleEvaluator.Factory(source(), toEvaluator.apply(estimates));
            case INT -> new ReliableIntEvaluator.Factory(source(), toEvaluator.apply(estimates));
            case LONG -> new ReliableLongEvaluator.Factory(source(), toEvaluator.apply(estimates));
            default -> throw EsqlIllegalArgumentException.illegalDataType(estimates.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Reliable(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Reliable::new, estimates);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(estimates);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Reliable other = (Reliable) obj;
        return Objects.equals(other.estimates, estimates);
    }

    @Evaluator(extraName = "Double")
    static void process(BooleanBlock.Builder builder, @Position int position, DoubleBlock estimatesBlock) {
        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getDouble(estimatesBlock.getFirstValueIndex(position) + i);
        }
        builder.appendBoolean(computeReliable(estimates));
    }

    @Evaluator(extraName = "Int")
    static void process(BooleanBlock.Builder builder, @Position int position, IntBlock estimatesBlock) {
        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getInt(estimatesBlock.getFirstValueIndex(position) + i);
        }
        builder.appendBoolean(computeReliable(estimates));
    }

    @Evaluator(extraName = "Long")
    static void process(BooleanBlock.Builder builder, @Position int position, LongBlock estimatesBlock) {
        Number[] estimates = new Number[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getLong(estimatesBlock.getFirstValueIndex(position) + i);
        }
        builder.appendBoolean(computeReliable(estimates));
    }

    public static boolean computeReliable(Number[] estimates) {
        int N = estimates.length;
        if (N < 5) {
            return false;
        }
        Skewness skew = new Skewness();
        Kurtosis kurtosis = new Kurtosis();
        for (Number estimate : estimates) {
            skew.increment(estimate.doubleValue());
            kurtosis.increment(estimate.doubleValue());
        }
        double maxSkew = Math.sqrt(6.0 * N * (N - 1) / ((N - 2) * (N + 1) * (N + 3))) * 1.96;
        double maxKurtosis = Math.sqrt(24.0 * N * (N - 1) * (N - 1) / ((N - 3) * (N - 2) * (N + 3) * (N + 5))) * 1.96;
        return Math.abs(skew.getResult()) < maxSkew && Math.abs(kurtosis.getResult()) < maxKurtosis;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
