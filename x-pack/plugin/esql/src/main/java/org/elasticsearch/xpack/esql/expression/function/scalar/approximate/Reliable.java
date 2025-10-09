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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;

/**
 * This function is used internally by {@link Approximate}, and is not exposed
 * to users via the {@link EsqlFunctionRegistry}.
 */
public class Reliable extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Reliable", Reliable::new);

    private final Expression estimates;
    private final Expression trialCount;
    private final Expression bucketCount;

    @FunctionInfo(returnType = { "boolean", }, description = "...")
    public Reliable(
        Source source,
        @Param(name = "estimates", type = { "double", "int", "long" }) Expression estimates,
        @Param(name = "trialCount", type = { "int" }) Expression trialCount,
        @Param(name = "bucketCount", type = { "int" }) Expression bucketCount
    ) {
        super(source, List.of(estimates));
        this.estimates = estimates;
        this.trialCount = trialCount;
        this.bucketCount = bucketCount;
    }

    private Reliable(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(estimates);
        out.writeNamedWriteable(trialCount);
        out.writeNamedWriteable(bucketCount);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(estimates, t -> t.isNumeric() && isRepresentable(t), sourceText(), SECOND, "numeric").and(
            isType(trialCount, t -> t == DataType.INTEGER, sourceText(), THIRD, "integer")
        ).and(isType(bucketCount, t -> t == DataType.INTEGER, sourceText(), FOURTH, "integer"));
    }

    @Override
    public boolean foldable() {
        return estimates.foldable() && trialCount.foldable() && bucketCount.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(estimates.dataType())) {
            case DOUBLE -> new ReliableDoubleEvaluator.Factory(
                source(),
                toEvaluator.apply(estimates),
                toEvaluator.apply(trialCount),
                toEvaluator.apply(bucketCount)
            );
            case INT -> new ReliableIntEvaluator.Factory(
                source(),
                toEvaluator.apply(estimates),
                toEvaluator.apply(trialCount),
                toEvaluator.apply(bucketCount)
            );
            case LONG -> new ReliableLongEvaluator.Factory(
                source(),
                toEvaluator.apply(estimates),
                toEvaluator.apply(trialCount),
                toEvaluator.apply(bucketCount)
            );
            default -> throw EsqlIllegalArgumentException.illegalDataType(estimates.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Reliable(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Reliable::new, estimates, trialCount, bucketCount);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(estimates, trialCount, bucketCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Reliable other = (Reliable) obj;
        return Objects.equals(other.estimates, estimates)
            && Objects.equals(other.trialCount, trialCount)
            && Objects.equals(other.bucketCount, bucketCount);
    }

    @Evaluator(extraName = "Double")
    static void process(
        BooleanBlock.Builder builder,
        @Position int position,
        DoubleBlock estimatesBlock,
        IntBlock trialCountBlock,
        IntBlock bucketCountBlock
    ) {
        if (trialCountBlock.getValueCount(position) != 1 || bucketCountBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        double[] estimates = new double[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getDouble(estimatesBlock.getFirstValueIndex(position) + i);
        }
        int trialCount = trialCountBlock.getInt(trialCountBlock.getFirstValueIndex(position));
        int bucketCount = bucketCountBlock.getInt(bucketCountBlock.getFirstValueIndex(position));
        builder.appendBoolean(computeReliable(estimates, trialCount, bucketCount));
    }

    @Evaluator(extraName = "Int")
    static void process(
        BooleanBlock.Builder builder,
        @Position int position,
        IntBlock estimatesBlock,
        IntBlock trialCountBlock,
        IntBlock bucketCountBlock
    ) {
        if (trialCountBlock.getValueCount(position) != 1 || bucketCountBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        double[] estimates = new double[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getInt(estimatesBlock.getFirstValueIndex(position) + i);
        }
        int trialCount = trialCountBlock.getInt(trialCountBlock.getFirstValueIndex(position));
        int bucketCount = bucketCountBlock.getInt(bucketCountBlock.getFirstValueIndex(position));
        builder.appendBoolean(computeReliable(estimates, trialCount, bucketCount));
    }

    @Evaluator(extraName = "Long")
    static void process(
        BooleanBlock.Builder builder,
        @Position int position,
        LongBlock estimatesBlock,
        IntBlock trialCountBlock,
        IntBlock bucketCountBlock
    ) {
        if (trialCountBlock.getValueCount(position) != 1 || bucketCountBlock.getValueCount(position) != 1) {
            builder.appendNull();
            return;
        }
        double[] estimates = new double[estimatesBlock.getValueCount(position)];
        for (int i = 0; i < estimatesBlock.getValueCount(position); i++) {
            estimates[i] = estimatesBlock.getLong(estimatesBlock.getFirstValueIndex(position) + i);
        }
        int trialCount = trialCountBlock.getInt(trialCountBlock.getFirstValueIndex(position));
        int bucketCount = bucketCountBlock.getInt(bucketCountBlock.getFirstValueIndex(position));
        builder.appendBoolean(computeReliable(estimates, trialCount, bucketCount));
    }

    public static boolean computeReliable(double[] estimates, int trialCount, int B) {
        if (B < 5) {
            return false;
        }
        double maxSkew = Math.sqrt(6.0 * B * (B - 1) / ((B - 2) * (B + 1) * (B + 3))) * 1.96;
        double maxKurtosis = Math.sqrt(24.0 * B * (B - 1) * (B - 1) / ((B - 3) * (B - 2) * (B + 3) * (B + 5))) * 1.96;
        int reliableCount = 0;
        for (int trial = 0; trial < trialCount; trial++) {
            Skewness skew = new Skewness();
            Kurtosis kurtosis = new Kurtosis();
            for (int bucket = 0; bucket < B; bucket++) {
                double estimate = estimates[trial * B + bucket];
                skew.increment(estimate);
                kurtosis.increment(estimate);
            }
            if (Math.abs(skew.getResult()) < maxSkew && Math.abs(kurtosis.getResult()) < maxKurtosis) {
                reliableCount++;
            }
        }
        System.out.println("estimates = " + Arrays.toString(estimates) + " -> reliableCount = " + reliableCount + " / " + trialCount);
        return 2 * reliableCount > trialCount;
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
