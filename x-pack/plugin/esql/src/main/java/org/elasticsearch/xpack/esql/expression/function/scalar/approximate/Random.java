/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.approximate;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * This function is used internally by {@link Approximation}, and is not exposed
 * to users via the {@link EsqlFunctionRegistry}.
 */
public class Random extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Random", Random::new);

    @FunctionInfo(
        returnType = { "integer" },
        description = "Returns a pseudorandom number, uniformly distributed between 0 (inclusive) and bound (exclusive).",
        type = FunctionType.SCALAR
    )
    public Random(
        Source source,
        @Param(name = "bound", type = { "integer" }, description = "The upper bound (exclusive).") Expression bound
    ) {
        super(source, bound);
    }

    private Random(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Evaluator
    static int process(int bound) {
        return Randomness.get().nextInt(bound);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new RandomEvaluator.Factory(source(), toEvaluator.apply(field));
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(field, t -> t == DataType.INTEGER, sourceText(), DEFAULT, "int");
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Random(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Random::new, field());
    }

    @Override
    public boolean foldable() {
        return field.dataType() == DataType.NULL;
    }
}
