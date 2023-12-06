/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;

/**
 * Function that emits a pseudo random number.
 */
public class Random extends ScalarFunction implements EvaluatorMapper {

    @FunctionInfo(returnType = "double", description = "Returns a random double between 0 (included) and 1 (excluded)")
    public Random(Source source) {
        super(source, List.of());
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public final int hashCode() {
        return Random.class.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public final ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator) {
        return new RandomNoSeedEvaluator.Factory(source());
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Random(source());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }

    @Evaluator(extraName = "NoSeed")
    static double process() {
        return Randomness.get().nextDouble();
    }
}
