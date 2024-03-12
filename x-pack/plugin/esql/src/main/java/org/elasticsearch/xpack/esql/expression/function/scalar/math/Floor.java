/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

/**
 * Round a number down to the nearest integer.
 * <p>
 *     Note that doubles are rounded down to the nearest valid double that is
 *     an integer ala {@link Math#floor}.
 * </p>
 */
public class Floor extends UnaryScalarFunction {
    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Round a number down to the nearest integer."
    )
    public Floor(Source source, @Param(name = "n", type = { "double", "integer", "long", "unsigned_long" }) Expression n) {
        super(source, n);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        if (dataType().isInteger()) {
            return toEvaluator.apply(field());
        }
        return new FloorDoubleEvaluator.Factory(source(), toEvaluator.apply(field()));
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Floor(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Floor::new, field());
    }

    @Evaluator(extraName = "Double")
    static double process(double val) {
        return Math.floor(val);
    }
}
