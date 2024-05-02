/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * Inverse cosine trigonometric function.
 */
public class Atan extends AbstractTrigonometricFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Returns the {wikipedia}/Inverse_trigonometric_functions[arctangent] of the input\n"
            + "numeric expression as an angle, expressed in radians.",
        examples = @Example(file = "floats", tag = "atan")
    )
    public Atan(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new AtanEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Atan(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Atan::new, field());
    }

    @Evaluator
    static double process(double val) {
        return Math.atan(val);
    }
}
