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
public class Acos extends AbstractTrigonometricFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Returns the {wikipedia}/Inverse_trigonometric_functions[arccosine] of `n` as an angle, expressed in radians.",
        examples = { @Example(file = "floats", tag = "acos") }
    )
    public Acos(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Number between -1 and 1. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new AcosEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Acos(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Acos::new, field());
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val) {
        if (Math.abs(val) > 1) {
            throw new ArithmeticException("Acos input out of range");
        }
        return Math.acos(val);
    }
}
