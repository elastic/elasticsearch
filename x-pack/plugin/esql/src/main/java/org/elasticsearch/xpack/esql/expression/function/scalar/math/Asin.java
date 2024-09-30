/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

/**
 * Inverse cosine trigonometric function.
 */
public class Asin extends AbstractTrigonometricFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Asin", Asin::new);

    @FunctionInfo(
        returnType = "double",
        description = "Returns the {wikipedia}/Inverse_trigonometric_functions[arcsine] of the input\n"
            + "numeric expression as an angle, expressed in radians.",
        examples = @Example(file = "floats", tag = "asin")
    )
    public Asin(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Number between -1 and 1. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Asin(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new AsinEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Asin(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Asin::new, field());
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val) {
        if (Math.abs(val) > 1) {
            throw new ArithmeticException("Asin input out of range");
        }
        return Math.asin(val);
    }
}
