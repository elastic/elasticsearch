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
import org.elasticsearch.core.ESSloppyMath;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

/**
 * Inverse hyperbolic cosine function.
 */
public class Acosh extends AbstractTrigonometricFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Acosh", Acosh::new);

    private static final double LN2 = Math.log(2);
    private static final double LARGE = (double) (1L << 28);

    @FunctionInfo(
        returnType = "double",
        description = "Returns the {wikipedia}/Inverse_trigonometric_functions[inverse hyperbolic cosine] of a number.",
        examples = @Example(file = "floats", tag = "acosh")
    )
    public Acosh(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Number greater than or equal to 1. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Acosh(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new AcoshEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Acosh(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Acosh::new, field());
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val) {
        if (val < 1.0) {
            throw new ArithmeticException("Acosh input out of range");
        }
        // This implementation is derived from the go version:
        // https://github.com/golang/go/blob/master/src/math/acosh.go
        return ESSloppyMath.acosh(val);
    }
}
