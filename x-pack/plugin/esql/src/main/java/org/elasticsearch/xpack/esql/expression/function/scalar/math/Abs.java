/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;

public class Abs extends UnaryScalarFunction {
    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Returns the absolute value.",
        examples = { @Example(file = "math", tag = "abs"), @Example(file = "math", tag = "abs-employees") }
    )
    public Abs(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    @Evaluator(extraName = "Double")
    static double process(double fieldVal) {
        return Math.abs(fieldVal);
    }

    @Evaluator(extraName = "Long")
    static long process(long fieldVal) {
        return Math.absExact(fieldVal);
    }

    @Evaluator(extraName = "Int")
    static int process(int fieldVal) {
        return Math.absExact(fieldVal);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var field = toEvaluator.apply(field());
        if (dataType() == DataTypes.DOUBLE) {
            return new AbsDoubleEvaluator.Factory(source(), field);
        }
        if (dataType() == DataTypes.UNSIGNED_LONG) {
            return field;
        }
        if (dataType() == DataTypes.LONG) {
            return new AbsLongEvaluator.Factory(source(), field);
        }
        if (dataType() == DataTypes.INTEGER) {
            return new AbsIntEvaluator.Factory(source(), field);
        }
        throw EsqlIllegalArgumentException.illegalDataType(dataType());
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Abs(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Abs::new, field());
    }
}
