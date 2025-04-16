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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

public class Abs extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Abs", Abs::new);

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

    private Abs(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        if (dataType() == DataType.DOUBLE) {
            return new AbsDoubleEvaluator.Factory(source(), field);
        }
        if (dataType() == DataType.UNSIGNED_LONG) {
            return field;
        }
        if (dataType() == DataType.LONG) {
            return new AbsLongEvaluator.Factory(source(), field);
        }
        if (dataType() == DataType.INTEGER) {
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
