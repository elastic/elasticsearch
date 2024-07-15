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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Returns the value of e raised to the power of tbe number specified as parameter
 */
public class Exp extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Exp", Exp::new);

    @FunctionInfo(
        returnType = { "double" },
        description = "Returns the value of e raised to the power of the given number.",
        examples = @Example(file = "math", tag = "exp")
    )
    public Exp(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Exp(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        var field = toEvaluator.apply(field());
        var fieldType = field().dataType();

        if (fieldType == DataType.DOUBLE) {
            return new ExpDoubleEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.INTEGER) {
            return new ExpIntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.LONG) {
            return new ExpLongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return new ExpUnsignedLongEvaluator.Factory(source(), field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Exp(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Exp::new, field());
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Evaluator(extraName = "Double")
    static double process(double val) {
        return Math.exp(val);
    }

    @Evaluator(extraName = "Int")
    static double process(int val) {
        return Math.exp(val);
    }

    @Evaluator(extraName = "Long")
    static double process(long val) {
        return Math.exp(val);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long val) {
        return Math.exp(NumericUtils.unsignedLongToDouble(val));
    }
}
