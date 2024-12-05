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

public class Signum extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Signum", Signum::new);

    @FunctionInfo(
        returnType = { "double" },
        description = "Returns the sign of the given number.\n"
            + "It returns `-1` for negative numbers, `0` for `0` and `1` for positive numbers.",
        examples = @Example(file = "math", tag = "signum")
    )
    public Signum(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Signum(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        var fieldType = field().dataType();

        if (fieldType == DataType.DOUBLE) {
            return new SignumDoubleEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.INTEGER) {
            return new SignumIntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.LONG) {
            return new SignumLongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return new SignumUnsignedLongEvaluator.Factory(source(), field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Signum(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Signum::new, field());
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Evaluator(extraName = "Double")
    static double process(double val) {
        return Math.signum(val);
    }

    @Evaluator(extraName = "Int")
    static double process(int val) {
        return Math.signum(val);
    }

    @Evaluator(extraName = "Long")
    static double process(long val) {
        return Math.signum(val);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long val) {
        return Math.signum(NumericUtils.unsignedLongToDouble(val));
    }
}
