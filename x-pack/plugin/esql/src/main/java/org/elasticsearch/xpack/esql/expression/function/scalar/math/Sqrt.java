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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class Sqrt extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sqrt", Sqrt::new);

    @FunctionInfo(returnType = "double", description = """
        Returns the square root of a number. The input can be any numeric value, the return value is always a double.
        Square roots of negative numbers and infinities are null.""", examples = @Example(file = "math", tag = "sqrt"))
    public Sqrt(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Sqrt(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        var fieldType = field().dataType();

        if (fieldType == DataType.DOUBLE) {
            return new SqrtDoubleEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.INTEGER) {
            return new SqrtIntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.LONG) {
            return new SqrtLongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return new SqrtUnsignedLongEvaluator.Factory(source(), field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Evaluator(extraName = "Double", warnExceptions = ArithmeticException.class)
    static double process(double val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Evaluator(extraName = "Long", warnExceptions = ArithmeticException.class)
    static double process(long val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long val) {
        return Math.sqrt(unsignedLongToDouble(val));
    }

    @Evaluator(extraName = "Int", warnExceptions = ArithmeticException.class)
    static double process(int val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Sqrt(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Sqrt::new, field());
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }
}
