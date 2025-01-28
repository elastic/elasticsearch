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

public class Cbrt extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Cbrt", Cbrt::new);

    @FunctionInfo(returnType = "double", description = """
        Returns the cube root of a number. The input can be any numeric value, the return value is always a double.
        Cube roots of infinities are null.""", examples = @Example(file = "math", tag = "cbrt"))
    public Cbrt(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        super(source, n);
    }

    private Cbrt(StreamInput in) throws IOException {
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
            return new CbrtDoubleEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.INTEGER) {
            return new CbrtIntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.LONG) {
            return new CbrtLongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return new CbrtUnsignedLongEvaluator.Factory(source(), field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Evaluator(extraName = "Double", warnExceptions = ArithmeticException.class)
    static double process(double val) {
        return Math.cbrt(val);
    }

    @Evaluator(extraName = "Long", warnExceptions = ArithmeticException.class)
    static double process(long val) {
        return Math.cbrt(val);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long val) {
        return Math.cbrt(unsignedLongToDouble(val));
    }

    @Evaluator(extraName = "Int", warnExceptions = ArithmeticException.class)
    static double process(int val) {
        return Math.cbrt(val);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Cbrt(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Cbrt::new, field());
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
