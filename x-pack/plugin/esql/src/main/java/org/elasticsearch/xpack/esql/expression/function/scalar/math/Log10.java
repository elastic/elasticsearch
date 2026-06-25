/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class Log10 extends UnaryScalarFunction implements NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Log10", Log10::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Log10.class).unary(Log10::new).name("log10");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .unaryNonFiniteValueTransformation(Log10::new)
        .description("Calculates the decimal logarithm for all elements in the input vector.")
        .example("log10(http_requests_total)")
        .name("log10");

    /**
     * When {@code true}, non-finite results ({@code NaN}/{@code ±Inf}) are returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that PromQL math follows IEEE-754
     * semantics (e.g. {@code log10(0)} returns {@code -Inf} and {@code log10(-x)} returns {@code NaN}); the
     * default is {@code false}, meaning non-finite values are rejected.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Returns the base-10 logarithm of a number.",
        description = "Returns the logarithm of a value to base 10. The input can "
            + "be any numeric value, the return value is always a double.\n"
            + "\n"
            + "Logs of 0 and negative numbers return `null` as well as a warning.",
        examples = @Example(file = "math", tag = "log10")
    )
    public Log10(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression n
    ) {
        this(source, n, false);
    }

    public Log10(Source source, Expression n, boolean allowNonFinite) {
        super(source, n);
        this.allowNonFinite = allowNonFinite;
    }

    private Log10(StreamInput in) throws IOException {
        super(in);
        this.allowNonFinite = NonFiniteSupport.readNonFinite(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeNonFinite(out);
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
            return new Log10DoubleEvaluator.Factory(source(), field, allowNonFinite);
        }
        if (fieldType == DataType.INTEGER) {
            return new Log10IntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.LONG) {
            return new Log10LongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataType.UNSIGNED_LONG) {
            return new Log10UnsignedLongEvaluator.Factory(source(), field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Evaluator(extraName = "Double", warnExceptions = ArithmeticException.class)
    static double process(double val, @Fixed(includeInToString = false) boolean allowNonFinite) {
        if (allowNonFinite == false && val <= 0d) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log10(val);
    }

    @Evaluator(extraName = "Long", warnExceptions = ArithmeticException.class)
    static double process(long val) {
        if (val <= 0L) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log10(val);
    }

    @Evaluator(extraName = "UnsignedLong", warnExceptions = ArithmeticException.class)
    static double processUnsignedLong(long val) {
        if (val == NumericUtils.ZERO_AS_UNSIGNED_LONG) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log10(unsignedLongToDouble(val));
    }

    @Evaluator(extraName = "Int", warnExceptions = ArithmeticException.class)
    static double process(int val) {
        if (val <= 0) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log10(val);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Log10(source(), newChildren.get(0), allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Log10::new, field(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Log10(source(), field(), false);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }
}
