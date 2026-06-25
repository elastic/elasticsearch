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
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;

public class Log extends EsqlScalarFunction implements OptionalArgument, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Log", Log::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Log.class).binary(Log::new).name("log");
    // Log casts both operands to double internally, so no ToDouble wrap is needed here; only the non-finite-preserving
    // flag is set so that PromQL log2/ln of non-positive inputs follow IEEE-754 (NaN/-Inf) instead of being dropped.
    public static final PromqlFunctionDefinition PROMQL_LOG2_DEFINITION = PromqlFunctionDefinition.def()
        .unaryValueTransformation((source, value) -> new Log(source, Literal.fromDouble(source, 2d), value, true))
        .description("Calculates the binary logarithm for all elements in the input vector.")
        .example("log2(memory_usage_bytes)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.LOG_DOMAIN_NOTE)
        .name("log2");
    public static final PromqlFunctionDefinition PROMQL_LN_DEFINITION = PromqlFunctionDefinition.def()
        .unaryValueTransformation((source, value) -> new Log(source, value, null, true))
        .description("Calculates the natural logarithm for all elements in the input vector.")
        .example("ln(memory_usage_bytes)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.LOG_DOMAIN_NOTE)
        .name("ln");

    private final Expression base;
    private final Expression value;

    /**
     * When {@code true}, non-finite results ({@code NaN}/{@code ±Inf}) are returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that PromQL log functions follow IEEE-754
     * semantics (e.g. {@code ln(0)} returns {@code -Inf} and {@code ln(-x)} returns {@code NaN}); the default is
     * {@code false}, meaning non-positive inputs are rejected.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Returns the logarithm of a value to a base.",
        description = "Returns the logarithm of a value to a base. The input can be any numeric value, "
            + "the return value is always a double.\n"
            + "\n"
            + "Logs of zero, negative numbers, and base of one return `null` as well as a warning.",
        examples = { @Example(file = "math", tag = "log"), @Example(file = "math", tag = "logUnary") }
    )
    public Log(
        Source source,
        @Param(
            name = "base",
            type = { "integer", "unsigned_long", "long", "double" },
            description = "Base of logarithm. If `null`, the function returns `null`. "
                + "If not provided, this function returns the natural logarithm (base e) of a value.",
            optional = true
        ) Expression base,
        @Param(
            name = "number",
            type = { "integer", "unsigned_long", "long", "double" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression value
    ) {
        this(source, base, value, false);
    }

    public Log(Source source, Expression base, Expression value, boolean allowNonFinite) {
        super(source, value != null ? Arrays.asList(base, value) : Arrays.asList(base));
        this.value = value != null ? value : base;
        this.base = value != null ? base : null;
        this.allowNonFinite = allowNonFinite;
    }

    private Log(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            NonFiniteSupport.readNonFinite(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        assert children().size() == 1 || children().size() == 2;
        out.writeNamedWriteable(children().get(0));
        out.writeOptionalNamedWriteable(children().size() == 2 ? children().get(1) : null);
        writeNonFinite(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (base != null) {
            TypeResolution resolution = isNumeric(base, sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return isNumeric(value, sourceText(), base != null ? SECOND : FIRST);
    }

    @Override
    public boolean foldable() {
        return (base == null || base.foldable()) && value.foldable();
    }

    @Evaluator(extraName = "Constant", warnExceptions = { ArithmeticException.class })
    static double process(double value, @Fixed(includeInToString = false) boolean allowNonFinite) throws ArithmeticException {
        if (allowNonFinite == false && value <= 0d) {
            throw new ArithmeticException("Log of non-positive number");
        }
        return Math.log(value);
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double base, double value, @Fixed(includeInToString = false) boolean allowNonFinite) throws ArithmeticException {
        if (allowNonFinite == false) {
            if (base <= 0d || value <= 0d) {
                throw new ArithmeticException("Log of non-positive number");
            }
            if (base == 1d) {
                throw new ArithmeticException("Log of base 1");
            }
        }
        return Math.log10(value) / Math.log10(base);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Log(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null, allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        Expression b = base != null ? base : value;
        Expression v = base != null ? value : null;
        return NodeInfo.create(this, Log::new, b, v, allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        // The (base, value) arguments are reconstructed the same way as in info() to preserve the unary/binary form.
        Expression b = base != null ? base : value;
        Expression v = base != null ? value : null;
        return new Log(source(), b, v, false);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var valueEval = Cast.cast(source(), value.dataType(), DataType.DOUBLE, toEvaluator.apply(value));
        if (base != null) {
            var baseEval = Cast.cast(source(), base.dataType(), DataType.DOUBLE, toEvaluator.apply(base));
            return new LogEvaluator.Factory(source(), baseEval, valueEval, allowNonFinite);
        }
        return new LogConstantEvaluator.Factory(source(), valueEval, allowNonFinite);
    }

    Expression value() {
        return value;
    }

    Expression base() {
        return base;
    }
}
