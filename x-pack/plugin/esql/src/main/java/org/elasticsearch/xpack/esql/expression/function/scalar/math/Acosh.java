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
import org.elasticsearch.core.ESSloppyMath;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.io.IOException;
import java.util.List;

/**
 * Inverse hyperbolic cosine function.
 */
public class Acosh extends AbstractTrigonometricFunction implements NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Acosh", Acosh::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Acosh.class).unary(Acosh::new).name("acosh");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // The input is cast to double by AbstractTrigonometricFunction, so no ToDouble wrap is needed here; only the
        // non-finite-preserving flag is set so that out-of-range inputs follow IEEE-754 instead of being dropped.
        .unaryValueTransformation((source, field) -> new Acosh(source, field, true))
        .description("Calculates the inverse hyperbolic cosine of all elements in the input vector.")
        .example("acosh(some_metric)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(
            "For inputs below 1, {{es}} returns `null` and emits a warning, rather than the `NaN` that Prometheus returns."
        )
        .name("acosh");

    /**
     * When {@code true}, non-finite results ({@code NaN}/{@code ±Inf}) are returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that PromQL math follows IEEE-754
     * semantics; the default is {@code false}, meaning out-of-range inputs are rejected.
     */
    private final boolean allowNonFinite;

    private static final double LN2 = Math.log(2);
    private static final double LARGE = (double) (1L << 28);

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Returns the inverse hyperbolic cosine of a number.",
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
        this(source, n, false);
    }

    public Acosh(Source source, Expression n, boolean allowNonFinite) {
        super(source, n);
        this.allowNonFinite = allowNonFinite;
    }

    private Acosh(StreamInput in) throws IOException {
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
    protected ExpressionEvaluator.Factory doubleEvaluator(ExpressionEvaluator.Factory field) {
        return new AcoshEvaluator.Factory(source(), field, allowNonFinite);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Acosh(source(), newChildren.getFirst(), allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Acosh::new, field(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Acosh(source(), field(), false);
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val, @Fixed(includeInToString = false) boolean allowNonFinite) {
        if (allowNonFinite == false && val < 1.0) {
            throw new ArithmeticException("Acosh input out of range");
        }
        // This implementation is derived from the go version:
        // https://github.com/golang/go/blob/master/src/math/acosh.go
        return ESSloppyMath.acosh(val);
    }
}
