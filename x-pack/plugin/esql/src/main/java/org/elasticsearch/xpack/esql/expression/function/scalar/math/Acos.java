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
 * Inverse cosine trigonometric function.
 */
public class Acos extends AbstractTrigonometricFunction implements NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Acos", Acos::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Acos.class).unary(Acos::new).name("acos");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // The input is cast to double by AbstractTrigonometricFunction, so no ToDouble wrap is needed here; only the
        // non-finite-preserving flag is set so that out-of-range inputs follow IEEE-754 instead of being dropped.
        .unaryValueTransformation((source, field) -> new Acos(source, field, true))
        .description("Calculates the arccosine of all elements in the input vector.")
        .example("acos(some_metric)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.DOMAIN_PLUS_MINUS_ONE_NOTE)
        .name("acos");

    /**
     * When {@code true}, non-finite results ({@code NaN}/{@code ±Inf}) are returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that PromQL math follows IEEE-754
     * semantics; the default is {@code false}, meaning out-of-range inputs are rejected.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Returns the arccosine of a number.",
        description = "Returns the {wikipedia}/Inverse_trigonometric_functions[arccosine] of `n` as an angle, expressed in radians.",
        examples = { @Example(file = "floats", tag = "acos") }
    )
    public Acos(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Number between -1 and 1. If `null`, the function returns `null`."
        ) Expression n
    ) {
        this(source, n, false);
    }

    public Acos(Source source, Expression n, boolean allowNonFinite) {
        super(source, n);
        this.allowNonFinite = allowNonFinite;
    }

    private Acos(StreamInput in) throws IOException {
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
        return new AcosEvaluator.Factory(source(), field, allowNonFinite);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Acos(source(), newChildren.get(0), allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Acos::new, field(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Acos(source(), field(), false);
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val, @Fixed(includeInToString = false) boolean allowNonFinite) {
        if (allowNonFinite == false && Math.abs(val) > 1) {
            throw new ArithmeticException("Acos input out of range");
        }
        return Math.acos(val);
    }
}
