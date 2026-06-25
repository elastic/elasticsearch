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
 * Cosine hyperbolic function.
 */
public class Cosh extends AbstractTrigonometricFunction implements NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Cosh", Cosh::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Cosh.class).unary(Cosh::new).name("cosh");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // The input is cast to double by AbstractTrigonometricFunction, so no ToDouble wrap is needed here; only the
        // non-finite-preserving flag is set so that an overflowing result follows IEEE-754 (+Inf) instead of being dropped.
        .unaryValueTransformation((source, field) -> new Cosh(source, field, true))
        .description("Calculates the hyperbolic cosine of all elements in the input vector.")
        .example("cosh(some_metric)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.OVERFLOW_NOTE)
        .name("cosh");

    /**
     * When {@code true}, an overflowing result ({@code +Inf}) is returned as-is instead of being rejected to
     * {@code null}. Set only by the PromQL translation so that PromQL math follows IEEE-754 semantics; the default
     * is {@code false}, meaning overflow is rejected.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Returns the hyperbolic cosine of a number.",
        description = "Returns the {wikipedia}/Hyperbolic_functions[hyperbolic cosine] of a number.",
        examples = @Example(file = "floats", tag = "cosh")
    )
    public Cosh(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric expression. If `null`, the function returns `null`."

        ) Expression angle
    ) {
        this(source, angle, false);
    }

    public Cosh(Source source, Expression angle, boolean allowNonFinite) {
        super(source, angle);
        this.allowNonFinite = allowNonFinite;
    }

    private Cosh(StreamInput in) throws IOException {
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
        return new CoshEvaluator.Factory(source(), field, allowNonFinite);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Cosh(source(), newChildren.get(0), allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Cosh::new, field(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Cosh(source(), field(), false);
    }

    @Evaluator(warnExceptions = ArithmeticException.class)
    static double process(double val, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double res = Math.cosh(val);
        if (allowNonFinite == false && (Double.isNaN(res) || Double.isInfinite(res))) {
            throw new ArithmeticException("cosh overflow");
        }
        return res;
    }
}
