/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Converts from <a href="https://en.wikipedia.org/wiki/Radian">radians</a>
 * to <a href="https://en.wikipedia.org/wiki/Degree_(angle)">degrees</a>.
 */
public class ToDegrees extends AbstractConvertFunction implements EvaluatorMapper, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToDegrees",
        ToDegrees::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToDegrees.class).unary(ToDegrees::new).name("to_degrees");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // Non-double inputs are cast to double by the factories below, so no ToDouble wrap is needed here; only the
        // non-finite-preserving flag is set so that NaN/±Inf follow IEEE-754 instead of being dropped.
        .unaryValueTransformation((source, field) -> new ToDegrees(source, field, true))
        .description("Converts input values from radians to degrees for all elements in the input vector.")
        .example("deg(some_metric)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(
            "For `NaN` or infinite inputs, {{es}} returns `null` and emits a warning, instead of returning the value unchanged."
        )
        .name("deg");

    private static final Map<DataType, BuildFactory> EVALUATORS = buildEvaluators(false);
    private static final Map<DataType, BuildFactory> EVALUATORS_NON_FINITE = buildEvaluators(true);

    private static Map<DataType, BuildFactory> buildEvaluators(boolean allowNonFinite) {
        return Map.ofEntries(
            Map.entry(DOUBLE, (source, field) -> new ToDegreesEvaluator.Factory(source, field, allowNonFinite)),
            Map.entry(
                INTEGER,
                (source, field) -> new ToDegreesEvaluator.Factory(
                    source,
                    new ToDoubleFromIntEvaluator.Factory(source, field),
                    allowNonFinite
                )
            ),
            Map.entry(
                LONG,
                (source, field) -> new ToDegreesEvaluator.Factory(
                    source,
                    new ToDoubleFromLongEvaluator.Factory(source, field),
                    allowNonFinite
                )
            ),
            Map.entry(
                UNSIGNED_LONG,
                (source, field) -> new ToDegreesEvaluator.Factory(
                    source,
                    new ToDoubleFromUnsignedLongEvaluator.Factory(source, field),
                    allowNonFinite
                )
            )
        );
    }

    /**
     * When {@code true}, non-finite results ({@code NaN}/{@code ±Inf}) are returned as-is instead of being rejected
     * to {@code null}. Set only by the PromQL translation so that PromQL math follows IEEE-754 semantics; the default
     * is {@code false}, preserving ES|QL's finite-only contract.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Converts a number in radians to degrees.",
        description = "Converts a number in {wikipedia}/Radian[radians] to {wikipedia}/Degree_(angle)[degrees].",
        examples = @Example(file = "floats", tag = "to_degrees")
    )
    public ToDegrees(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        this(source, field, false);
    }

    public ToDegrees(Source source, Expression field, boolean allowNonFinite) {
        super(source, field);
        this.allowNonFinite = allowNonFinite;
    }

    private ToDegrees(StreamInput in) throws IOException {
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
    protected Map<DataType, BuildFactory> factories() {
        return allowNonFinite ? EVALUATORS_NON_FINITE : EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDegrees(source(), newChildren.get(0), allowNonFinite);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDegrees::new, field(), allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new ToDegrees(source(), field(), false);
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @ConvertEvaluator(warnExceptions = { ArithmeticException.class })
    static double process(double deg, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double result = Math.toDegrees(deg);
        return allowNonFinite ? result : NumericUtils.asFiniteNumber(result);
    }
}
