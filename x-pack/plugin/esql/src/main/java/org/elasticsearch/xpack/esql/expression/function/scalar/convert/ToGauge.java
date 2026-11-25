/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Converts a counter-typed value to its plain numeric (gauge) equivalent.
 * The output type depends on the input: {@code counter_long} becomes {@code long},
 * {@code counter_integer} becomes {@code integer}, and {@code counter_double} becomes {@code double}.
 * Plain numeric inputs are returned unchanged (idempotent).
 * {@code aggregate_metric_double} inputs are also returned unchanged (idempotent).
 * No values are modified; this is a pure type-annotation change.
 */
public class ToGauge extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToGauge", ToGauge::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToGauge.class).unary(ToGauge::new).name("to_gauge");

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.of(
        LONG,
        (source, field) -> field,
        INTEGER,
        (source, field) -> field,
        DOUBLE,
        (source, field) -> field,
        COUNTER_LONG,
        (source, field) -> field,
        COUNTER_INTEGER,
        (source, field) -> field,
        COUNTER_DOUBLE,
        (source, field) -> field,
        AGGREGATE_METRIC_DOUBLE,
        (source, field) -> field
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.5.0") },
        returnType = { "long", "integer", "double", "aggregate_metric_double" },
        briefSummary = "Converts a counter value to its gauge numeric equivalent.",
        description = """
            Converts a counter value to its gauge (plain numeric) equivalent. The output type is determined by the input:
            `counter_long` converts to `long`, `counter_integer` to `integer`, and `counter_double` to `double`.
            No values are modified; only the type annotation changes. If the input is already a plain numeric or \
            `aggregate_metric_double`, the function is a no-op. This is useful when a metric field was misclassified as a \
            counter type instead of a plain numeric (gauge) in the index mapping.
            This function is also available as the `::gauge` cast operator.""",
        appendix = """
            ::::{warning}
            Applying `TO_GAUGE` to a field that is a genuine monotonically increasing counter, rather than a \
            misclassified gauge, will produce raw cumulative counter values instead of gauge samples. \
            Results from aggregations on such values are not meaningful.
            ::::""",
        examples = @Example(file = "k8s-timeseries-avg-over-time", tag = "toGauge")
    )
    public ToGauge(
        Source source,
        @Param(
            name = "field",
            type = { "integer", "counter_integer", "long", "counter_long", "double", "counter_double", "aggregate_metric_double" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToGauge(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    /**
     * Returns the plain numeric variant of the input type: {@code counter_long→long}, etc.
     * Plain numeric inputs pass through unchanged. Returns the input type unchanged when it
     * is not yet resolved or has no counter variant to strip ({@link DataType#noCounter()} default branch).
     */
    @Override
    public DataType dataType() {
        return field().dataType().noCounter();
    }

    /**
     * Returns {@code true} when {@code TO_GAUGE} would be a no-op on every branch of a union field — every mapped type is
     * already a non-counter type (including {@link DataType#AGGREGATE_METRIC_DOUBLE}).
     */
    public static boolean isNoOpOnAllUnionTypes(TypeConflictedField tcf) {
        return tcf.types().stream().allMatch(type -> type.isCounter() == false);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToGauge(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToGauge::new, field());
    }
}
