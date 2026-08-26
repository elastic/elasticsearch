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
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Converts a numeric value to its counter-typed equivalent.
 * The output type depends on the input: {@code long} becomes {@code counter_long},
 * {@code integer} becomes {@code counter_integer}, and {@code double} becomes {@code counter_double}.
 * Counter inputs are returned unchanged (idempotent).
 * No values are modified; this is a pure type-annotation change.
 */
public class ToCounter extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToCounter",
        ToCounter::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToCounter.class).unary(ToCounter::new).name("to_counter");

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
        (source, field) -> field
    );

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.5.0") },
        returnType = { "counter_long", "counter_integer", "counter_double" },
        briefSummary = "Converts a numeric value to its counter type equivalent.",
        description = """
            Converts a numeric value to its counter equivalent. The output type is determined by the input:
            `long` converts to `counter_long`, `integer` to `counter_integer`, and `double` to `counter_double`.
            No values are modified; only the type annotation changes. If the input is already a counter, the \
            function is a no-op. This is useful when a metric field was misclassified as a plain numeric type \
            instead of a counter in the index mapping.
            This function is also available as the `::counter` cast operator.""",
        appendix = """
            ::::{warning}
            Applying `TO_COUNTER` to a field that is a genuine gauge, rather than a misclassified counter, \
            will produce raw gauge values with counter semantics. Results from aggregations on such values \
            are not meaningful.
            ::::""",
        examples = @Example(file = "k8s-timeseries-rate", tag = "toCounter")
    )
    public ToCounter(
        Source source,
        @Param(
            name = "field",
            type = { "integer", "counter_integer", "long", "counter_long", "double", "counter_double" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToCounter(StreamInput in) throws IOException {
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
     * Returns the counter variant of the input type: {@code long→counter_long}, etc.
     * Counter inputs pass through unchanged. Returns {@link DataType#NULL} when the
     * field type is not yet resolved or has no counter variant.
     */
    @Override
    public DataType dataType() {
        DataType fieldType = field().dataType();
        if (fieldType.isCounter()) {
            return fieldType;
        }
        DataType counter = fieldType.counter();
        return counter != null ? counter : DataType.NULL;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToCounter(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToCounter::new, field());
    }
}
