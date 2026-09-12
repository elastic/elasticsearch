/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ChangesDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ChangesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ChangesLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;

/**
 * Implements the PromQL {@code changes()} range-vector function for per-series numeric values.
 */
public class Changes extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Changes", Changes::new);
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(Changes::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the number of times the value changed in each time series in a range vector.")
        .example("changes(process_start_time_seconds[1h])")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.COUNT_NOTE)
        .name("changes");

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long" },
        briefSummary = "Calculates the number of value changes in a time window.",
        description = "Calculates the number of times the value of a numeric metric field changes in a time window.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.7.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.8.0") }
    )
    public Changes(
        Source source,
        @Param(
            name = "field",
            type = { "long", "integer", "double", "counter_long", "counter_integer", "counter_double" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute changes",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public Changes(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Changes(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class).getFirst()
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Changes> info() {
        return NodeInfo.create(this, Changes::new, field(), filter(), window(), timestamp);
    }

    @Override
    public Changes replaceChildren(List<Expression> newChildren) {
        return new Changes(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Changes withFilter(Expression filter) {
        return new Changes(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> (dt.isNumeric() || DataType.isCounter(dt)) && dt != AGGREGATE_METRIC_DOUBLE,
            sourceText(),
            DEFAULT,
            "numeric or counter except aggregate_metric_double"
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return switch (field().dataType()) {
            case LONG, COUNTER_LONG -> new ChangesLongAggregatorFunctionSupplier();
            case INTEGER, COUNTER_INTEGER -> new ChangesIntAggregatorFunctionSupplier();
            case DOUBLE, COUNTER_DOUBLE -> new ChangesDoubleAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(field().dataType());
        };
    }

    @Override
    public Changes perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "changes(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
