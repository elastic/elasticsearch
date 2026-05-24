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
import org.elasticsearch.compute.aggregation.RateDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.RateIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.RateLongGroupingAggregatorFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The {@code increase()} function calculates the absolute increase of a counter field in a time window.
 *
 * It is similar to the {@code rate()} function, but instead of calculating the per-second average rate of increase,
 * it calculates the total increase over the time window.
 */
public class Increase extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware, TemporalityAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Increase",
        Increase::readFrom
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Increase.class)
        .ternary(Increase::createWithImplicitTemporality)
        .name("increase");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(Increase::createWithImplicitTemporality)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.REQUIRED)
        .description("Calculates the increase in the time series in the range vector, adjusting for counter resets.")
        .example("increase(http_requests_total[5m])")
        .name("increase");

    private final Expression timestamp;
    private final Expression temporality;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the absolute increase of a counter field in a time window.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries-increase", tag = "increase") }
    )
    public Increase(
        Source source,
        @Param(
            name = "field",
            type = { "counter_long", "counter_integer", "counter_double" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the increase over time",
            optional = true
        ) Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, temporality);
    }

    public static Increase createWithImplicitTemporality(Source source, Expression field, Expression window, Expression timestamp) {
        return new Increase(source, field, window, timestamp, null);
    }

    public Increase(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        super(source, field, filter, window, temporality == null ? List.of(timestamp) : List.of(timestamp, temporality));
        this.timestamp = timestamp;
        this.temporality = temporality;
    }

    private static Increase readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new Increase(source, field, filter, window, parameters.getFirst(), parameters.size() > 1 ? parameters.get(1) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Increase> info() {
        if (temporality != null) {
            return NodeInfo.create(this, Increase::new, field(), filter(), window(), timestamp, temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window, timestamp) -> new Increase(source, field, filter, window, timestamp, null),
                field(),
                filter(),
                window(),
                timestamp
            );
        }
    }

    @Override
    public Increase replaceChildren(List<Expression> newChildren) {
        return new Increase(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.size() > 4 ? newChildren.get(4) : null
        );
    }

    @Override
    public Increase withFilter(Expression filter) {
        return new Increase(source(), field(), filter, window(), timestamp, temporality);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> DataType.isCounter(dt), sourceText(), FIRST, "counter_long", "counter_integer", "counter_double");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        final DataType tsType = timestamp().dataType();
        final boolean isDateNanos = tsType == DataType.DATE_NANOS;
        return switch (type) {
            case COUNTER_LONG -> new RateLongGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos, source());
            case COUNTER_INTEGER -> new RateIntGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos, source());
            case COUNTER_DOUBLE -> new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos, source());
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Increase perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "increase(" + field() + ", " + timestamp() + ", " + temporality() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public Expression temporality() {
        return temporality;
    }

    @Override
    public Increase withTemporality(Expression newTemporality) {
        return new Increase(source(), field(), filter(), window(), timestamp, newTemporality);
    }

    @Override
    public boolean requiredTimeSeriesSource() {
        return true;
    }
}
