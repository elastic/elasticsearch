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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
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
public class Increase extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Increase", Increase::new);

    private final Expression timestamp;

    /**
     * When true, accepts any numeric type (for PromQL compatibility).
     * When false, requires counter types only (ESQL TS behavior).
     */
    private final boolean lenientTypeCheck;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the absolute increase of a counter field in a time window.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
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
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, false);
    }

    public Increase(Source source, Expression field, Expression window, Expression timestamp, boolean lenientTypeCheck) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, lenientTypeCheck);
    }

    public Increase(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        this(source, field, filter, window, timestamp, false);
    }

    public Increase(Source source, Expression field, Expression filter, Expression window, Expression timestamp, boolean lenientTypeCheck) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
        this.lenientTypeCheck = lenientTypeCheck;
    }

    public Increase(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class).getFirst(),
            false
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Increase> info() {
        return NodeInfo.create(this, Increase::new, field(), filter(), window(), timestamp, lenientTypeCheck);
    }

    @Override
    public Increase replaceChildren(List<Expression> newChildren) {
        return new Increase(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3), lenientTypeCheck);
    }

    @Override
    public Increase withFilter(Expression filter) {
        return new Increase(source(), field(), filter, window(), timestamp, lenientTypeCheck);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (lenientTypeCheck) {
            return isType(field(), dt -> dt.isNumeric() || DataType.isCounter(dt), sourceText(), FIRST, "numeric or counter");
        }
        return isType(field(), dt -> DataType.isCounter(dt), sourceText(), FIRST, "counter_long", "counter_integer", "counter_double");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        final DataType tsType = timestamp().dataType();
        final boolean isDateNanos = tsType == DataType.DATE_NANOS;
        return switch (type) {
            case COUNTER_LONG, LONG -> new RateLongGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos);
            case COUNTER_INTEGER, INTEGER -> new RateIntGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos);
            case COUNTER_DOUBLE, DOUBLE -> new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, isDateNanos);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Increase perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "increase(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public boolean requiredTimeSeriesSource() {
        return true;
    }
}
