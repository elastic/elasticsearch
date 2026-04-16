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
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Rate extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TemporalityAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Rate", Rate::readFrom);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Rate.class)
        .ternary(Rate::createWithImplicitTemporality)
        .name("rate");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(Rate::createWithImplicitTemporality)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.REQUIRED)
        .description("Calculates the per-second average rate of increase of the time series in the range vector.")
        .example("rate(http_requests_total[5m])")
        .name("rate");

    private final Expression timestamp;
    private final Expression temporality;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the per-second average rate of increase of a"
            + " [counter](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric). "
            + "Rate calculations account for breaks in monotonicity, such as counter resets when a service restarts, and extrapolate "
            + "values within each bucketed time interval. Rate is the most appropriate aggregate function for counters. It is only allowed "
            + "in a [STATS](/reference/query-languages/esql/commands/stats-by.md) command under a "
            + "[`TS`](/reference/query-languages/esql/commands/ts.md) source command, to be properly applied per time series.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries", tag = "rate") }
    )

    public Rate(
        Source source,
        @Param(
            name = "field",
            type = { "counter_long", "counter_integer", "counter_double" },
            description = "the counter field whose per-second average rate of increase is computed"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which the rate is computed",
            optional = true
        ) Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, temporality);
    }

    public static Rate createWithImplicitTemporality(Source source, Expression field, Expression window, Expression timestamp) {
        return new Rate(source, field, window, timestamp, null);
    }

    public Rate(
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

    private static Rate readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new Rate(source, field, filter, window, parameters.getFirst(), parameters.size() > 1 ? parameters.get(1) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Rate> info() {
        if (temporality != null) {
            return NodeInfo.create(this, Rate::new, field(), filter(), window(), timestamp, temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window, timestamp) -> new Rate(source, field, filter, window, timestamp, null),
                field(),
                filter(),
                window(),
                timestamp
            );
        }
    }

    @Override
    public Rate replaceChildren(List<Expression> newChildren) {
        return new Rate(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.size() > 4 ? newChildren.get(4) : null
        );
    }

    @Override
    public Rate withFilter(Expression filter) {
        return new Rate(source(), field(), filter, window(), timestamp, temporality);
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
            case COUNTER_LONG -> new RateLongGroupingAggregatorFunction.FunctionSupplier(true, isDateNanos);
            case COUNTER_INTEGER -> new RateIntGroupingAggregatorFunction.FunctionSupplier(true, isDateNanos);
            case COUNTER_DOUBLE -> new RateDoubleGroupingAggregatorFunction.FunctionSupplier(true, isDateNanos);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Rate perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "rate(" + field() + ", " + timestamp() + ", " + temporality() + ")";
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
    public Rate withTemporality(Expression newTemporality) {
        return new Rate(source(), field(), filter(), window(), timestamp, newTemporality);
    }

    @Override
    public boolean requiredTimeSeriesSource() {
        return true;
    }
}
