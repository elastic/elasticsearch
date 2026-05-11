/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IrateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IrateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IrateLongAggregatorFunctionSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
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

public class Irate extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        ToAggregator,
        TimestampAware,
        TemporalityAware,
        TransportVersionAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Irate_v2",
        Irate::readFrom
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Irate.class)
        .ternary(Irate::createWithImplicitTemporality)
        .name("irate");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(Irate::createWithImplicitTemporality)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.REQUIRED)
        .description("Calculates the per-second instant rate of increase based on the last two data points.")
        .example("irate(http_requests_total[5m])")
        .name("irate");

    private static final TransportVersion IRATE_V2 = TransportVersion.fromName("esql_irate_v2");

    private final Expression timestamp;
    private final Expression temporality;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the irate of a counter field. irate is the per-second rate of increase between the last two data points ("
            + "it ignores all but the last two data points in each time period). "
            + "This function is very similar to rate, but is more responsive to recent changes in the rate of increase.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries-irate", tag = "irate") }
    )
    public Irate(
        Source source,
        @Param(
            name = "field",
            type = { "counter_long", "counter_integer", "counter_double" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the irate",
            optional = true
        ) Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp, temporality);
    }

    public static Irate createWithImplicitTemporality(Source source, Expression field, Expression window, Expression timestamp) {
        return new Irate(source, field, window, timestamp, null);
    }

    public Irate(
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

    private static Irate readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new Irate(source, field, filter, window, parameters.getFirst(), parameters.size() > 1 ? parameters.get(1) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Irate> info() {
        if (temporality != null) {
            return NodeInfo.create(this, Irate::new, field(), filter(), window(), timestamp, temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window, timestamp) -> new Irate(source, field, filter, window, timestamp, null),
                field(),
                filter(),
                window(),
                timestamp
            );
        }
    }

    @Override
    public Irate replaceChildren(List<Expression> newChildren) {
        return new Irate(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.size() > 4 ? newChildren.get(4) : null
        );
    }

    @Override
    public Irate withFilter(Expression filter) {
        return new Irate(source(), field(), filter, window(), timestamp, temporality);
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
            case COUNTER_LONG -> new IrateLongAggregatorFunctionSupplier(source(), isDateNanos);
            case COUNTER_INTEGER -> new IrateIntAggregatorFunctionSupplier(source(), isDateNanos);
            case COUNTER_DOUBLE -> new IrateDoubleAggregatorFunctionSupplier(source(), isDateNanos);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Irate perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "irate(" + field() + ", " + timestamp() + ", " + temporality() + ")";
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
    public Irate withTemporality(Expression newTemporality) {
        return new Irate(source(), field(), filter(), window(), timestamp(), newTemporality);
    }

    @Override
    public Expression forTransportVersion(TransportVersion minTransportVersion) {
        if (minTransportVersion.supports(IRATE_V2) == false) {
            // For older nodes in the cluster / CCS we need to fallback to the legacy implementation for compatibility
            return new LegacyIrate(source(), field(), filter(), window(), timestamp(), temporality());
        }
        return this;
    }
}
