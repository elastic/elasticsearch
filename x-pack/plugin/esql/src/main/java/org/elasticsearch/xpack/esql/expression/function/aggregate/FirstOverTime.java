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
import org.elasticsearch.compute.aggregation.FirstDoubleByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstExponentialHistogramByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstFloatByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstIntByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstLongByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstTDigestByTimestampAggregatorFunctionSupplier;
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
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class FirstOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FirstOverTime",
        FirstOverTime::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(FirstOverTime.class)
        .ternary(FirstOverTime::new)
        .name("first_over_time");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(FirstOverTime::new)
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description("Returns the first value of each time series in the specified time range.")
        .example("first_over_time(http_requests_total[1h])")
        .name("first_over_time");

    private final Expression timestamp;

    // TODO: support all types
    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long", "integer", "double", "exponential_histogram", "tdigest" },
        description = "Calculates the earliest value of a field, where recency determined by the `@timestamp` field.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries", tag = "first_over_time") }
    )
    public FirstOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "counter_long", "counter_integer", "counter_double", "long", "integer", "double", "exponential_histogram", "tdigest" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the first over time value",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public FirstOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public FirstOverTime(StreamInput in) throws IOException {
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
    protected NodeInfo<FirstOverTime> info() {
        return NodeInfo.create(this, FirstOverTime::new, field(), filter(), window(), timestamp);
    }

    @Override
    public FirstOverTime replaceChildren(List<Expression> newChildren) {
        return new FirstOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public FirstOverTime withFilter(Expression filter) {
        return new FirstOverTime(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return field().dataType().noCounter();
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> (dt.noCounter().isNumeric() && dt != DataType.UNSIGNED_LONG)
                || dt == DataType.AGGREGATE_METRIC_DOUBLE
                || dt == DataType.EXPONENTIAL_HISTOGRAM
                || dt == DataType.TDIGEST,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long"
        ).and(
            isType(timestamp, dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), SECOND, "date_nanos or datetime")
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        // TODO: When processing TSDB data_streams they are sorted by `_tsid` and timestamp in descending order,
        // we can read the first encountered value for each group of `_tsid` and time bucket.
        final DataType type = field().dataType();
        return switch (type) {
            case LONG, COUNTER_LONG -> new FirstLongByTimestampAggregatorFunctionSupplier();
            case INTEGER, COUNTER_INTEGER -> new FirstIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE, COUNTER_DOUBLE -> new FirstDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new FirstFloatByTimestampAggregatorFunctionSupplier();
            case EXPONENTIAL_HISTOGRAM -> new FirstExponentialHistogramByTimestampAggregatorFunctionSupplier();
            case TDIGEST -> new FirstTDigestByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public FirstOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "first_over_time(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
