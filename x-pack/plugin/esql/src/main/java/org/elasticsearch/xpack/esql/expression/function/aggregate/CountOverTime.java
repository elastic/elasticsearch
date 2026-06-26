/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;

/**
 * Similar to {@link Count}, but it is used to calculate the count of values over a time series from the given field.
 */
public class CountOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        AggregateMetricDoubleNativeSupport,
        SurrogateExpression,
        TimestampAware,
        ToAggregator {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(CountOverTime.class)
        .ternary(CountOverTime::new)
        .name("count_over_time");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(CountOverTime::new)
        .description("Returns the count of all values in the specified time range.")
        .example("count_over_time(http_requests_total[5m])")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.COUNT_NOTE)
        .name("count_over_time");

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long" },
        briefSummary = "Calculates the count over time value of a field.",
        description = "Calculates the count over time value of a field.",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries", tag = "count_over_time") }
    )
    public CountOverTime(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "date",
                "date_nanos",
                "double",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "long",
                "text",
                "unsigned_long",
                "version" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the count over time",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public CountOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("CountOverTime is not directly serializable");
    }

    @Override
    public CountOverTime withFilter(Expression filter) {
        return new CountOverTime(source(), field(), filter, window(), timestamp);
    }

    @Override
    protected NodeInfo<CountOverTime> info() {
        return NodeInfo.create(this, CountOverTime::new, field(), filter(), window(), timestamp);
    }

    @Override
    public CountOverTime replaceChildren(List<Expression> newChildren) {
        return new CountOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    protected TypeResolution resolveType() {
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return perTimeSeriesAggregation().supplier();
    }

    @Override
    public Count perTimeSeriesAggregation() {
        return new Count(source(), field(), filter(), window());
    }

    @Override
    public Expression surrogate() {
        if (field().dataType() == EXPONENTIAL_HISTOGRAM || field().dataType() == DataType.TDIGEST) {
            var mergeOverTime = new HistogramMergeOverTime(source(), field(), filter(), window(), timestamp);
            return new Coalesce(
                source(),
                new ToLong(source(), ExtractHistogramComponent.create(source(), mergeOverTime, HistogramBlock.Component.COUNT)),
                List.of(new Literal(source(), 0L, DataType.LONG))
            );
        }
        return null;
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
