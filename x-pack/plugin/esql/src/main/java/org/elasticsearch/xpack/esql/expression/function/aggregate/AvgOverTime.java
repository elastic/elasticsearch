/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

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
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;

/**
 * Similar to {@link Avg}, but it is used to calculate the average value over a time series of values from the given field.
 */
public class AvgOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        SurrogateExpression,
        TimestampAware,
        AggregateMetricDoubleNativeSupport {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(AvgOverTime.class)
        .ternary(AvgOverTime::new)
        .name("avg_over_time");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(AvgOverTime::new)
        .description("Returns the average value of all points in the specified time range.")
        .example("avg_over_time(http_requests_total[5m])")
        .name("avg_over_time");

    private final Expression timestamp;

    @FunctionInfo(
        returnType = "double",
        briefSummary = "Calculates the average over time of a numeric field.",
        description = "Calculates the average over time of a numeric field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries", tag = "avg_over_time") }
    )
    public AvgOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "aggregate_metric_double", "double", "integer", "long", "exponential_histogram", "tdigest" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the average",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public AvgOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    @Override
    protected TypeResolution resolveType() {
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("AvgOverTime is not directly serializable");
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    protected NodeInfo<AvgOverTime> info() {
        return NodeInfo.create(this, AvgOverTime::new, field(), filter(), window(), timestamp);
    }

    @Override
    public AvgOverTime replaceChildren(List<Expression> newChildren) {
        return new AvgOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public AvgOverTime withFilter(Expression filter) {
        return new AvgOverTime(source(), field(), filter, window(), timestamp);
    }

    @Override
    public Expression surrogate() {
        if (field().dataType() == EXPONENTIAL_HISTOGRAM || field().dataType() == DataType.TDIGEST) {
            var mergeOverTime = new HistogramMergeOverTime(source(), field(), filter(), window(), timestamp);
            return new Div(
                source(),
                ExtractHistogramComponent.create(source(), mergeOverTime, HistogramBlock.Component.SUM),
                ExtractHistogramComponent.create(source(), mergeOverTime, HistogramBlock.Component.COUNT)
            );
        }
        return null;
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return new Avg(source(), field(), filter(), window(), SummationMode.LOSSY_LITERAL);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
