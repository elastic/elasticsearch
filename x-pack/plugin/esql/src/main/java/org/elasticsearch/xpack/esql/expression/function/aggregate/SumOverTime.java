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
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;
import java.util.Objects;

/**
 * Similar to {@link Sum}, but it is used to calculate the sum of values over a time series from the given field.
 */
public class SumOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        SurrogateExpression,
        TimestampAware,
        AggregateMetricDoubleNativeSupport,
        ToAggregator {
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(SumOverTime.class)
        .ternary(SumOverTime::new)
        .name("sum_over_time");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .withinSeries(SumOverTime::new)
        .description("Returns the sum of all values in the specified time range.")
        .example("sum_over_time(http_requests_total[5m])")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("sum_over_time");

    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "double", "long" },
        briefSummary = "Calculates the sum over time value of a field.",
        description = "Calculates the sum over time value of a field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = { @Example(file = "k8s-timeseries", tag = "sum_over_time") }
    )
    public SumOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "aggregate_metric_double", "double", "integer", "long", "exponential_histogram", "tdigest" },
            description = "the metric field to calculate the value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the sum over time",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public SumOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("SumOverTime is not directly serializable");
    }

    @Override
    public SumOverTime withFilter(Expression filter) {
        return new SumOverTime(source(), field(), filter, window(), timestamp);
    }

    @Override
    protected NodeInfo<SumOverTime> info() {
        return NodeInfo.create(this, SumOverTime::new, field(), filter(), window(), timestamp);
    }

    @Override
    public SumOverTime replaceChildren(List<Expression> newChildren) {
        return new SumOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
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
    public Expression surrogate() {
        if (field().dataType() == DataType.EXPONENTIAL_HISTOGRAM || field().dataType() == DataType.TDIGEST) {
            var mergeOverTime = new HistogramMergeOverTime(source(), field(), filter(), window(), timestamp);
            return ExtractHistogramComponent.create(source(), mergeOverTime, HistogramBlock.Component.SUM);
        }
        return null;
    }

    @Override
    public Sum perTimeSeriesAggregation() {
        return new Sum(source(), field(), filter(), window(), SummationMode.LOSSY_LITERAL);
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
