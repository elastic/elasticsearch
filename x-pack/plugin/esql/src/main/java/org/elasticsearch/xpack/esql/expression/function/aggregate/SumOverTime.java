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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Similar to {@link Sum}, but it is used to calculate the sum of values over a time series from the given field.
 */
public class SumOverTime extends TimeSeriesAggregateFunction
    implements
        OptionalArgument,
        AggregateMetricDoubleNativeSupport,
        SurrogateExpression,
        ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SumOverTime",
        SumOverTime::new
    );

    @FunctionInfo(
        returnType = { "double", "long" },
        description = "Calculates the sum over time value of a field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
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
        ) Expression window
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW));
    }

    public SumOverTime(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private SumOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public SumOverTime withFilter(Expression filter) {
        return new SumOverTime(source(), field(), filter, window());
    }

    @Override
    protected NodeInfo<SumOverTime> info() {
        return NodeInfo.create(this, SumOverTime::new, field(), filter(), window());
    }

    @Override
    public SumOverTime replaceChildren(List<Expression> newChildren) {
        return new SumOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
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
    public Expression surrogate() {
        return perTimeSeriesAggregation();
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return perTimeSeriesAggregation().supplier();
    }

    @Override
    public Sum perTimeSeriesAggregation() {
        return new Sum(source(), field(), filter(), window(), SummationMode.LOSSY_LITERAL);
    }
}
