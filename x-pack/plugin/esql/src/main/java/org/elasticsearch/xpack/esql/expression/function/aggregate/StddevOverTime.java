/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Similar to {@link StdDev}, but it is used to calculate the standard deviation over a time series of values from the given field.
 */
public class StddevOverTime extends TimeSeriesAggregateFunction implements SurrogateExpression, ToAggregator {
    @FunctionInfo(
        returnType = "double",
        description = "Calculates the population standard deviation over time of a numeric field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "stddev_over_time") }
    )
    public StddevOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long" },
            description = "the metric field to calculate the standard deviation for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to compute the standard deviation over time",
            optional = true
        ) Expression window
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW));
    }

    public StddevOverTime(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    @Override
    protected TypeResolution resolveType() {
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("StdDevOverTime does not have a writeable name");
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    protected NodeInfo<StddevOverTime> info() {
        return NodeInfo.create(this, StddevOverTime::new, field(), filter(), window());
    }

    @Override
    public StddevOverTime replaceChildren(List<Expression> newChildren) {
        return new StddevOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public StddevOverTime withFilter(Expression filter) {
        return new StddevOverTime(source(), field(), filter, window());
    }

    @Override
    public Expression surrogate() {
        return perTimeSeriesAggregation();
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return ((ToAggregator) perTimeSeriesAggregation()).supplier();
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return new StdDev(source(), field(), filter(), window());
    }
}
