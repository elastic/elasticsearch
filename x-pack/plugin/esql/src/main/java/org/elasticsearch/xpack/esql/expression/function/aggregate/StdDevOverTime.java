/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

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
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Similar to {@link StdDev}, but it is used to calculate the standard deviation over a time series of values from the given field.
 */
public class StdDevOverTime extends TimeSeriesAggregateFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Calculates the population standard deviation over time of a numeric field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "stddev_over_time") }
    )
    public StdDevOverTime(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Expression that outputs values to calculate the standard deviation of."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public StdDevOverTime(Source source, Expression field, Expression filter, Expression window) {
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
    protected NodeInfo<StdDevOverTime> info() {
        return NodeInfo.create(this, StdDevOverTime::new, field(), filter(), window());
    }

    @Override
    public StdDevOverTime replaceChildren(List<Expression> newChildren) {
        return new StdDevOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public StdDevOverTime withFilter(Expression filter) {
        return new StdDevOverTime(source(), field(), filter, window());
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return new StdDev(source(), field(), filter(), window());
    }
}
