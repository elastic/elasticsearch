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
 * Similar to {@link Variance}, but it is used to calculate the variance over a time series of values from the given field.
 */
public class VarianceOverTime extends TimeSeriesAggregateFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Calculates the population variance over time of a numeric field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "variance_over_time") }
    )
    public VarianceOverTime(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Expression for which to calculate the variance over time."
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public VarianceOverTime(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    @Override
    protected TypeResolution resolveType() {
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("VarianceOverTime does not have a writeable name");
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    protected NodeInfo<VarianceOverTime> info() {
        return NodeInfo.create(this, VarianceOverTime::new, field(), filter(), window());
    }

    @Override
    public VarianceOverTime replaceChildren(List<Expression> newChildren) {
        return new VarianceOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public VarianceOverTime withFilter(Expression filter) {
        return new VarianceOverTime(source(), field(), filter, window());
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return new Variance(source(), field(), filter(), window());
    }
}
