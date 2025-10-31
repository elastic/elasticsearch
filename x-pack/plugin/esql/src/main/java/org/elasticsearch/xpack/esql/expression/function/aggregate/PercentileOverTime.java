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

/**
 * Similar to {@link Percentile}, but it is used to calculate the percentile value over a time series of values from the given field.
 */
public class PercentileOverTime extends TimeSeriesAggregateFunction {
    @FunctionInfo(
        returnType = "double",
        description = "Calculates the percentile over time of a numeric field.",
        type = FunctionType.TIME_SERIES_AGGREGATE,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "percentile_over_time") }
    )
    public PercentileOverTime(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Expression that outputs values to calculate the percentile of."
        ) Expression field,
        @Param(name = "percentile", type = { "double", "integer", "long" }) Expression percentile
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, percentile);
    }

    public PercentileOverTime(Source source, Expression field, Expression filter, Expression window, Expression percentile) {
        super(source, field, filter, window, List.of(percentile));
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    protected NodeInfo<PercentileOverTime> info() {
        return NodeInfo.create(this, PercentileOverTime::new, field(), filter(), window(), children().get(3));
    }

    @Override
    public PercentileOverTime replaceChildren(List<Expression> newChildren) {
        return new PercentileOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public PercentileOverTime withFilter(Expression filter) {
        return new PercentileOverTime(source(), field(), filter, window(), children().get(3));
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return new Percentile(source(), field(), filter(), window(), children().get(3));
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PercentileOverTime is not directly serializable");
    }
}
