/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
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

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Similar to {@link Count}, but it is used to calculate the count of values over a time series from the given field.
 */
public class CountOverTime extends TimeSeriesAggregateFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CountOverTime",
        CountOverTime::new
    );

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long" },
        description = "Calculates the count over time value of a field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
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
                "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW);
    }

    public CountOverTime(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private CountOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public CountOverTime withFilter(Expression filter) {
        return new CountOverTime(source(), field(), filter, window());
    }

    @Override
    protected NodeInfo<CountOverTime> info() {
        return NodeInfo.create(this, CountOverTime::new, field(), filter(), window());
    }

    @Override
    public CountOverTime replaceChildren(List<Expression> newChildren) {
        return new CountOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
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
    public Count perTimeSeriesAggregation() {
        return new Count(source(), field(), filter(), window());
    }
}
