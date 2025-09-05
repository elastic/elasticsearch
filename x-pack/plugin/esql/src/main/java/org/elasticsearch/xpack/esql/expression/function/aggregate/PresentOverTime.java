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
 * Similar to {@link Present}, but it is used to check the presence of values over a time series in the given field.
 */
public class PresentOverTime extends TimeSeriesAggregateFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PresentOverTime",
        PresentOverTime::new
    );

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "boolean" },
        description = "The presence of a field in the output result over time range.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.UNAVAILABLE) },
        note = "Available with the [TS](/reference/query-languages/esql/commands/source-commands.md#esql-ts) command in snapshot builds",
        examples = { @Example(file = "k8s-timeseries", tag = "present_over_time") }
    )
    public PresentOverTime(
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
        this(source, field, Literal.TRUE);
    }

    public PresentOverTime(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private PresentOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public PresentOverTime withFilter(Expression filter) {
        return new PresentOverTime(source(), field(), filter);
    }

    @Override
    protected NodeInfo<PresentOverTime> info() {
        return NodeInfo.create(this, PresentOverTime::new, field(), filter());
    }

    @Override
    public PresentOverTime replaceChildren(List<Expression> newChildren) {
        return new PresentOverTime(source(), newChildren.get(0), newChildren.get(1));
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
    public Present perTimeSeriesAggregation() {
        return new Present(source(), field(), filter());
    }
}
