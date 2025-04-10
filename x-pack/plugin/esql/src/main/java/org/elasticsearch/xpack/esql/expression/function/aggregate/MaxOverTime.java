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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Similar to {@link Max}, but it is used to calculate the maximum value over a time series of values from the given field.
 */
public class MaxOverTime extends TimeSeriesAggregateFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MaxOverTime",
        MaxOverTime::new
    );

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "date_nanos", "ip", "keyword", "long", "version" },
        description = "The maximum over time value of a field.",
        type = FunctionType.AGGREGATE
    )
    public MaxOverTime(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "double",
                "integer",
                "long",
                "date",
                "date_nanos",
                "ip",
                "keyword",
                "text",
                "long",
                "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public MaxOverTime(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private MaxOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public MaxOverTime withFilter(Expression filter) {
        return new MaxOverTime(source(), field(), filter);
    }

    @Override
    protected NodeInfo<MaxOverTime> info() {
        return NodeInfo.create(this, MaxOverTime::new, field(), filter());
    }

    @Override
    public MaxOverTime replaceChildren(List<Expression> newChildren) {
        return new MaxOverTime(source(), newChildren.get(0), newChildren.get(1));
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
    public Max perTimeSeriesAggregation() {
        return new Max(source(), field(), filter());
    }
}
