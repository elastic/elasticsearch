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
 * Similar to {@link Sum}, but it is used to calculate the sum of values over a time series from the given field.
 */
public class SumOverTime extends TimeSeriesAggregateFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SumOverTime",
        SumOverTime::new
    );

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The sum over time value of a field.",
        type = FunctionType.AGGREGATE
    )
    public SumOverTime(
        Source source,
        @Param(name = "field", type = { "aggregate_metric_double", "double", "integer", "long" }) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public SumOverTime(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
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
        return new SumOverTime(source(), field(), filter);
    }

    @Override
    protected NodeInfo<SumOverTime> info() {
        return NodeInfo.create(this, SumOverTime::new, field(), filter());
    }

    @Override
    public SumOverTime replaceChildren(List<Expression> newChildren) {
        return new SumOverTime(source(), newChildren.get(0), newChildren.get(1));
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
    public Sum perTimeSeriesAggregation() {
        return new Sum(source(), field(), filter());
    }
}
