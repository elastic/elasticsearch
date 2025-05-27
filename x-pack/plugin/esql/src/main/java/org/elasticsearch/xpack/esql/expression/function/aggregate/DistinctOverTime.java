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
 * Similar to {@link CountDistinct}, but it is used to calculate the distinct count of values over a time series from the given field.
 */
public class DistinctOverTime extends TimeSeriesAggregateFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DistinctOverTime",
        DistinctOverTime::new
    );

    @FunctionInfo(
        returnType = { "integer", "long" },
        description = "The count of distinct values over time for a field.",
        type = FunctionType.AGGREGATE
    )
    public DistinctOverTime(
        Source source,
        @Param(name = "field", type = { "aggregate_metric_double", "double", "integer", "long", "float" }) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public DistinctOverTime(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private DistinctOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DistinctOverTime withFilter(Expression filter) {
        return new DistinctOverTime(source(), field(), filter);
    }

    @Override
    protected NodeInfo<DistinctOverTime> info() {
        return NodeInfo.create(this, DistinctOverTime::new, field(), filter());
    }

    @Override
    public DistinctOverTime replaceChildren(List<Expression> newChildren) {
        return new DistinctOverTime(source(), newChildren.get(0), newChildren.get(1));
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
    public CountDistinct perTimeSeriesAggregation() {
        // TODO(pabloem): Do we need to take in a precision parameter here?
        return new CountDistinct(source(), field(), filter(), null);
    }
}
