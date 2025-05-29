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
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

/**
 * Similar to {@link CountDistinct}, but it is used to calculate the distinct count of values over a time series from the given field.
 */
public class DistinctOverTime extends TimeSeriesAggregateFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DistinctOverTime",
        DistinctOverTime::new
    );

    private final Expression precision;

    @FunctionInfo(
        returnType = { "integer", "long" },
        description = "The count of distinct values over time for a field.",
        type = FunctionType.AGGREGATE
    )
    public DistinctOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" }
        ) Expression field,
        @Param(
            optional = true,
            name = "precision",
            type = { "integer", "long", "unsigned_long" },
            description = "Precision threshold. Refer to <<esql-agg-count-distinct-approximate>>. "
                + "The maximum supported value is 40000. Thresholds above this number will have the "
                + "same effect as a threshold of 40000. The default value is 3000."
        ) Expression precision
    ) {
        this(source, field, Literal.TRUE, precision);
    }

    public DistinctOverTime(Source source, Expression field, Expression filter, Expression precision) {
        super(source, field, filter, precision == null ? List.of() : List.of(precision));
        this.precision = precision;
    }

    private DistinctOverTime(StreamInput in) throws IOException {
        super(in);
        this.precision = parameters().isEmpty() ? null : parameters().getFirst();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DistinctOverTime withFilter(Expression filter) {
        return new DistinctOverTime(source(), field(), filter, precision);
    }

    @Override
    protected NodeInfo<DistinctOverTime> info() {
        return NodeInfo.create(this, DistinctOverTime::new, field(), filter(), precision);
    }

    @Override
    public DistinctOverTime replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() > 2) {
            return new DistinctOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
        }
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
        return new CountDistinct(source(), field(), filter(), precision);
    }
}
