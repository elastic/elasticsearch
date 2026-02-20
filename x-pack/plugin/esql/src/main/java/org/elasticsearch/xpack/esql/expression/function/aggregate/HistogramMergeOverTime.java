/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Currently just a surrogate for applying {@link HistogramMerge} per series.
 */
public class HistogramMergeOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, SurrogateExpression, ToAggregator {
    // TODO Eventually we want to replace this with some increase/rate implementation
    // for histograms to be consistent with counters on extrapolation.

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "HistogramMergeOverTime",
        HistogramMergeOverTime::new
    );

    @FunctionInfo(returnType = { "exponential_histogram", "tdigest" }, type = FunctionType.TIME_SERIES_AGGREGATE)
    public HistogramMergeOverTime(
        Source source,
        @Param(name = "histogram", type = { "exponential_histogram", "tdigest" }) Expression field,
        @Param(name = "window", type = "time_duration", optional = true) Expression window
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW));
    }

    public HistogramMergeOverTime(Source source, Expression field, Expression filter, Expression window) {
        super(source, field, filter, window, emptyList());
    }

    private HistogramMergeOverTime(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected TypeResolution resolveType() {
        return perTimeSeriesAggregation().resolveType();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return perTimeSeriesAggregation().dataType();
    }

    @Override
    protected NodeInfo<HistogramMergeOverTime> info() {
        return NodeInfo.create(this, HistogramMergeOverTime::new, field(), filter(), window());
    }

    @Override
    public HistogramMergeOverTime replaceChildren(List<Expression> newChildren) {
        return new HistogramMergeOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public HistogramMergeOverTime withFilter(Expression filter) {
        return new HistogramMergeOverTime(source(), field(), filter, window());
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
        return new HistogramMerge(source(), field(), filter(), window());
    }
}
