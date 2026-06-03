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
import org.elasticsearch.compute.aggregation.PrometheusHistogramQuantileAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.doubleValueOf;

/**
 * Internal aggregate implementing Prometheus classic-histogram quantile evaluation.
 * This is only intended for lowering {@code histogram_quantile()} inside PROMQL, with the
 * classic-histogram {@code le} label passed through as a keyword upper bound.
 */
public class PrometheusHistogramQuantile extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PrometheusHistogramQuantile",
        PrometheusHistogramQuantile::new
    );

    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .histogramBinary(PromqlFunctionDefinition.QUANTILE, (source, target, ctx, extraParams) -> {
            throw new UnsupportedOperationException("histogram_quantile is lowered via a dedicated logical node");
        })
        .description("Returns the φ-quantile of a classic histogram represented by cumulative `le` buckets.")
        .example("histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[5m]))")
        .name("histogram_quantile");

    private final Expression upperBound;
    private final Expression quantile;

    @FunctionInfo(returnType = "double", type = FunctionType.AGGREGATE)
    public PrometheusHistogramQuantile(
        Source source,
        @Param(name = "count", type = { "double" }) Expression field,
        @Param(name = "upper_bound", type = { "keyword" }) Expression upperBound,
        @Param(name = "quantile", type = { "double", "integer", "long" }) Expression quantile
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, upperBound, quantile);
    }

    public PrometheusHistogramQuantile(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression upperBound,
        Expression quantile
    ) {
        super(source, field, filter, window, List.of(upperBound, quantile));
        this.upperBound = upperBound;
        this.quantile = quantile;
    }

    private PrometheusHistogramQuantile(StreamInput in) throws IOException {
        super(in);
        this.upperBound = parameters().get(0);
        this.quantile = parameters().get(1);
    }

    public Expression upperBound() {
        return upperBound;
    }

    public Expression quantile() {
        return quantile;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt == DataType.DOUBLE, sourceText(), FIRST, "double").and(
            isType(upperBound, dt -> dt == DataType.KEYWORD, sourceText(), SECOND, "keyword")
        )
            .and(
                isType(quantile, dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG, sourceText(), THIRD, "numeric except unsigned_long")
            )
            .and(isFoldable(quantile, sourceText(), THIRD))
            .and(isNotNull(quantile, sourceText(), THIRD));
    }

    @Override
    protected NodeInfo<PrometheusHistogramQuantile> info() {
        return NodeInfo.create(this, PrometheusHistogramQuantile::new, field(), filter(), window(), upperBound, quantile);
    }

    @Override
    public PrometheusHistogramQuantile replaceChildren(List<Expression> newChildren) {
        return new PrometheusHistogramQuantile(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4)
        );
    }

    @Override
    public PrometheusHistogramQuantile withFilter(Expression filter) {
        return new PrometheusHistogramQuantile(source(), field(), filter, window(), upperBound, quantile);
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new PrometheusHistogramQuantileAggregatorFunctionSupplier(source(), quantileValue());
    }

    private double quantileValue() {
        return doubleValueOf(quantile, source().text(), "PrometheusHistogramQuantile");
    }
}
