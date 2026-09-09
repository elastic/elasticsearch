/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PromqlHistogramQuantileAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramPercentile;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramQuantile;
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
public class PromqlHistogramQuantile extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PromqlHistogramQuantile",
        PromqlHistogramQuantile::readFrom
    );

    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .histogramBinary(PromqlFunctionDefinition.QUANTILE, (source, target, ctx, extraParams) -> {
            if (target.resolved() == false || target.dataType().isHistogram() == false) {
                throw new IllegalStateException(
                    "histogram_quantile for classic histograms should have been replaced with PromqlHistogramQuantile by the planner"
                );
            }

            Expression quantile = extraParams.getFirst();
            if (quantile.foldable()) {
                // TODO: we should probably be using a shared fold context stored in PromqlContext?
                Object folded = quantile.fold(FoldContext.small());
                if (folded instanceof Number n) {
                    if (n.doubleValue() == 0.0) {
                        return ExtractHistogramComponent.create(source, target, ExponentialHistogramBlock.Component.MIN);
                    } else if (n.doubleValue() == 1.0) {
                        return ExtractHistogramComponent.create(source, target, ExponentialHistogramBlock.Component.MAX);
                    }
                }
            }
            return new HistogramPercentile(source, target, PromqlFunctionDefinition.quantileToPercentile(source, quantile));
        })
        .classicHistogramHandler(HistogramQuantile::new)
        .description(
            "Returns the φ-quantile of a classic histogram represented by cumulative `le` buckets or a native (exponential) histogram."
        )
        .example("histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[5m]))")
        .stack(PromqlFunctionDefinition.STACK_GA_9_5)
        .name("histogram_quantile");

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "double",
        type = FunctionType.AGGREGATE
    )
    public PromqlHistogramQuantile(
        Source source,
        @Param(name = "count", type = { "double" }) Expression field,
        @Param(name = "upper_bound", type = { "keyword" }) Expression upperBound,
        @Param(name = "quantile", type = { "double", "integer", "long" }) Expression quantile
    ) {
        this(source, field, upperBound, Literal.TRUE, NO_WINDOW, quantile);
    }

    public PromqlHistogramQuantile(
        Source source,
        Expression field,
        Expression upperBound,
        Expression filter,
        Expression window,
        Expression quantile
    ) {
        super(source, List.of(field, upperBound), filter, window, List.of(quantile));
    }

    private static PromqlHistogramQuantile readFrom(StreamInput in) throws IOException {
        // Legacy serialization format for backwards compatibility
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new PromqlHistogramQuantile(source, field, parameters.get(0), filter, window, parameters.get(1));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(List.of(upperBound(), quantile()));
    }

    public Expression field() {
        return fields().get(0);
    }

    public Expression upperBound() {
        return fields().get(1);
    }

    public Expression quantile() {
        return parameters().get(0);
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
            isType(upperBound(), dt -> dt == DataType.KEYWORD, sourceText(), SECOND, "keyword")
        )
            .and(
                isType(
                    quantile(),
                    dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
                    sourceText(),
                    THIRD,
                    "numeric except unsigned_long"
                )
            )
            .and(isFoldable(quantile(), sourceText(), THIRD))
            .and(isNotNull(quantile(), sourceText(), THIRD));
    }

    @Override
    protected NodeInfo<PromqlHistogramQuantile> info() {
        return NodeInfo.create(this, PromqlHistogramQuantile::new, field(), upperBound(), filter(), window(), quantile());
    }

    @Override
    public PromqlHistogramQuantile replaceChildren(List<Expression> newChildren) {
        return new PromqlHistogramQuantile(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4)
        );
    }

    @Override
    public PromqlHistogramQuantile withFilter(Expression filter) {
        return new PromqlHistogramQuantile(source(), field(), upperBound(), filter, window(), quantile());
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        return new PromqlHistogramQuantileAggregatorFunctionSupplier(source(), quantileValue());
    }

    private double quantileValue() {
        return doubleValueOf(quantile(), source().text(), "PromqlHistogramQuantile");
    }
}
