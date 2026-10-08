/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Dedicated logical node for PromQL {@code histogram_quantile()} over classic histograms.
 * The function consumes the cumulative bucket counts identified by the {@code le} label and
 * produces the same label set as its child except for {@code le}.
 */
public final class HistogramQuantile extends HistogramFunctionCall {
    private final Expression quantile;

    public HistogramQuantile(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
        this.quantile = parameters.getFirst();
    }

    public Expression quantile() {
        return quantile;
    }

    @Override
    public Expression buildAggregateFunction(Expression count, Expression upperBound) {
        return new PromqlHistogramQuantile(source(), count, upperBound, quantile);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, HistogramQuantile::new, child(), definition(), parameters());
    }

    @Override
    public HistogramQuantile replaceChild(LogicalPlan newChild) {
        return new HistogramQuantile(source(), newChild, definition(), parameters());
    }

}
