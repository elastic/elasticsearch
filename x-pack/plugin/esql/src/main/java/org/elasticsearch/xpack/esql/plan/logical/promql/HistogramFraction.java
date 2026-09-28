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
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramFraction;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/** Dedicated logical node for PromQL {@code histogram_fraction()} over classic histograms. */
public final class HistogramFraction extends HistogramFunctionCall {
    private final Expression lower;
    private final Expression upper;

    public HistogramFraction(Source source, LogicalPlan child, PromqlFunctionDefinition definition, List<Expression> parameters) {
        super(source, child, definition, parameters);
        this.lower = parameters.get(0);
        this.upper = parameters.get(1);
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }

    @Override
    public Expression buildAggregateFunction(Expression count, Expression upperBound) {
        return new PromqlHistogramFraction(source(), count, upperBound, lower, upper);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, HistogramFraction::new, child(), definition(), parameters());
    }

    @Override
    public HistogramFraction replaceChild(LogicalPlan newChild) {
        return new HistogramFraction(source(), newChild, definition(), parameters());
    }
}
