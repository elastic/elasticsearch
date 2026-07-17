/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping;

import java.util.List;
import java.util.Objects;

/**
 * Across-series reduction such as {@code topk}.
 * <p>
 * Like {@link AcrossSeriesAggregate}, it partitions the input series, but unlike it,
 * doesn't aggregate them or change series identity - labels stay as-is, with reduction
 * applied per partition. Partitions never appear in the output.
 */
public final class AcrossSeriesReduction extends PromqlFunctionCall {

    private final Grouping grouping;
    private final List<Attribute> groupings;

    public AcrossSeriesReduction(
        Source source,
        LogicalPlan child,
        PromqlFunctionDefinition definition,
        List<Expression> parameters,
        Grouping grouping,
        List<Attribute> groupings
    ) {
        super(source, child, definition, parameters);
        this.grouping = grouping;
        this.groupings = groupings;
    }

    public Grouping grouping() {
        return grouping;
    }

    public List<Attribute> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && super.expressionsResolved();
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, AcrossSeriesReduction::new, child(), definition(), parameters(), grouping(), groupings());
    }

    @Override
    public AcrossSeriesReduction replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesReduction(source(), newChild, definition(), parameters(), grouping(), groupings());
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesReduction that = (AcrossSeriesReduction) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.ACROSS_SERIES_REDUCTION;
    }
}
