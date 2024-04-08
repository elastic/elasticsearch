/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class InlineStats extends UnaryPlan {

    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    public InlineStats(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    @Override
    protected NodeInfo<InlineStats> info() {
        return NodeInfo.create(this, InlineStats::new, child(), groupings, aggregates);
    }

    @Override
    public InlineStats replaceChild(LogicalPlan newChild) {
        return new InlineStats(source(), newChild, groupings, aggregates);
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InlineStats other = (InlineStats) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child());
    }
}
