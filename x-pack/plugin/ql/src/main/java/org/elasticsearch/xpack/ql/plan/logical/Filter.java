/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

/**
 * A {@code Filter} is a type of Plan that performs filtering of results. In
 * {@code SELECT x FROM y WHERE z ..} the "WHERE" clause is a Filter. A
 * {@code Filter} has a "condition" Expression that does the filtering.
 */
public class Filter extends UnaryPlan {

    private final Expression condition;

    public Filter(Source source, LogicalPlan child, Expression condition) {
        super(source, child);
        this.condition = condition;
    }

    @Override
    protected NodeInfo<Filter> info() {
        return NodeInfo.create(this, Filter::new, child(), condition);
    }

    @Override
    public Filter replaceChild(LogicalPlan newChild) {
        return new Filter(source(), newChild, condition);
    }

    public Expression condition() {
        return condition;
    }

    @Override
    public boolean expressionsResolved() {
        return condition.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Filter other = (Filter) obj;

        return Objects.equals(condition, other.condition)
                && Objects.equals(child(), other.child());
    }

    public Filter with(Expression condition) {
        return new Filter(source(), child(), condition);
    }

    public Filter with(LogicalPlan child, Expression condition) {
        return new Filter(source(), child, condition);
    }
}
