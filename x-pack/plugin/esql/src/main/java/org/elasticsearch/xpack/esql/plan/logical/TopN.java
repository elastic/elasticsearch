/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class TopN extends UnaryPlan {

    private final List<Order> order;
    private final Expression limit;

    public TopN(Source source, LogicalPlan child, List<Order> order, Expression limit) {
        super(source, child);
        this.order = order;
        this.limit = limit;
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved() && Resolvables.resolved(order);
    }

    @Override
    protected NodeInfo<TopN> info() {
        return NodeInfo.create(this, TopN::new, child(), order, limit);
    }

    @Override
    public TopN replaceChild(LogicalPlan newChild) {
        return new TopN(source(), newChild, order, limit);
    }

    public Expression limit() {
        return limit;
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            var other = (TopN) obj;
            return Objects.equals(order, other.order) && Objects.equals(limit, other.limit);
        }
        return false;
    }
}
