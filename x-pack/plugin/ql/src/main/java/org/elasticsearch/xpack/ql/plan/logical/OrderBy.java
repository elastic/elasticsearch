/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class OrderBy extends UnaryPlan {

    private final List<Order> order;

    public OrderBy(Source source, LogicalPlan child, List<Order> order) {
        super(source, child);
        this.order = order;
    }

    @Override
    protected NodeInfo<OrderBy> info() {
        return NodeInfo.create(this, OrderBy::new, child(), order);
    }

    @Override
    public OrderBy replaceChild(LogicalPlan newChild) {
        return new OrderBy(source(), newChild, order);
    }

    public List<Order> order() {
        return order;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(order, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        OrderBy other = (OrderBy) obj;
        return Objects.equals(order, other.order) && Objects.equals(child(), other.child());
    }
}
