/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class TopNExec extends UnaryExec {

    private final Expression limit;
    private final List<Order> order;

    public TopNExec(Source source, PhysicalPlan child, List<Order> order, Expression limit) {
        super(source, child);
        this.order = order;
        this.limit = limit;
    }

    @Override
    protected NodeInfo<TopNExec> info() {
        return NodeInfo.create(this, TopNExec::new, child(), order, limit);
    }

    @Override
    public TopNExec replaceChild(PhysicalPlan newChild) {
        return new TopNExec(source(), newChild, order, limit);
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
        boolean equals = super.equals(obj);
        if (equals) {
            var other = (TopNExec) obj;
            equals = Objects.equals(order, other.order) && Objects.equals(limit, other.limit);
        }
        return equals;
    }
}
