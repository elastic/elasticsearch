/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

@Experimental
public class TopNExec extends UnaryExec {

    private final List<Order> order;
    private final Expression limit;
    private final Mode mode;

    public enum Mode {
        SINGLE,
        PARTIAL, // maps raw inputs to intermediate outputs
        FINAL, // maps intermediate inputs to final outputs
    }

    public TopNExec(Source source, PhysicalPlan child, List<Order> order, Expression limit) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.mode = Mode.SINGLE;
    }

    public TopNExec(Source source, PhysicalPlan child, List<Order> order, Expression limit, Mode mode) {
        super(source, child);
        this.order = order;
        this.limit = limit;
        this.mode = mode;
    }

    @Override
    protected NodeInfo<TopNExec> info() {
        return NodeInfo.create(this, TopNExec::new, child(), order, limit, mode);
    }

    @Override
    public TopNExec replaceChild(PhysicalPlan newChild) {
        return new TopNExec(source(), newChild, order, limit, mode);
    }

    public List<Order> order() {
        return order;
    }

    public Expression getLimit() {
        return limit;
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public boolean singleNode() {
        if (mode != TopNExec.Mode.PARTIAL) {
            return true;
        }
        return child().singleNode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(order, limit, mode, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TopNExec other = (TopNExec) obj;
        return Objects.equals(order, other.order)
            && Objects.equals(limit, other.limit)
            && Objects.equals(mode, other.mode)
            && Objects.equals(child(), other.child());
    }

}
