/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class Tail extends UnaryPlan {

    private final Expression limit;

    public Tail(Source source, Expression limit, LogicalPlan child) {
        super(source, child);
        this.limit = limit;
    }

    @Override
    protected NodeInfo<Tail> info() {
        return NodeInfo.create(this, Tail::new, limit, child());
    }

    @Override
    protected Tail replaceChild(LogicalPlan newChild) {
        return new Tail(source(), limit, newChild);
    }

    public Expression limit() {
        return limit;
    }

    @Override
    public boolean expressionsResolved() {
        return limit.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Tail other = (Tail) obj;

        return Objects.equals(limit, other.limit)
                && Objects.equals(child(), other.child());
    }
}