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

public class Limit extends UnaryPlan {

    private final Expression limit;

    public Limit(Source source, Expression limit, LogicalPlan child) {
        super(source, child);
        this.limit = limit;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Limit::new, limit, child());
    }

    @Override
    protected Limit replaceChild(LogicalPlan newChild) {
        return new Limit(source(), limit, newChild);
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

        Limit other = (Limit) obj;

        return Objects.equals(limit, other.limit)
                && Objects.equals(child(), other.child());
    }
}
