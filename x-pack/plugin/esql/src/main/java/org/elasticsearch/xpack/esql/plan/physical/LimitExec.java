/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class LimitExec extends UnaryExec {

    private final Expression limit;

    public LimitExec(Source source, PhysicalPlan child, Expression limit) {
        super(source, child);
        this.limit = limit;
    }

    @Override
    protected NodeInfo<? extends LimitExec> info() {
        return NodeInfo.create(this, LimitExec::new, child(), limit);
    }

    @Override
    public LimitExec replaceChild(PhysicalPlan newChild) {
        return new LimitExec(source(), newChild, limit);
    }

    public Expression limit() {
        return limit;
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

        LimitExec other = (LimitExec) obj;
        return Objects.equals(limit, other.limit) && Objects.equals(child(), other.child());
    }
}
