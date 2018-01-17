/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class LimitExec extends UnaryExec implements Unexecutable {

    private final Expression limit;

    public LimitExec(Location location, PhysicalPlan child, Expression limit) {
        super(location, child);
        this.limit = limit;
    }

    @Override
    protected NodeInfo<LimitExec> info() {
        return NodeInfo.create(this, LimitExec::new, child(), limit);
    }

    @Override
    protected LimitExec replaceChild(PhysicalPlan newChild) {
        return new LimitExec(location(), newChild, limit);
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
        return Objects.equals(limit, other.limit)
                && Objects.equals(child(), other.child());
    }
}
