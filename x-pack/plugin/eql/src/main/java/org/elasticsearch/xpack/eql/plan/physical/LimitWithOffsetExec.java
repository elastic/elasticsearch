/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class LimitWithOffsetExec extends UnaryExec implements Unexecutable {

    private final Limit limit;

    public LimitWithOffsetExec(Source source, PhysicalPlan child, Limit limit) {
        super(source, child);
        this.limit = limit;
    }

    @Override
    protected NodeInfo<LimitWithOffsetExec> info() {
        return NodeInfo.create(this, LimitWithOffsetExec::new, child(), limit);
    }

    @Override
    protected LimitWithOffsetExec replaceChild(PhysicalPlan newChild) {
        return new LimitWithOffsetExec(source(), newChild, limit);
    }

    public Limit limit() {
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

        LimitWithOffsetExec other = (LimitWithOffsetExec) obj;
        return Objects.equals(limit, other.limit)
                && Objects.equals(child(), other.child());
    }
}
