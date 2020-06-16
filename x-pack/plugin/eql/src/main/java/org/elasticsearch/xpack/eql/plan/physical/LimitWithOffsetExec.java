/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class LimitWithOffsetExec extends UnaryExec implements Unexecutable {

    private final int limit;
    private final int offset;

    public LimitWithOffsetExec(Source source, PhysicalPlan child, int limit, int offset) {
        super(source, child);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    protected NodeInfo<LimitWithOffsetExec> info() {
        return NodeInfo.create(this, LimitWithOffsetExec::new, child(), limit, offset);
    }

    @Override
    protected LimitWithOffsetExec replaceChild(PhysicalPlan newChild) {
        return new LimitWithOffsetExec(source(), newChild, limit, offset);
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, offset, child());
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
                && Objects.equals(offset, other.offset)
                && Objects.equals(child(), other.child());
    }
}
