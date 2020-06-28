/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class LimitWithOffset extends org.elasticsearch.xpack.ql.plan.logical.Limit {

    private final int offset;

    public LimitWithOffset(Source source, Expression limit, LogicalPlan child) {
        this(source, limit, 0, child);
    }

    public LimitWithOffset(Source source, Expression limit, int offset, LogicalPlan child) {
        super(source, limit, child);
        this.offset = offset;
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, LimitWithOffset::new, limit(), offset, child());
    }

    @Override
    protected LimitWithOffset replaceChild(LogicalPlan newChild) {
        return new LimitWithOffset(source(), limit(), offset, newChild);
    }

    public int offset() {
        return offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            LimitWithOffset other = (LimitWithOffset) obj;
            return Objects.equals(offset, other.offset);
        }
        return false;
    }
}
