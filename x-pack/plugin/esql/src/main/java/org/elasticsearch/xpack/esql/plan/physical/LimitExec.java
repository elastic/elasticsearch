/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

@Experimental
public class LimitExec extends UnaryExec {

    private final Expression limit;

    private final Mode mode;

    public enum Mode {
        SINGLE,
        PARTIAL, // maps raw inputs to intermediate outputs
        FINAL, // maps intermediate inputs to final outputs
    }

    public LimitExec(Source source, PhysicalPlan child, Expression limit) {
        this(source, child, limit, Mode.SINGLE);
    }

    public LimitExec(Source source, PhysicalPlan child, Expression limit, Mode mode) {
        super(source, child);
        this.limit = limit;
        this.mode = mode;
    }

    @Override
    protected NodeInfo<LimitExec> info() {
        return NodeInfo.create(this, LimitExec::new, child(), limit, mode);
    }

    @Override
    public LimitExec replaceChild(PhysicalPlan newChild) {
        return new LimitExec(source(), newChild, limit, mode);
    }

    public Expression limit() {
        return limit;
    }

    public Mode mode() {
        return mode;
    }

    @Override
    public boolean singleNode() {
        if (mode != Mode.PARTIAL) {
            return true;
        }
        return child().singleNode();
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
