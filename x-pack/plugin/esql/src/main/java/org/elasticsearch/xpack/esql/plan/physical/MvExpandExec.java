/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class MvExpandExec extends UnaryExec {

    private final NamedExpression target;

    public MvExpandExec(Source source, PhysicalPlan child, NamedExpression target) {
        super(source, child);
        this.target = target;
    }

    @Override
    protected NodeInfo<MvExpandExec> info() {
        return NodeInfo.create(this, MvExpandExec::new, child(), target);
    }

    @Override
    public MvExpandExec replaceChild(PhysicalPlan newChild) {
        return new MvExpandExec(source(), newChild, target);
    }

    public NamedExpression target() {
        return target;
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MvExpandExec other = (MvExpandExec) obj;

        return Objects.equals(target, other.target) && Objects.equals(child(), other.child());
    }
}
