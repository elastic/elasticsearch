/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class MvExpand extends UnaryPlan {
    private final NamedExpression target;

    public MvExpand(Source source, LogicalPlan child, NamedExpression target) {
        super(source, child);
        this.target = target;
    }

    public NamedExpression target() {
        return target;
    }

    @Override
    public boolean expressionsResolved() {
        return target.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new MvExpand(source(), newChild, target);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MvExpand::new, child(), target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), target);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        return Objects.equals(target, ((MvExpand) obj).target);
    }
}
