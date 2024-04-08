/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class Drop extends UnaryPlan {
    private final List<NamedExpression> removals;

    public Drop(Source source, LogicalPlan child, List<NamedExpression> removals) {
        super(source, child);
        this.removals = removals;
    }

    public List<NamedExpression> removals() {
        return removals;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(removals);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Drop(source(), newChild, removals);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Drop::new, child(), removals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), removals);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        return Objects.equals(removals, ((Drop) obj).removals);
    }
}
