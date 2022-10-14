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
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class ProjectReorderRenameRemove extends Project {

    private final List<? extends NamedExpression> removals;

    public ProjectReorderRenameRemove(
        Source source,
        LogicalPlan child,
        List<? extends NamedExpression> projections,
        List<? extends NamedExpression> removals
    ) {
        super(source, child, projections);
        this.removals = removals;
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, ProjectReorderRenameRemove::new, child(), projections(), removals);
    }

    @Override
    public Project replaceChild(LogicalPlan newChild) {
        return new ProjectReorderRenameRemove(source(), newChild, projections(), removals);
    }

    public List<? extends NamedExpression> removals() {
        return removals;
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && Resolvables.resolved(removals);
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
        ProjectReorderRenameRemove other = (ProjectReorderRenameRemove) obj;
        return Objects.equals(removals, other.removals);
    }
}
