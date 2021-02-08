/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.Functions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * A {@code Project} is a {@code Plan} with one child. In {@code SELECT x FROM y}, the "SELECT" statement is a Project.
 */
public class Project extends UnaryPlan {

    private final List<? extends NamedExpression> projections;

    public Project(Source source, LogicalPlan child, List<? extends NamedExpression> projections) {
        super(source, child);
        this.projections = projections;
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, Project::new, child(), projections);
    }

    @Override
    protected Project replaceChild(LogicalPlan newChild) {
        return new Project(source(), newChild, projections);
    }

    public List<? extends NamedExpression> projections() {
        return projections;
    }

    @Override
    public boolean resolved() {
        return super.resolved() && Expressions.anyMatch(projections, Functions::isAggregate) == false;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(projections);
    }

    @Override
    public List<Attribute> output() {
        return Expressions.asAttributes(projections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projections, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Project other = (Project) obj;

        return Objects.equals(projections, other.projections)
                && Objects.equals(child(), other.child());
    }
}
