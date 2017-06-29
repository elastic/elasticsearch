/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.tree.Location;

public class Project extends UnaryPlan {

    private final List<? extends NamedExpression> projections;

    public Project(Location location, LogicalPlan child, List<? extends NamedExpression> projections) {
        super(location, child);
        this.projections = projections;
    }

    public List<? extends NamedExpression> projections() {
        return projections;
    }

    @Override
    public boolean resolved() {
        return super.resolved() && !Expressions.anyMatchInList(projections, Functions::isAggregateFunction);
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
