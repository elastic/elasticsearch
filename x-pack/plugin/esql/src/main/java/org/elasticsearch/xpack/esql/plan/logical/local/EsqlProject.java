/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class EsqlProject extends Project {

    public EsqlProject(Source source, LogicalPlan child, List<? extends NamedExpression> projections) {
        super(source, child, projections);
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, EsqlProject::new, child(), projections());
    }

    @Override
    public EsqlProject replaceChild(LogicalPlan newChild) {
        return new EsqlProject(source(), newChild, projections());
    }

    @Override
    public boolean expressionsResolved() {
        for (NamedExpression projection : projections()) {
            // don't call dataType() - it will fail on UnresolvedAttribute
            if (projection.resolved() == false && projection instanceof UnsupportedAttribute == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Project withProjections(List<? extends NamedExpression> projections) {
        return new EsqlProject(source(), child(), projections);
    }
}
