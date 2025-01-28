/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.io.IOException;
import java.util.List;

public class EsqlProject extends Project {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "EsqlProject",
        EsqlProject::new
    );

    public EsqlProject(Source source, LogicalPlan child, List<? extends NamedExpression> projections) {
        super(source, child, projections);
    }

    public EsqlProject(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(projections());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
