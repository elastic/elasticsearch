/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ProjectExec extends UnaryExec {  // TODO implement EstimatesRowSize *somehow*
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ProjectExec",
        ProjectExec::new
    );

    private final List<? extends NamedExpression> projections;

    public ProjectExec(Source source, PhysicalPlan child, List<? extends NamedExpression> projections) {
        super(source, child);
        this.projections = projections;
    }

    private ProjectExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
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
    protected NodeInfo<ProjectExec> info() {
        return NodeInfo.create(this, ProjectExec::new, child(), projections);
    }

    @Override
    public ProjectExec replaceChild(PhysicalPlan newChild) {
        return new ProjectExec(source(), newChild, projections);
    }

    public List<? extends NamedExpression> projections() {
        return projections;
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

        ProjectExec other = (ProjectExec) obj;

        return Objects.equals(projections, other.projections) && Objects.equals(child(), other.child());
    }
}
