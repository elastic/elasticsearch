/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Functions;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@code Project} is a {@code Plan} with one child. In {@code FROM idx | KEEP x, y}, the {@code KEEP} statement is a Project.
 */
public class Project extends UnaryPlan implements Streaming, SortAgnostic, SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Project", Project::new);

    /**
     * Backward compatibility entry + name for reading the consolidated `EsqlProject` plans from pre-9.4.0 nodes.
     */
    private static final String LEGACY_PROJECT_NAME = "EsqlProject";

    @UpdateForV10(owner = UpdateForV10.Owner.SEARCH_ANALYTICS)
    public static final NamedWriteableRegistry.Entry V9_ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        LEGACY_PROJECT_NAME,
        Project::readLegacyEsqlProject
    );

    private static Project readLegacyEsqlProject(StreamInput in) throws IOException {
        return new Project(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            V9_ENTRY.name
        );
    }

    private final List<? extends NamedExpression> projections;
    private final String writeableName;

    public Project(Source source, LogicalPlan child, List<? extends NamedExpression> projections) {
        this(source, child, projections, ENTRY.name);
    }

    /**
     * Constructor that allows specifying a custom writeable name for backward compatibility.
     * Used when deserializing legacy "EsqlProject" plans from older cluster versions.
     */
    private Project(Source source, LogicalPlan child, List<? extends NamedExpression> projections, String writeableName) {
        super(source, child);
        this.projections = projections;
        this.writeableName = writeableName;
        assert validateProjections(projections);
    }

    private boolean validateProjections(List<? extends NamedExpression> projections) {
        for (NamedExpression ne : projections) {
            if (ne instanceof Alias as) {
                if (as.child() instanceof Attribute == false) {
                    return false;
                }
            } else if (ne instanceof Attribute == false && ne instanceof UnresolvedNamedExpression == false) {
                return false;
            }
        }
        return true;
    }

    private Project(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            ENTRY.name
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
        return writeableName;
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, Project::new, child(), projections);
    }

    @Override
    public Project replaceChild(LogicalPlan newChild) {
        return new Project(source(), newChild, projections);
    }

    public List<? extends NamedExpression> projections() {
        return projections;
    }

    public Project withProjections(List<? extends NamedExpression> projections) {
        return new Project(source(), child(), projections);
    }

    @Override
    public boolean resolved() {
        return super.resolved() && Expressions.anyMatch(projections, Functions::isAggregate) == false;
    }

    @Override
    public boolean expressionsResolved() {
        for (NamedExpression projection : projections) {
            // don't call dataType() - it will fail on UnresolvedAttribute
            if (projection.resolved() == false && projection instanceof UnsupportedAttribute == false) {
                return false;
            }
        }
        return true;
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

        return Objects.equals(projections, other.projections) && Objects.equals(child(), other.child());
    }
}
