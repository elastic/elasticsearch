/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.esql.analysis.rules.ResolveUnmapped;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * This version of {@link EsqlProject} saves part of its state the computing of projections based on its child's output. This allows
 * reapplying the modeled rules (RENAME/DROP/KEEP) transparently, in case its child changes its output; this can occur if
 * {@link ResolveUnmapped} injects null attributes or source extractors, as these are discovered downstream from this node and injected
 * upstream of it.
 */
public class ResolvingProject extends EsqlProject {

    private final Function<List<Attribute>, List<? extends NamedExpression>> resolver;

    public ResolvingProject(Source source, LogicalPlan child, Function<List<Attribute>, List<? extends NamedExpression>> resolver) {
        this(source, child, resolver, resolver.apply(child.output()));
    }

    public ResolvingProject(
        Source source,
        LogicalPlan child,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        List<? extends NamedExpression> projections
    ) {
        super(source, child, projections);
        this.resolver = resolver;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public Function<List<Attribute>, List<? extends NamedExpression>> resolver() {
        return resolver;
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, ResolvingProject::new, child(), resolver, projections());
    }

    @Override
    public ResolvingProject replaceChild(LogicalPlan newChild) {
        return new ResolvingProject(source(), newChild, resolver);
    }

    @Override
    public Project withProjections(List<? extends NamedExpression> projections) {
        return new ResolvingProject(source(), child(), resolver, projections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolver);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ResolvingProject other = (ResolvingProject) obj;
        return super.equals(obj) && Objects.equals(resolver, other.resolver);
    }

    public EsqlProject asEsqlProject() {
        return new EsqlProject(source(), child(), projections());
    }
}
