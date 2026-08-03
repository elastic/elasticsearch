/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * This version of {@link Project} saves part of its state for computing its projections based on its child's output. This avoids
 * the problem that once the projections are computed, we don't know which pattern was used to generate them. This is important
 * when dealing with unmapped fields: E.g. in
 * {@code SET unmapped_fields="nullify"; FROM idx | KEEP foo* | WHERE foo_bar > 10}, if {@code foo_bar} is not mapped, we need to inject
 * a {@code NULL} literal for it before the {@code KEEP}. It's correct to update the projection of the {@code KEEP} to include this new
 * attribute because the pattern {@code foo*} matches it. But if the pattern was {@code foo_baz}, it would be incorrect to do so.
 */
public class ResolvingProject extends Project {

    /** The command this node was built from, i.e. how {@link #originalProjections} translate into an {@link UnmappedFieldsPattern}. */
    public enum Kind {
        KEEP,
        DROP,
        RENAME
    }

    private final Function<List<Attribute>, List<? extends NamedExpression>> resolver;
    private final Kind kind;
    private final List<? extends NamedExpression> originalProjections;

    public ResolvingProject(
        Source source,
        LogicalPlan child,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        Kind kind,
        List<? extends NamedExpression> originalProjections
    ) {
        this(source, child, computeProjections(child.output(), resolver), resolver, kind, originalProjections);
    }

    /**
     * Runs the resolver against the child output, keeping any {@link UnmappedFieldsAttribute}
     * instances out of the resolver's scope (so KEEP/DROP/RENAME patterns cannot match the
     * synthetic column), then re-appending them unconditionally at the end of the projections.
     */
    private static List<? extends NamedExpression> computeProjections(
        List<Attribute> childOutput,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver
    ) {
        List<Attribute> unmappedAttrs = childOutput.stream().filter(a -> a instanceof UnmappedFieldsAttribute).toList();
        List<Attribute> resolverInput = unmappedAttrs.isEmpty()
            ? childOutput
            : childOutput.stream().filter(a -> (a instanceof UnmappedFieldsAttribute) == false).toList();
        List<? extends NamedExpression> resolved = resolver.apply(resolverInput);
        if (unmappedAttrs.isEmpty()) {
            return resolved;
        }
        return CollectionUtils.combine(resolved, unmappedAttrs);
    }

    private ResolvingProject(
        Source source,
        LogicalPlan child,
        List<? extends NamedExpression> projections,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        Kind kind,
        List<? extends NamedExpression> originalProjections
    ) {
        super(source, child, projections);
        this.resolver = resolver;
        this.kind = kind;
        this.originalProjections = originalProjections;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public Function<List<Attribute>, List<? extends NamedExpression>> resolver() {
        return resolver;
    }

    /**
     * Which unmapped source fields this command lets through, derived from the projections it was written with — by the time
     * {@link #projections()} is computed, {@code ResolveRefs} has replaced the original wildcard expressions with the attributes
     * they matched. Only {@code DetermineUnmappedFieldsToKeep} reads this, so it is only ever derived under
     * {@code unmapped_fields="LOAD_ALL"}.
     */
    public UnmappedFieldsPattern unmappedFieldsPattern() {
        return switch (kind) {
            case KEEP -> UnmappedFieldsPattern.forKeep(originalProjections);
            case DROP -> UnmappedFieldsPattern.forDrop(originalProjections);
            // A RENAME keeps every column, so it restricts nothing; its target names shadow the source fields of the same name, which
            // DetermineUnmappedFieldsToKeep excludes from this node's output on its own.
            case RENAME -> UnmappedFieldsPattern.ALL;
        };
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, ResolvingProject::new, child(), projections(), resolver, kind, originalProjections);
    }

    /**
     * The default implementation harvests every expression reachable from the node's properties, which would pull the pre-resolution
     * {@link #originalProjections} into {@link #expressions()} and hence into {@link #references()}. A {@code DROP}'s removals would then
     * look like references, and FORK's alignment (which materializes an unmapped field dropped in one branch in its siblings) skips
     * referenced fields. Only the resolved projections describe this node, exactly as in {@link Project}.
     */
    @Override
    protected List<Expression> computeExpressions() {
        return new ArrayList<>(projections());
    }

    @Override
    public ResolvingProject replaceChild(LogicalPlan newChild) {
        return new ResolvingProject(source(), newChild, resolver, kind, originalProjections);
    }

    @Override
    public Project withProjections(List<? extends NamedExpression> projections) {
        return new ResolvingProject(source(), child(), projections, resolver, kind, originalProjections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolver, kind, originalProjections);
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
        return super.equals(obj)
            && Objects.equals(resolver, other.resolver)
            && kind == other.kind
            && Objects.equals(originalProjections, other.originalProjections);
    }

    public Project asProject() {
        return new Project(source(), child(), projections());
    }
}
