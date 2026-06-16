/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
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
    private final Function<List<Attribute>, List<? extends NamedExpression>> resolver;
    /**
     * The unmapped-fields glob pattern describing which additional source fields survive
     * the KEEP/DROP/RENAME that created this node. Pre-computed at creation time so it
     * remains available in the Finish Analysis batch (after {@code ResolveRefs} has already
     * discarded the original wildcard expressions).
     */
    private final UnmappedFieldsPattern unmappedFieldsPattern;

    public ResolvingProject(
        Source source,
        LogicalPlan child,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        UnmappedFieldsPattern unmappedFieldsPattern
    ) {
        this(source, child, computeProjections(child.output(), resolver), resolver, unmappedFieldsPattern);
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
        List<NamedExpression> combined = new ArrayList<>(resolved.size() + unmappedAttrs.size());
        combined.addAll(resolved);
        combined.addAll(unmappedAttrs);
        return combined;
    }

    private ResolvingProject(
        Source source,
        LogicalPlan child,
        List<? extends NamedExpression> projections,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        UnmappedFieldsPattern unmappedFieldsPattern
    ) {
        super(source, child, projections);
        this.resolver = resolver;
        this.unmappedFieldsPattern = unmappedFieldsPattern;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public Function<List<Attribute>, List<? extends NamedExpression>> resolver() {
        return resolver;
    }

    /**
     * Computes the unmapped-fields pattern for the sub-plan rooted at this node.
     * <ul>
     *   <li>If this node was created from a KEEP, {@code unmappedFieldsPattern.includes()} is the
     *       set of patterns to keep; child-node excludes (e.g. from EVAL) are appended.</li>
     *   <li>If this node was created from a DROP or RENAME, {@code unmappedFieldsPattern.includes()}
     *       is {@code ["*"]}, so the effective includes are inherited from the child (allowing a KEEP
     *       lower in the tree to still restrict the field set).</li>
     * </ul>
     */
    @Override
    public UnmappedFieldsPattern unmappedFieldsToKeep() {
        UnmappedFieldsPattern childPattern = child().unmappedFieldsToKeep();
        // If our own includes is ["*"] (DROP or RENAME), inherit the child's (more specific) includes.
        // Otherwise (KEEP with explicit patterns), use our own includes.
        List<String> effectiveIncludes = unmappedFieldsPattern.includes().equals(List.of("*"))
            ? childPattern.includes()
            : unmappedFieldsPattern.includes();
        List<String> allExcludes = new ArrayList<>(unmappedFieldsPattern.excludes());
        allExcludes.addAll(childPattern.excludes());
        return new UnmappedFieldsPattern(effectiveIncludes, allExcludes);
    }

    /**
     * Static factory used by {@link NodeInfo} so that {@code projections()} is included as a
     * property. This lets {@link org.elasticsearch.xpack.esql.core.tree.NodeInfo#transform} visit
     * expressions inside the projections (needed by ResolveRefs).
     */
    static ResolvingProject create(
        Source source,
        LogicalPlan child,
        List<? extends NamedExpression> projections,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        UnmappedFieldsPattern unmappedFieldsPattern
    ) {
        return new ResolvingProject(source, child, projections, resolver, unmappedFieldsPattern);
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(this, ResolvingProject::create, child(), projections(), resolver, unmappedFieldsPattern);
    }

    @Override
    public ResolvingProject replaceChild(LogicalPlan newChild) {
        return new ResolvingProject(source(), newChild, resolver, unmappedFieldsPattern);
    }

    @Override
    public Project withProjections(List<? extends NamedExpression> projections) {
        return new ResolvingProject(source(), child(), projections, resolver, unmappedFieldsPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolver, unmappedFieldsPattern);
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
            && Objects.equals(unmappedFieldsPattern, other.unmappedFieldsPattern);
    }

    public Project asProject() {
        return new Project(source(), child(), projections());
    }
}
