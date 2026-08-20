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

    /** The kind of command a {@link Command} was built from, i.e. how its projections translate into an {@link UnmappedFieldsPattern}. */
    public enum Kind {
        KEEP,
        DROP,
        RENAME
    }

    /**
     * The command this node was built from: the projections as they were written, plus how to resolve them against an input.
     * <p>
     * This is deliberately kept out of {@link ResolvingProject#info()}. Node properties are what plan transformations rewrite, and
     * {@link #projections} must stay unresolved for {@link #unmappedFieldsPattern()} to read them.
     */
    public record Command(
        Kind kind,
        List<? extends NamedExpression> projections,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver
    ) {
        /**
         * Which unmapped source fields this command lets through, derived from the projections it was written with — by the time
         * {@link ResolvingProject#projections()} is computed, {@code ResolveRefs} has replaced the original wildcard expressions with
         * the attributes they matched. Only {@code DetermineUnmappedFieldsToKeep} reads this, so it is only ever derived under
         * {@code unmapped_fields="LOAD_ALL"}.
         */
        public UnmappedFieldsPattern unmappedFieldsPattern() {
            return switch (kind) {
                case KEEP -> UnmappedFieldsPattern.forKeep(projections);
                case DROP -> UnmappedFieldsPattern.forDrop(projections);
                // A RENAME keeps every column, so it restricts nothing; its target names shadow the source fields of the same name, which
                // DetermineUnmappedFieldsToKeep excludes from this node's output on its own.
                case RENAME -> UnmappedFieldsPattern.ALL;
            };
        }
    }

    private final Command command;

    public ResolvingProject(Source source, LogicalPlan child, Command command) {
        this(source, child, computeProjections(child.output(), command.resolver(), command.kind()), command);
    }

    /**
     * Runs the resolver against the child output, keeping any {@link UnmappedFieldsAttribute} instances out of the resolver's scope
     * (so KEEP/DROP/RENAME patterns cannot match the synthetic column), and re-inserting them at the correct position.
     */
    private static List<? extends NamedExpression> computeProjections(
        List<Attribute> childOutput,
        Function<List<Attribute>, List<? extends NamedExpression>> resolver,
        Kind kind
    ) {
        List<Attribute> unmappedAttrs = new ArrayList<>();
        List<Attribute> resolverInput = new ArrayList<>();
        int beforeCount = 0;
        for (Attribute a : childOutput) {
            if (a instanceof UnmappedFieldsAttribute) {
                unmappedAttrs.add(a);
            } else {
                resolverInput.add(a);
                if (unmappedAttrs.isEmpty()) {
                    beforeCount++;
                }
            }
        }
        if (unmappedAttrs.isEmpty()) {
            return resolver.apply(childOutput);
        }
        List<? extends NamedExpression> resolved = resolver.apply(resolverInput);
        List<NamedExpression> result = new ArrayList<>(resolved.size() + unmappedAttrs.size());
        if (kind == Kind.KEEP) {
            result.addAll(resolved);
            result.addAll(unmappedAttrs);
        } else {
            int insertAt = Math.min(beforeCount, resolved.size());
            result.addAll(resolved.subList(0, insertAt));
            result.addAll(unmappedAttrs);
            result.addAll(resolved.subList(insertAt, resolved.size()));
        }
        return result;
    }

    private ResolvingProject(Source source, LogicalPlan child, List<? extends NamedExpression> projections, Command command) {
        super(source, child, projections);
        this.command = command;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public UnmappedFieldsPattern unmappedFieldsPattern() {
        return command.unmappedFieldsPattern();
    }

    /** Whether this node was built from a {@code KEEP} (as opposed to a {@code DROP} or {@code RENAME}). */
    public boolean isKeep() {
        return command.kind() == Kind.KEEP;
    }

    /** Whether this node was built from a {@code RENAME}. */
    public boolean isRename() {
        return command.kind() == Kind.RENAME;
    }

    /**
     * This {@code KEEP}'s projection terms in written order, for replaying its column ordering over {@code LOAD_ALL}-expanded leaves on
     * the coordinator (see {@link UnmappedFieldsPattern#keepOrdered}). Only valid on a KEEP; reads the same still-unresolved projections
     * {@link Command#unmappedFieldsPattern()} does, so it must run while the {@link ResolvingProject} is intact (before
     * {@code ResolvedProjects}).
     */
    public List<UnmappedFieldsPattern.KeepTerm> keepOrderTerms() {
        assert isKeep() : "keepOrderTerms is only defined for a KEEP, not " + command.kind();
        return UnmappedFieldsPattern.orderTerms(command.projections());
    }

    @Override
    protected NodeInfo<Project> info() {
        return NodeInfo.create(
            this,
            (source, child, projections) -> new ResolvingProject(source, child, projections, command),
            child(),
            projections()
        );
    }

    @Override
    public ResolvingProject replaceChild(LogicalPlan newChild) {
        return new ResolvingProject(source(), newChild, command);
    }

    @Override
    public Project withProjections(List<? extends NamedExpression> projections) {
        return new ResolvingProject(source(), child(), projections, command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), command);
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
        return super.equals(obj) && Objects.equals(command, other.command);
    }

    public Project asProject() {
        return new Project(source(), child(), projections());
    }
}
