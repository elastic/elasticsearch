/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.RelationClass;

import java.util.ArrayList;
import java.util.List;

/**
 * What a {@link ViewUnionAll} branch <em>is</em>: a resolved view (with its name), a user-written subquery, or a bare
 * index pattern. Carried as first-class state on the union node — not inferred from transient {@link Subquery}/
 * {@link NamedSubquery} wrappers, whose survival depends on compaction decisions, and not from the branch map's keys,
 * which are uniquified ({@code "meow#2"}) and so are labels rather than names.
 *
 * <p>An identity describes only its own nesting level, and it is the <em>durable</em> truth about that level: the kind
 * the boundary features consult (the request-filter rewriter targets {@code kind == VIEW} branches, {@code Mapper}
 * marks the fragments beneath them). The <em>request-point</em> truth — what {@code METADATA _class}/{@code _name}
 * report — is transient by design: it lives in the parser/resolver {@link Subquery}/{@link NamedSubquery} wrappers
 * and is consumed exactly once, by the analyzer's metadata freeze, before any compaction can disturb it. A branch
 * like {@code (FROM my_view)} therefore needs no second identity: its wrapper answers {@code subquery} at the freeze,
 * and the view's own identity lives a level deeper until post-freeze compaction lifts it.
 *
 * <p>{@link #lineage} records the chain of view names a branch passed through during resolution, outermost first; a
 * plain view's lineage is just its own name. It exists to answer a future {@code METADATA _lineage} and is not yet
 * consumed. (Tracked under esql-planning#2264.)
 */
public record BranchIdentity(@Nullable String name, RelationClass kind, List<String> lineage) {

    /** A bare index-pattern branch: the existing pre-analysis Lucene paths apply, nothing view-like to track. */
    public static final BranchIdentity INDEX = new BranchIdentity(null, RelationClass.INDEX, List.of());

    /** A literal user-written subquery branch: named by the user’s text, not by any catalog entity. */
    public static final BranchIdentity SUBQUERY = new BranchIdentity(null, RelationClass.SUBQUERY, List.of());

    public static BranchIdentity view(String name) {
        return new BranchIdentity(name, RelationClass.VIEW, List.of(name));
    }

    public boolean isView() {
        return kind == RelationClass.VIEW;
    }

    /**
     * The identity a view branch has after being lifted out of {@code outer}'s subtree into {@code outer}'s own level:
     * the outer view's name wins (lifting happens only after the metadata freeze, so this name serves warnings and
     * {@code _lineage}, not {@code _name}), with the outer lineage prepended so the nesting chain survives. Lifting
     * through a non-view branch, or lifting a non-view piece, changes nothing.
     *
     * <p>Deliberately narrow: a bare index or subquery piece inside a view body keeps its own identity when lifted, so
     * that lifting stays behaviour-preserving — bare pieces remain mergeable with sibling patterns and keep the Lucene
     * filter path. Promoting them to the enclosing view's identity ("everything inside a view belongs to the view") is
     * a semantic change tracked under esql-planning#2264.
     */
    public BranchIdentity liftedThrough(BranchIdentity outer) {
        if (outer.isView() == false || isView() == false) {
            return this;
        }
        List<String> chain = new ArrayList<>(outer.lineage);
        chain.addAll(lineage);
        return new BranchIdentity(outer.name, RelationClass.VIEW, List.copyOf(chain));
    }
}
