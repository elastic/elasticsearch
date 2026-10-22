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
 * What a {@link BranchPlan} branch <em>is</em>: a resolved view (with its name), a user-written subquery, a resolved
 * dataset (with its name), or a bare index pattern. Carried as first-class state on the union node — not inferred from
 * transient {@link Subquery}/{@link NamedSubquery} wrappers, whose survival depends on compaction decisions, and not
 * from the branch map's keys, which are uniquified ({@code "meow~2"}) and so are labels rather than names.
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

    /**
     * A resolved dataset branch, as built by {@code DatasetRewriter}. Lineage stays empty: it records the chain of
     * <em>view</em> names a branch passed through, and a dataset is not a view. A dataset piece inside a view body
     * still promotes to the view's identity when lifted — see {@link #liftedThrough}.
     */
    public static BranchIdentity dataset(String name) {
        return new BranchIdentity(name, RelationClass.DATASET, List.of());
    }

    public boolean isView() {
        return kind == RelationClass.VIEW;
    }

    /**
     * The view whose own {@code SET} governs this branch. {@link #name()} is the outermost view after compaction
     * lifts a nested view into its parent — that name is what warnings report — while {@link #lineage()} still ends
     * with the view that owned the piece. A definition's settings do not leak into a view nested inside it, so they
     * are keyed by this innermost name. Before any lift, lineage is just {@code [name]} and the two agree.
     */
    public String settingsViewName() {
        if (isView() == false) {
            return null;
        }
        if (lineage.isEmpty() == false) {
            return lineage.getLast();
        }
        return name;
    }

    /**
     * The identity a branch has after being lifted out of {@code outer}'s subtree into {@code outer}'s own level.
     * Lifting through a view promotes every piece to the view's identity — the outer view's name wins (lifting happens
     * only after the metadata freeze, so this name serves warnings and {@code _lineage}, not {@code _name}), with the
     * outer lineage prepended so the nesting chain survives. Lifting through a non-view branch changes nothing.
     *
     * <p>Everything inside a view body belongs to the view: boundary features must treat a lifted piece exactly like
     * the un-lifted view. Concretely, the request-filter rewriter targets {@code kind == VIEW} branches with the
     * DSL-translated filter applied to the branch <em>output</em>; a piece that computes a column, left un-promoted,
     * would take the raw Lucene pushdown instead, which cannot express a filter on that column. This mirrors the
     * identity propagation the plain-{@code MergePlan} lift has always done for view bodies. (The lift site exempts
     * bare non-view relation pieces, for which the Lucene path is equivalent — see {@code ViewCompaction}.)
     */
    public BranchIdentity liftedThrough(BranchIdentity outer) {
        if (outer.isView() == false) {
            return this;
        }
        List<String> chain = new ArrayList<>(outer.lineage);
        chain.addAll(lineage);
        return new BranchIdentity(outer.name, RelationClass.VIEW, List.copyOf(chain));
    }
}
