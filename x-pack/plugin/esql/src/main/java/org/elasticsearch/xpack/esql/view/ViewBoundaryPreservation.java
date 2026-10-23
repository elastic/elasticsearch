/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;

import java.util.Map;

/**
 * How much view structure {@link ViewResolver} must keep intact for the boundary features that act on a view's
 * <em>output</em>. The levels are ordered — each keeps everything the previous one keeps — and the request decides
 * the level once, in {@code EsqlSession}: the weakest level whose guarantees cover everything the request carries.
 * Everything kept is collapse-eligible again once its consumer has acted; the level only controls what survives
 * resolution and compaction long enough to be consumed.
 *
 * <p>{@link #NONE}: nothing acts at view outputs. Compaction applies in full, with single-branch
 * {@link org.elasticsearch.xpack.esql.plan.logical.BranchPlan}s collapsing to their sole child.
 *
 * <p>{@link #KEEP_VIEW_BRANCHES} — a request DSL filter applies to view outputs: non-pass-through view subplans stay
 * wrapped in a {@link org.elasticsearch.xpack.esql.plan.logical.BranchPlan} with a view
 * {@link org.elasticsearch.xpack.esql.plan.logical.BranchIdentity}, which is how
 * {@link org.elasticsearch.xpack.esql.dsltranslate.ViewRequestFilterRewriter} finds view output boundaries to apply
 * the translated filter at the view level rather than letting the raw DSL be pushed into the view's source scan.
 * Pass-through views (bare-{@link org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation} bodies) still
 * collapse into their patterns: filtering a pass-through view's output and filtering its source index are the same
 * operation, and the Lucene path honors filter constructs the DSL translator cannot express, so collapsing is both
 * safe and strictly more faithful there.
 *
 * <p>{@link #KEEP_VIEW_IDENTITY} — a query setting must stop at view boundaries (currently: {@code unmapped_fields}
 * is not {@code DEFAULT}, or {@code wildcards_match_datasets} is on): pass-through views keep
 * their identity too, wrapped as named view branches instead of merging with sibling or outer patterns. The view
 * branches are the boundary itself: {@code ResolveUnmapped} skips every view branch wholesale and null-fills at its
 * output, and the dataset rewrite turns wildcard dataset matching off inside view branches, so a view must never end up
 * outside one — a merged relation serves caller-owned content as well, and would either leak the setting into the
 * view or wrongly deny it to the caller's own pattern. The pass-through equivalence that lets {@link #KEEP_VIEW_BRANCHES}
 * collapse does not hold here: loading an unmapped field from {@code _source} at the index is not the same as
 * null-filling at the view output — a view is a virtual index with no {@code _source} of its own.
 */
public enum ViewBoundaryPreservation {
    NONE,
    KEEP_VIEW_BRANCHES,
    KEEP_VIEW_IDENTITY;

    /**
     * Whether the query settings alone require {@link #KEEP_VIEW_IDENTITY}: a non-{@code DEFAULT} {@code unmapped_fields}, or
     * {@code wildcards_match_datasets} on. The single source of truth for that decision — production resolution and the
     * analyzer test harness must both call it so they cannot drift when a new boundary-scoped setting is added.
     */
    public static boolean settingsRequireViewIdentity(UnmappedResolution unmappedResolution, boolean wildcardsMatchDatasets) {
        return unmappedResolution != UnmappedResolution.DEFAULT || wildcardsMatchDatasets;
    }

    /**
     * Whether the analyzer must keep view branches because of the caller's {@code unmapped_fields}: only that setting is applied
     * during analysis ({@code ResolveUnmapped} acts on the view branches). {@code wildcards_match_datasets} is applied before it, on
     * the plan view resolution produced, so by the time of analysis nothing relies on the view branches for it.
     */
    public static boolean analysisRequiresViewBranches(UnmappedResolution unmappedResolution) {
        return unmappedResolution != UnmappedResolution.DEFAULT;
    }

    /**
     * Whether the analyzer must keep view branches because of {@code unmapped_fields}, either the caller's or a {@code SET} in a view
     * definition. A definition's setting applies to that view's body alone, so its branch has to survive compaction for
     * {@code ResolveUnmapped} to find it. Shared by production and the analyzer test harness so they cannot drift.
     */
    public static boolean analysisRequiresViewBranches(UnmappedResolution unmappedResolution, Map<String, ViewQuerySettings> viewSettings) {
        return analysisRequiresViewBranches(unmappedResolution)
            || viewSettings.values().stream().anyMatch(ViewQuerySettings::requiresViewBranches);
    }

    /** View subplans stay wrapped as identified {@code BranchPlan} branches (both non-{@link #NONE} levels). */
    public boolean keepsViewBranches() {
        return this != NONE;
    }

    /** Pass-through views keep their identity too, instead of merging with caller-owned patterns. */
    public boolean keepsPassThroughIdentity() {
        return this == KEEP_VIEW_IDENTITY;
    }
}
