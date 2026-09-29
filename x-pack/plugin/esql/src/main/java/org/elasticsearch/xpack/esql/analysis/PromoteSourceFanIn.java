/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.view.ViewCompaction;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Turns a {@link ViewUnionAll} that is only a resolved {@code FROM} into a {@link SourceFanInUnionAll}.
 * <p>
 * A view whose body is datasets, indices, or a mix, including any pipeline on that expansion
 * beside a matched namesake, is the same source list the dataset rewriter builds. An index-only
 * view union stays a {@link ViewUnionAll}. A branch holding a {@code FORK}, a union, or a subquery
 * the user wrote is not a source list and is left alone (see {@link SourceFanInUnionAll#isBranching}).
 * <p>
 * This runs for every query, not only under {@code FORK}: every fan-in in the plan also has its
 * sibling index reads merged (see {@link SourceFanInUnionAll#withIndexReadsCollapsed}).
 * <p>
 * A view union that stays a {@link ViewUnionAll} has any fan-in branch lifted into separate
 * branches, the same shape {@code ViewCompaction} builds for a user-written union inside a view.
 * When lifting would exceed {@link MergePlan#MAX_BRANCHES}, the view union keeps its nested fan-in.
 */
public final class PromoteSourceFanIn extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return promote(plan, context.preserveViewBoundaries(), context.unmappedResolution().loadsUnmappedFields());
    }

    /**
     * Promotes every source-expansion view union, merges sibling index reads in every fan-in (see
     * {@link SourceFanInUnionAll#withIndexReadsCollapsed}), and collapses any single-child fan-in that results.
     *
     * @param preserveViewBoundaries {@code true} when the request carries a DSL filter that is applied at view
     *                               output boundaries. A view branch that computes over its sources then keeps
     *                               its {@link ViewUnionAll}, because promotion would drop that boundary.
     * @param loadUnmappedFields     {@code true} when unmapped fields are loaded, see
     *                               {@link SourceFanInUnionAll#withIndexReadsCollapsed}
     */
    public static LogicalPlan promote(LogicalPlan plan, boolean preserveViewBoundaries, boolean loadUnmappedFields) {
        return plan.transformUp(ViewUnionAll.class, view -> promoteOne(view, preserveViewBoundaries))
            .transformUp(SourceFanInUnionAll.class, fanIn -> fanIn.withIndexReadsCollapsed(loadUnmappedFields))
            .transformDown(SourceFanInUnionAll.class, SourceFanInUnionAll::collapseSingleChild);
    }

    private static LogicalPlan promoteOne(ViewUnionAll view, boolean preserveViewBoundaries) {
        if (canPromote(view, preserveViewBoundaries)) {
            return new SourceFanInUnionAll(view.source(), view.children(), view.output());
        }
        return liftFanInBranches(view);
    }

    /**
     * True when {@code view} is one {@code FROM}'s sources and promoting it keeps request-filter results the same.
     * Promotion drops the view boundaries, so with a filter applied at view outputs every view branch must be
     * sources under {@code WHERE}s only.
     */
    private static boolean canPromote(ViewUnionAll view, boolean preserveViewBoundaries) {
        if (SourceFanInUnionAll.isSourceExpansion(view) == false) {
            return false;
        }
        return preserveViewBoundaries == false || viewOutputMatchesSources(view);
    }

    /**
     * True when a request filter applied below every view branch gives the same rows as one applied on the
     * view's output: each view branch is its sources, optionally under {@code WHERE}s. Any other command can
     * compute, rename, drop, or limit what the filter sees.
     */
    private static boolean viewOutputMatchesSources(ViewUnionAll view) {
        for (Map.Entry<String, LogicalPlan> entry : view.namedSubqueries().entrySet()) {
            if (view.isViewBranch(entry.getKey()) == false) {
                continue;
            }
            LogicalPlan current = entry.getValue();
            while (current instanceof Filter filter) {
                current = filter.child();
            }
            if ((current instanceof SourceFanInUnionAll || current instanceof ExternalRelation || current instanceof EsRelation) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Lifts every branch that is itself a fan-in into one branch per producer. The lifted branches are not view
     * branches: each is a bare producer, so a request filter reads the same fields below it as above it.
     * Returns {@code view} unchanged when nothing is lifted or lifting would exceed {@link MergePlan#MAX_BRANCHES}.
     */
    private static LogicalPlan liftFanInBranches(ViewUnionAll view) {
        if (view.children().stream().noneMatch(child -> child instanceof SourceFanInUnionAll)) {
            return view;
        }
        // Kept branches go in first so a lifted key cannot take a kept branch's name, as in ViewCompaction.
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        Set<String> viewBranchKeys = new HashSet<>();
        for (Map.Entry<String, LogicalPlan> entry : view.namedSubqueries().entrySet()) {
            if (entry.getValue() instanceof SourceFanInUnionAll == false) {
                flat.put(entry.getKey(), entry.getValue());
                if (view.isViewBranch(entry.getKey())) {
                    viewBranchKeys.add(entry.getKey());
                }
            }
        }
        for (Map.Entry<String, LogicalPlan> entry : view.namedSubqueries().entrySet()) {
            if (entry.getValue() instanceof SourceFanInUnionAll fanIn) {
                for (LogicalPlan child : fanIn.children()) {
                    flat.put(ViewCompaction.makeUniqueKey(flat, entry.getKey()), child);
                }
            }
        }
        if (flat.size() > MergePlan.MAX_BRANCHES) {
            return view;
        }
        return new ViewUnionAll(view.source(), flat, viewBranchKeys, view.output());
    }
}
