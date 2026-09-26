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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Turns a {@link ViewUnionAll} that is only a resolved {@code FROM} into a {@link SourceFanInUnionAll}.
 * <p>
 * A view whose body is datasets, indices, or a mix, including a {@code WHERE} (or another unary
 * pipeline) on that expansion beside a matched namesake, is the same source list the dataset
 * rewriter builds. An index-only view union stays a {@link ViewUnionAll}. A {@code FORK}, a join,
 * a subquery, or a union the user wrote is not a source list and is left alone.
 * <p>
 * This runs for every query, not only under {@code FORK}: every fan-in in the plan also has its
 * sibling index reads merged (see {@link SourceFanInUnionAll#withIndexReadsCollapsed}).
 * <p>
 * A view union that stays a {@link ViewUnionAll} has any fan-in branch lifted into separate
 * branches, the same shape {@code ViewCompaction} builds for a user-written union inside a view,
 * so a fan-in is never left nested under a view union it was not promoted into.
 */
public final class PromoteSourceFanIn extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return promote(plan, context.preserveViewBoundaries());
    }

    /** {@link #promote(LogicalPlan, boolean)} for a request without a DSL filter. */
    public static LogicalPlan promote(LogicalPlan plan) {
        return promote(plan, false);
    }

    /**
     * Promotes every source-expansion view union, merges sibling index reads in every fan-in (see
     * {@link SourceFanInUnionAll#withIndexReadsCollapsed}), and collapses any single-child fan-in that results.
     *
     * @param preserveViewBoundaries {@code true} when the request carries a DSL filter that is applied at view
     *                               output boundaries. A view branch that computes over its sources then keeps
     *                               its {@link ViewUnionAll}, because promotion would drop that boundary.
     */
    public static LogicalPlan promote(LogicalPlan plan, boolean preserveViewBoundaries) {
        LogicalPlan promoted = plan.transformUp(ViewUnionAll.class, view -> promoteOne(view, preserveViewBoundaries));
        return collapseSingleChildFanIns(promoted.transformUp(SourceFanInUnionAll.class, SourceFanInUnionAll::withIndexReadsCollapsed));
    }

    private static LogicalPlan collapseSingleChildFanIns(LogicalPlan plan) {
        return plan.transformDown(SourceFanInUnionAll.class, fanIn -> {
            if (fanIn.children().size() == 1) {
                return fanIn.children().getFirst();
            }
            return fanIn;
        });
    }

    private static LogicalPlan promoteOne(ViewUnionAll view, boolean preserveViewBoundaries) {
        if (SourceFanInUnionAll.isSourceExpansion(view) == false || (preserveViewBoundaries && viewOutputMatchesSources(view) == false)) {
            return liftFanInBranches(view);
        }
        return new SourceFanInUnionAll(view.source(), new ArrayList<>(view.children()), view.output());
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
        LinkedHashMap<String, LogicalPlan> flat = new LinkedHashMap<>();
        Set<String> viewBranchKeys = new HashSet<>();
        Set<String> originalKeys = view.namedSubqueries().keySet();
        for (Map.Entry<String, LogicalPlan> entry : view.namedSubqueries().entrySet()) {
            if (entry.getValue() instanceof SourceFanInUnionAll fanIn) {
                String parentKey = entry.getKey() == null ? "main" : entry.getKey();
                int childIndex = 1;
                for (LogicalPlan child : fanIn.children()) {
                    flat.put(uniqueKey(flat, originalKeys, parentKey + "#" + childIndex++), child);
                }
            } else {
                flat.put(entry.getKey(), entry.getValue());
                if (view.isViewBranch(entry.getKey())) {
                    viewBranchKeys.add(entry.getKey());
                }
            }
        }
        if (flat.size() > MergePlan.MAX_BRANCHES) {
            return view;
        }
        return new ViewUnionAll(view.source(), flat, viewBranchKeys, view.output());
    }

    private static String uniqueKey(Map<String, LogicalPlan> flat, Set<String> originalKeys, String key) {
        String candidate = key;
        int counter = 2;
        while (flat.containsKey(candidate) || originalKeys.contains(candidate)) {
            candidate = key + "#" + counter++;
        }
        return candidate;
    }
}
