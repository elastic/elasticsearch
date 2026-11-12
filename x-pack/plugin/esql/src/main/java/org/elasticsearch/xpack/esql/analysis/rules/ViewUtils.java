/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Walks a plan without entering the bodies of views, for the analyzer rules that a view is a boundary for: query settings such as
 * {@code unmapped_fields} do not reach into a view, which is a virtual index with no {@code _source} of its own.
 * <p>
 * A view body is a view branch of a {@link BranchPlan}, and being one is a property of the branch's position in its parent, not of
 * the node. It therefore holds however the plan around or atop the view is rewritten, which a mark on the nodes themselves
 * (such as the {@code Source} of a node) would not. It relies on the view branches being kept while such a setting is active; see
 * {@code ViewBoundaryPreservation#KEEP_VIEW_IDENTITY}.
 * <p>
 * Two traversals differ on the root of a view body. {@link #forEachOutsideViews} does not visit it, as it is part of the view. {@link
 * #anyMatchOutsideViews} does test it, as it is what the view contributes to the query (its output), but nothing below it.
 */
public final class ViewUtils {
    private ViewUtils() {}

    /** Whether {@code parent}'s child at {@code childIndex} is a view body: a view branch of a {@link BranchPlan}. */
    public static boolean isViewBody(LogicalPlan parent, int childIndex) {
        return parent instanceof BranchPlan branchPlan && branchPlan.isViewBranch(childIndex);
    }

    /** Applies {@code rule} bottom-up to every node of {@code plan} except those inside view bodies, the roots of which included. */
    public static LogicalPlan transformUpOutsideViews(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> rule) {
        return plan.transformUpSkipBranch(ViewUtils::isViewBody, rule);
    }

    /** {@link #transformUpOutsideViews(LogicalPlan, Function)}, for the nodes of one type. */
    public static <T extends LogicalPlan> LogicalPlan transformUpOutsideViews(
        LogicalPlan plan,
        Class<T> type,
        Function<T, LogicalPlan> rule
    ) {
        return transformUpOutsideViews(plan, p -> type.isInstance(p) ? rule.apply(type.cast(p)) : p);
    }

    /** Visits {@code plan} top-down, skipping view bodies: the root of a view body is not visited either. */
    public static void forEachOutsideViews(LogicalPlan plan, Consumer<LogicalPlan> action) {
        action.accept(plan);
        List<LogicalPlan> children = plan.children();
        for (int i = 0; i < children.size(); i++) {
            if (isViewBody(plan, i) == false) {
                forEachOutsideViews(children.get(i), action);
            }
        }
    }

    /**
     * Whether {@code predicate} holds for any node of {@code plan} outside view bodies. The root of a view body is tested, as it is
     * what the view contributes to the query (its output), but nothing below it.
     */
    public static boolean anyMatchOutsideViews(LogicalPlan plan, Predicate<LogicalPlan> predicate) {
        if (predicate.test(plan)) {
            return true;
        }
        List<LogicalPlan> children = plan.children();
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            if (isViewBody(plan, i) ? predicate.test(child) : anyMatchOutsideViews(child, predicate)) {
                return true;
            }
        }
        return false;
    }
}
