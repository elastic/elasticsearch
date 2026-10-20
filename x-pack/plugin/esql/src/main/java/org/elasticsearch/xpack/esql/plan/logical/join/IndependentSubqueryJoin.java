/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A join whose right side is a non-correlated query scope, separate from the outer pipeline.
 */
public interface IndependentSubqueryJoin extends SubPlanJoin {

    /**
     * Returns the outer pipeline whose rows are preserved or filtered by this join.
     */
    LogicalPlan left();

    /**
     * Returns the left-side attributes used to match or evaluate the join.
     */
    AttributeSet leftReferences();

    /**
     * Returns children that belong to the query scope rooted at {@code plan}.
     */
    static List<LogicalPlan> mainScopeChildren(LogicalPlan plan) {
        return plan instanceof IndependentSubqueryJoin join ? List.of(join.left()) : plan.children();
    }

    /**
     * Visits the query scope rooted at {@code plan}, without descending into independent subquery right sides.
     */
    static void forEachInMainScope(LogicalPlan plan, Consumer<LogicalPlan> consumer) {
        consumer.accept(plan);
        for (LogicalPlan child : mainScopeChildren(plan)) {
            forEachInMainScope(child, consumer);
        }
    }

    /**
     * Visits the query scope rooted at {@code plan} in post-order, without descending into independent subquery right sides.
     */
    static void forEachUpInMainScope(LogicalPlan plan, Consumer<LogicalPlan> consumer) {
        for (LogicalPlan child : mainScopeChildren(plan)) {
            forEachUpInMainScope(child, consumer);
        }
        consumer.accept(plan);
    }

    /**
     * Visits the query scope rooted at {@code plan} until {@code predicate} returns {@code true}, without descending into independent
     * subquery right sides.
     */
    static boolean forEachInMainScopeUntil(LogicalPlan plan, Predicate<LogicalPlan> predicate) {
        if (predicate.test(plan)) {
            return true;
        }
        for (LogicalPlan child : mainScopeChildren(plan)) {
            if (forEachInMainScopeUntil(child, predicate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether any node in the query scope rooted at {@code plan} matches {@code predicate}.
     */
    static boolean anyMatchInMainScope(LogicalPlan plan, Predicate<LogicalPlan> predicate) {
        return forEachInMainScopeUntil(plan, predicate);
    }

    /**
     * Collects the first matching nodes on each branch of the query scope rooted at {@code plan}.
     */
    static List<LogicalPlan> collectFirstChildrenInMainScope(LogicalPlan plan, Predicate<LogicalPlan> predicate) {
        List<LogicalPlan> matches = new ArrayList<>();
        collectFirstChildrenInMainScope(plan, predicate, matches);
        return matches;
    }

    /**
     * Transforms the query scope rooted at {@code plan} bottom-up, without transforming independent subquery right sides.
     */
    static LogicalPlan transformUpInMainScope(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> transform) {
        return transform.apply(transformChildrenInMainScope(plan, child -> transformUpInMainScope(child, transform)));
    }

    /**
     * Transforms the query scope rooted at {@code plan} top-down, without transforming independent subquery right sides.
     */
    static LogicalPlan transformDownInMainScope(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> transform) {
        return transformDownInMainScopeSkipBranch(plan, (node, skipBranch) -> transform.apply(node));
    }

    /**
     * Transforms the query scope rooted at {@code plan} top-down, skipping descendants when {@code transform} sets the supplied holder
     * to {@code true}, and never descending into independent subquery right sides.
     */
    static LogicalPlan transformDownInMainScopeSkipBranch(
        LogicalPlan plan,
        BiFunction<LogicalPlan, Holder<Boolean>, LogicalPlan> transform
    ) {
        Holder<Boolean> skipBranch = new Holder<>(Boolean.FALSE);
        LogicalPlan transformedPlan = transform.apply(plan, skipBranch);
        if (skipBranch.get()) {
            return transformedPlan;
        }
        return transformChildrenInMainScope(transformedPlan, child -> transformDownInMainScopeSkipBranch(child, transform));
    }

    private static LogicalPlan transformChildrenInMainScope(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> transformChild) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> mainScopeChildren = mainScopeChildren(plan);
        List<LogicalPlan> transformedChildren = null;
        for (int i = 0; i < mainScopeChildren.size(); i++) {
            LogicalPlan child = mainScopeChildren.get(i);
            LogicalPlan transformedChild = transformChild.apply(child);
            if (child.equals(transformedChild) == false) {
                if (transformedChildren == null) {
                    transformedChildren = new ArrayList<>(children);
                }
                transformedChildren.set(i, transformedChild);
            }
        }
        return transformedChildren == null ? plan : plan.replaceChildren(transformedChildren);
    }

    private static void collectFirstChildrenInMainScope(LogicalPlan plan, Predicate<LogicalPlan> predicate, List<LogicalPlan> matches) {
        if (predicate.test(plan)) {
            matches.add(plan);
            return;
        }
        for (LogicalPlan child : mainScopeChildren(plan)) {
            collectFirstChildrenInMainScope(child, predicate, matches);
        }
    }
}
