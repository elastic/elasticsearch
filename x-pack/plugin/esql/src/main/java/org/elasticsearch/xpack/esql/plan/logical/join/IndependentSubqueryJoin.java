/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.Predicate;

/**
 * A join whose right side is a separate query. That query runs on its own, before the main query, and only its result reaches the
 * join. None of its sources, aggregations, limits, sorts or fields feed the main query's rows, so plan walks that reason about the
 * main query must not descend into it. The right side is always child {@link #SUBQUERY_CHILD_INDEX}.
 */
public interface IndependentSubqueryJoin {

    /** The position of the right side among the join's children. */
    int SUBQUERY_CHILD_INDEX = 1;

    LogicalPlan left();

    LogicalPlan right();

    /**
     * Returns the children of {@code plan} that belong to the same query as {@code plan}: only the left side of an
     * {@link IndependentSubqueryJoin}, all children otherwise.
     */
    static List<LogicalPlan> mainScopeChildren(LogicalPlan plan) {
        return plan instanceof IndependentSubqueryJoin join ? List.of(join.left()) : plan.children();
    }

    /**
     * Returns whether the child at {@code childIndex} of {@code parent} is the right side of an {@link IndependentSubqueryJoin}.
     * Shaped for {@code Node#transformUpSkipBranch}.
     */
    static boolean isSubqueryBranch(LogicalPlan parent, int childIndex) {
        return parent instanceof IndependentSubqueryJoin && parent instanceof BinaryPlan && childIndex == SUBQUERY_CHILD_INDEX;
    }

    /**
     * Like {@code Node#anyMatch}, but does not descend into the right side of an {@link IndependentSubqueryJoin}.
     */
    static boolean anyMatchInMainScope(LogicalPlan plan, Predicate<? super LogicalPlan> predicate) {
        if (predicate.test(plan)) {
            return true;
        }
        for (LogicalPlan child : mainScopeChildren(plan)) {
            if (anyMatchInMainScope(child, predicate)) {
                return true;
            }
        }
        return false;
    }
}
