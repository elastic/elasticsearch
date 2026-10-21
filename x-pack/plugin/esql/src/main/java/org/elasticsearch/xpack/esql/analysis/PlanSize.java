/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Running total of plan nodes in the query being expanded, checked against the maximum allowed. Written from inside the async
 * resolution callbacks, with cross-thread visibility provided by the ActionListener plumbing like the other threaded state.
 */
public class PlanSize {
    private final int max;
    private int size;

    public PlanSize(int initialSize, int max) {
        this.size = initialSize;
        this.max = max;
    }

    /**
     * A running total that starts with the nodes of the parsed {@code plan}. A FORK repeats its upstream plan in every branch, so a
     * query within the command limit may already hold {@link QueryPragmas#MAX_BRANCH_COUNT_PER_MERGE_MAX} times {@code max} nodes,
     * plus the FORK's own, before anything is expanded into it; that much is counted and, as long as nothing is expanded, allowed.
     * Past that ceiling the plan can only be FORKs nested through subqueries, whose branches share their input by reference and whose
     * path count grows exponentially, so it is rejected without being walked any further.
     */
    public static PlanSize of(LogicalPlan plan, int max) {
        int ceiling = (int) Math.min((long) max * (QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE_MAX + 1), Integer.MAX_VALUE);
        int size = countNodes(plan, ceiling);
        if (size > ceiling) {
            throw exceeded(max, size);
        }
        return new PlanSize(size, max);
    }

    /**
     * Adds {@code addedNodes} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(int addedNodes) {
        size += addedNodes;
        if (size > max) {
            throw exceeded(max, size);
        }
    }

    private static VerificationException exceeded(int max, int size) {
        return new VerificationException(
            "ES|QL query exceeded the maximum of [{}] plan nodes allowed after expanding views, subqueries and FORKs. "
                + "Found at least [{}] plan nodes.",
            max,
            size
        );
    }

    /**
     * Adds the nodes of {@code plan} to the total. A plan whose FORK branches share their input by reference has one path per
     * branch combination, so the count stops as soon as the maximum is exceeded rather than walking every path.
     */
    public void addNodesOf(LogicalPlan plan) {
        add(countNodes(plan, max - size));
    }

    public int size() {
        return size;
    }

    /**
     * Counts the nodes of {@code plan}, a shared child being counted once per parent. Iterative, so deep plans cannot overflow the stack.
     */
    public static int countNodes(LogicalPlan plan) {
        return countNodes(plan, Integer.MAX_VALUE);
    }

    /**
     * Like {@link #countNodes(LogicalPlan)}, but gives up once the count exceeds {@code limit}, returning {@code limit + 1}.
     */
    private static int countNodes(LogicalPlan plan, int limit) {
        int count = 0;
        Deque<LogicalPlan> pending = new ArrayDeque<>();
        pending.push(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan node = pending.pop();
            count++;
            if (count > limit) {
                return count;
            }
            node.children().forEach(pending::push);
        }
        return count;
    }
}
