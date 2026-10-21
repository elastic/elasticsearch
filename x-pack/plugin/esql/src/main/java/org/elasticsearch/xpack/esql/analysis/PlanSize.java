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
import java.util.IdentityHashMap;
import java.util.Map;

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
     * A running total that starts with the nodes of the parsed {@code plan}, rejected past {@link QueryPragmas#maxParsedPlanNodes}.
     * The parser applies {@link #checkParsedPlan} at every FORK it builds; this is the check for plans built elsewhere.
     */
    public static PlanSize of(LogicalPlan plan, int max) {
        return new PlanSize(checkParsedPlan(plan, max, new IdentityHashMap<>()), max);
    }

    /**
     * Counts {@code plan} through {@code sizes} and rejects it past {@link QueryPragmas#maxParsedPlanNodes}. A FORK repeats its
     * input in every branch and the count follows, so FORKs nested through subqueries are rejected here, before the plan is walked
     * once per path by anything else. Returns the count.
     */
    public static int checkParsedPlan(LogicalPlan plan, int max, Map<LogicalPlan, Integer> sizes) {
        int size = countNodes(plan, sizes);
        if (size > QueryPragmas.maxParsedPlanNodes(max)) {
            throw exceeded(max, size);
        }
        return size;
    }

    /**
     * Adds {@code addedNodes} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(int addedNodes) {
        size = (int) Math.min((long) size + addedNodes, Integer.MAX_VALUE);
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
     * Adds the nodes of {@code plan} to the total.
     */
    public void addNodesOf(LogicalPlan plan) {
        add(countNodes(plan));
    }

    public int size() {
        return size;
    }

    /**
     * Counts the nodes of {@code plan}, a shared child being counted once per parent: the size the plan reaches once analysis copies
     * a FORK's input per branch.
     */
    public static int countNodes(LogicalPlan plan) {
        return countNodes(plan, new IdentityHashMap<>());
    }

    /**
     * Like {@link #countNodes(LogicalPlan)}, with the count of every node reached kept in {@code sizes} by identity, so that a node
     * is walked once however many parents hold it and a later count over a plan that contains it finds it ready. FORK branches
     * share their input by reference, so without this the walk would take one step per path, exponential in the FORKs nested.
     * Saturates at {@link Integer#MAX_VALUE}. Iterative, so deep plans cannot overflow the stack.
     */
    public static int countNodes(LogicalPlan plan, Map<LogicalPlan, Integer> sizes) {
        Deque<LogicalPlan> pending = new ArrayDeque<>();
        pending.push(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan node = pending.peek();
            if (sizes.containsKey(node)) {
                pending.pop();
                continue;
            }
            long size = 1;
            boolean childrenCounted = true;
            for (LogicalPlan child : node.children()) {
                Integer childSize = sizes.get(child);
                if (childSize == null) {
                    pending.push(child);
                    childrenCounted = false;
                } else {
                    size += childSize;
                }
            }
            if (childrenCounted) {
                sizes.put(node, (int) Math.min(size, Integer.MAX_VALUE));
                pending.pop();
            }
        }
        return sizes.get(plan);
    }
}
