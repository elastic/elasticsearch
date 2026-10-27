/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

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
    private final Map<LogicalPlan, Integer> sizes = new IdentityHashMap<>();

    /**
     * Starts the total at the nodes of {@code initial} without checking it: a plan over the maximum is accepted until something is
     * expanded into it.
     */
    public PlanSize(LogicalPlan initial, int max) {
        this.max = max;
        this.size = countNodes(initial);
    }

    public PlanSize(int max) {
        this.size = 0;
        this.max = max;
    }

    public PlanSize(int initialSize, int max) {
        if (initialSize > max) {
            throw exceeded(initialSize);
        }
        this.size = initialSize;
        this.max = max;
    }

    /**
     * Adds {@code addedNodes} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(int addedNodes) {
        size = (int) Math.min((long) size + addedNodes, Integer.MAX_VALUE);
        if (size > max) {
            throw exceeded(size);
        }
    }

    /**
     * Adds the nodes of {@code plan} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(LogicalPlan plan) {
        add(countNodes(plan));
    }

    /**
     * Fails if {@code plan} by itself exceeds the maximum, leaving the total alone: the check for a plan still being built, where
     * every plan checked holds the ones checked before it.
     */
    public void check(LogicalPlan plan) {
        int nodes = countNodes(plan);
        if (nodes > max) {
            throw exceeded(nodes);
        }
    }

    private VerificationException exceeded(int found) {
        return new VerificationException(
            "ES|QL query exceeded the maximum of [{}] plan nodes allowed after expanding views, subqueries and FORKs. "
                + "Found at least [{}] plan nodes.",
            max,
            found
        );
    }

    public int size() {
        return size;
    }

    /**
     * Counts the nodes of {@code plan}, uses a cache to re-visiting already walked nodes.
     */
    public final int countNodes(LogicalPlan plan) {
        Deque<LogicalPlan> pending = new ArrayDeque<>();
        pending.push(plan);

        while (pending.isEmpty() == false) {
            LogicalPlan node = pending.peek();
            if (sizes.containsKey(node)) {
                pending.pop();
                continue;
            }
            long nodes = 1;
            for (LogicalPlan child : node.children()) {
                Integer childNodes = sizes.get(child);
                if (childNodes == null) {
                    pending.push(child);
                } else {
                    nodes += childNodes;
                }
            }
            if (pending.peek() == node) {
                sizes.put(node, (int) Math.min(nodes, Integer.MAX_VALUE));
                pending.pop();
            }
        }
        return sizes.get(plan);
    }
}
