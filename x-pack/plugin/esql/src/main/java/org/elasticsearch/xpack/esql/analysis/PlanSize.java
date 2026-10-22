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
     * Adds {@code addedNodes} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(int addedNodes) {
        size += addedNodes;
        if (size > max) {
            throw new VerificationException(
                "ES|QL query exceeded the maximum of [{}] plan nodes allowed after expanding views, subqueries and FORKs. "
                    + "Found at least [{}] plan nodes.",
                max,
                size
            );
        }
    }

    public int size() {
        return size;
    }

    /**
     * Counts the nodes of {@code plan}, a shared child being counted once per parent. Iterative, so deep plans cannot overflow the stack.
     */
    public static int countNodes(LogicalPlan plan) {
        int count = 0;
        Deque<LogicalPlan> pending = new ArrayDeque<>();
        pending.push(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan node = pending.pop();
            count++;
            node.children().forEach(pending::push);
        }
        return count;
    }
}
