/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Running total of plan nodes in the query being expanded, checked against the maximum allowed. Written from inside the async
 * resolution callbacks, with cross-thread visibility provided by the ActionListener plumbing like the other threaded state.
 */
public class PlanSize {
    private final int max;
    private int size;
    private final Map<LogicalPlan, Integer> sizes = new IdentityHashMap<>();
    private final Map<LogicalPlan, UnionAll.UnionStats> unionStats = new IdentityHashMap<>();

    public PlanSize(int max) {
        this.size = 0;
        this.max = max;
    }

    /**
     * Starts the total at the nodes of {@code initial}, failing if they already exceed the maximum.
     */
    public PlanSize(LogicalPlan initial, int max) {
        this.size = countNodes(initial);
        this.max = max;
    }

    // Used in testing
    public PlanSize(int initialSize, int max) {
        this.size = initialSize;
        this.max = max;
    }

    /**
     * Adds {@code addedNodes} to the total and fails as soon as the maximum is exceeded.
     */
    public void add(int addedNodes) {
        addNodes(addedNodes);
    }

    private void addNodes(int addedNodes) {
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
     * Fails if the leaf branches of {@code plan}, counted once per path as {@link UnionAll#unionStats} does, exceed
     * {@code maxBranchCount}
     */
    public void checkPlanWidth(LogicalPlan plan, int maxBranchCount) {
        UnionAll.UnionStats stats = UnionAll.unionStats(plan, unionStats);
        if (stats.leaves() > maxBranchCount) {
            String source = QueryPragmas.EMPTY.maxBranchCountLimitSource(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey());
            throw new VerificationException(
                List.of(Failure.fail(plan, UnionAll.TOTAL_BRANCH_COUNT_ERROR, stats.leaves(), maxBranchCount, source))
            );
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
