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
 * Running total of plan nodes in the query being expanded, checked against the maximum allowed, and the width of the plans built
 * so far, checked against the maximum leaf branches. Both walks cache what they have counted by node identity, so a plan whose
 * FORK branches share their input is walked once rather than once per path. Written from inside the async resolution callbacks,
 * with cross-thread visibility provided by the ActionListener plumbing like the other threaded state.
 */
public class PlanSize {
    /** Where the limit comes from when no query pragma sets it, named in the error. */
    public static final String CLUSTER_SETTING_SOURCE = QueryPragmas.EMPTY.maxCommandCountLimitSource(
        EsqlFlags.ESQL_MAX_COMMAND_COUNT.getKey()
    );

    private final int max;
    private final String maxSource;
    private int size;
    private final Map<LogicalPlan, Integer> sizes = new IdentityHashMap<>();
    private final Map<LogicalPlan, UnionAll.UnionStats> unionStats = new IdentityHashMap<>();

    /**
     * A size that is counted but never checked, for plans that another limit bounds: the ones the parser builds, and IN subqueries.
     */
    public static PlanSize unlimited() {
        return new PlanSize(Integer.MAX_VALUE, CLUSTER_SETTING_SOURCE);
    }

    /**
     * Starts the total node count at 0.
     *
     * @param maxSource the pragma or cluster setting that supplied {@code max}, named in the error
     */
    public PlanSize(int max, String maxSource) {
        this.size = 0;
        this.max = max;
        this.maxSource = maxSource;
    }

    /**
     * Starts the total at the nodes of {@code initial} without checking whether this is past the {@code max} threshold.
     */
    public PlanSize(LogicalPlan initial, int max, String maxSource) {
        this(max, maxSource);
        this.size = countNodes(initial);
    }

    // Only for testing
    public PlanSize(int initialSize, int max) {
        this(max, CLUSTER_SETTING_SOURCE);
        this.size = initialSize;
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
            throw maxPlanNodesExceeded(size);
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
    public void checkPlanWidth(LogicalPlan plan, int maxBranchCount, String maxBranchCountSource) {
        UnionAll.UnionStats stats = UnionAll.unionStats(plan, unionStats);
        if (stats.leaves() > maxBranchCount) {
            throw new VerificationException(
                List.of(Failure.fail(plan, UnionAll.PARTIAL_BRANCH_COUNT_ERROR, stats.leaves(), maxBranchCount, maxBranchCountSource))
            );
        }
    }

    private VerificationException maxPlanNodesExceeded(int found) {
        return new VerificationException(
            "ES|QL query exceeded the maximum of [{}] plan nodes allowed after expanding views, subqueries and FORKs. "
                + "Found at least [{}] plan nodes. The maximum is set by the {}.",
            max,
            found,
            maxSource
        );
    }

    public int size() {
        return size;
    }

    /**
     * Counts the nodes of {@code plan}, uses a cache to re-visiting already walked nodes. A shared child gets counted once per parent.
     * Iterative, so deep plans cannot overflow the stack.
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
