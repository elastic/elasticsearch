/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manages the lifecycle of {@link BalancingSummary} data structures tracking allocation balancing round results. There are many balancing
 * rounds and this class manages their reporting.
 *
 * Summarizing balancer rounds and reporting the results will provide information with which to do a cost-benefit analysis of the work that
 * the allocation rebalancing performs.
 *
 * TODO (Dianna): how to handle master step down. Probably refuse to take further add*() calls, but return any previous results that have
 * not yet been drained for reporting
 */
public class AllocationBalancingRoundSummaryService {

    public static final AllocationBalancingRoundSummaryService NOOP = new AllocationBalancingRoundSummaryService();

    /** Value to return if no balancing rounds have occurred in the requested time period. */
    public static final BalancingSummary.CombinedClusterBalancingRoundSummary EMPTY_RESULTS =
        new BalancingSummary.CombinedClusterBalancingRoundSummary(
            new LinkedList<>(),
            new BalancingSummary.ClusterShardAssignments(0L, 0L, 0L, 0L, 0L, 0L),
            new HashMap<>()
        );

    /**
     * A concurrency-safe list of balancing round summaries. Balancer rounds are run and added here serially, so the queue will naturally
     * progress from newer to older results.
     */
    private ConcurrentLinkedQueue<BalancingSummary.BalancingRoundSummary> summaries = new ConcurrentLinkedQueue<>();

    /**
     * Returns a combined summary of all unreported allocation round summaries: may summarize a single balancer round, multiple, or none.
     *
     * @return returns {@link #EMPTY_RESULTS} if there are no unreported balancing rounds.
     */
    public BalancingSummary.CombinedClusterBalancingRoundSummary combineSummaries() {
        // TODO: implement
        return EMPTY_RESULTS;
    }

    public void addBalancerRoundSummary(BalancingSummary.BalancingRoundSummary summary) {
        summaries.add(summary);
    }

}
