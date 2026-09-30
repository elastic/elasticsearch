/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

/**
 * Adaptive distribution strategy for external sources.
 * <p>
 * Distributes when the plan reduces rows (aggregations, TopN, and the same operators inside a
 * fragment), when the split count exceeds the number of eligible remote workers, or when this is
 * a UNION leaf with sibling datasets. Stays on the coordinator for a single split that has no
 * source siblings, for LIMIT-only plans that filter nothing before the limit, and for an empty
 * split list. A filter under the limit (any {@code WHERE}, including a runtime {@code MATCH})
 * can force the scan through most of the data before the limit fills, so such a plan follows
 * the same split-count rule as a plain scan instead of reading every split on one node. An
 * empty eligible-worker set, including an index-only cluster, returns {@code LOCAL} so the
 * coordinator runs the scan itself.
 * <p>
 * Assignment is offset by {@link SiblingPlacement#stride(int, int)} so concurrent UNION leaves
 * (and concurrent FORK branches) do not all start at eligible node 0.
 */
public final class AdaptiveStrategy implements ExternalDistributionStrategy {

    private final NodeEligibilityStrategy eligibility;

    public AdaptiveStrategy(NodeEligibilityStrategy eligibility) {
        if (eligibility == null) {
            throw new IllegalArgumentException("eligibility must not be null");
        }
        this.eligibility = eligibility;
    }

    public AdaptiveStrategy() {
        this(NodeEligibilityStrategy.EXTERNAL_WORKER_NODES);
    }

    @Override
    public ExternalDistributionPlan planDistribution(ExternalDistributionContext context) {
        List<ExternalSplit> splits = context.splits();
        if (splits.isEmpty()) {
            return ExternalDistributionPlan.LOCAL;
        }
        if (splits.size() == 1 && context.placement().hasSourceSiblings() == false) {
            return ExternalDistributionPlan.LOCAL;
        }

        PhysicalPlan plan = context.plan();

        if (isLimitOnly(plan)) {
            return ExternalDistributionPlan.LOCAL;
        }

        List<DiscoveryNode> nodes = eligibility.eligibleNodes(context.availableNodes());
        if (nodes.isEmpty()) {
            return ExternalDistributionPlan.LOCAL;
        }

        boolean hasPipelineBreaker = ExternalDistributionStrategy.hasReducingOperator(plan);
        boolean manySplits = splits.size() > nodes.size();

        if (hasPipelineBreaker || manySplits || context.placement().hasSourceSiblings()) {
            int stride = context.placement().stride(splits.size(), nodes.size());
            boolean allHaveSize = true;
            for (ExternalSplit split : splits) {
                // Unknown size is negative. Zero is an empty file; it still has open cost.
                if (split.estimatedSizeInBytes() < 0) {
                    allHaveSize = false;
                    break;
                }
            }
            if (allHaveSize) {
                return WeightedRoundRobinStrategy.assignByWeight(splits, nodes, stride);
            }
            return RoundRobinStrategy.assignRoundRobin(splits, nodes, stride);
        }

        return ExternalDistributionPlan.LOCAL;
    }

    /**
     * True when a coordinator {@code LimitExec} is the only breaker and nothing drops rows before a
     * limit: no aggregation, TopN, TopNBy, or LimitBy on the physical tree or inside a fragment, and no
     * filter between the scan and the first limit above it. Such a read stops after about {@code LIMIT}
     * rows, so one node is cheapest. A UNION STATS leaf that still carries a limit is not limit-only,
     * so the reduction check can still hop it.
     */
    private static boolean isLimitOnly(PhysicalPlan plan) {
        return plan.anyMatch(n -> n instanceof LimitExec)
            && ExternalDistributionStrategy.hasReducingOperator(plan) == false
            && filtersBeforeLimit(plan) == false;
    }

    /**
     * Whether a filter runs on the scan side of every limit: a {@link FilterExec}, or a logical {@link Filter}
     * inside a {@link FragmentExec}, with no limit under it. A filter above a limit
     * ({@code LIMIT 100 | WHERE x}) only sees the rows that limit already let through, so it does not count.
     */
    private static boolean filtersBeforeLimit(PhysicalPlan plan) {
        return plan.anyMatch(
            n -> (n instanceof FilterExec filter && filter.child().anyMatch(c -> c instanceof LimitExec) == false)
                || (n instanceof FragmentExec fragment && fragment.fragment().anyMatch(AdaptiveStrategy::isFilterBeforeLimit))
        );
    }

    private static boolean isFilterBeforeLimit(LogicalPlan node) {
        return node instanceof Filter filter && filter.child().anyMatch(c -> c instanceof Limit) == false;
    }
}
