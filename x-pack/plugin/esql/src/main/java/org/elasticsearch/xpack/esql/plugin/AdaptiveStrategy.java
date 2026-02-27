/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * Adaptive distribution strategy for external sources.
 * <p>
 * Distributes when the plan contains aggregations and there are multiple splits,
 * or when the split count exceeds the number of eligible nodes.
 * Stays on the coordinator for single splits or LIMIT-only plans.
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
        this(NodeEligibilityStrategy.DATA_NODES_ONLY);
    }

    @Override
    public ExternalDistributionPlan planDistribution(ExternalDistributionContext context) {
        List<ExternalSplit> splits = context.splits();
        if (splits.size() <= 1) {
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

        boolean hasAggregation = plan.anyMatch(n -> n instanceof AggregateExec);
        boolean manySplits = splits.size() > nodes.size();

        if (hasAggregation || manySplits) {
            return RoundRobinStrategy.assignRoundRobin(splits, nodes);
        }

        return ExternalDistributionPlan.LOCAL;
    }

    private static boolean isLimitOnly(PhysicalPlan plan) {
        boolean hasLimit = plan.anyMatch(n -> n instanceof LimitExec);
        boolean hasAgg = plan.anyMatch(n -> n instanceof AggregateExec);
        boolean hasTopN = plan.anyMatch(n -> n instanceof TopNExec);
        return hasLimit && hasAgg == false && hasTopN == false;
    }
}
