/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;

/**
 * Adaptive distribution strategy for external sources.
 * <p>
 * Distributes when the plan contains pipeline breakers (aggregations, TopN)
 * and there are multiple splits, or when the split count exceeds the number
 * of eligible nodes. Stays on the coordinator for LIMIT-only plans, and for a single split when that split is the
 * query's only external read.
 * <p>
 * A single split is placed like any other once the query has several producers reading concurrently. Distributing one
 * split buys no parallelism and costs a transport hop, which is why a lone read stays put, but each producer of a
 * fan-in decides this on its own: applying the lone-read reasoning per producer would put every read of a query over
 * many small datasets on the coordinator at once, so the hop it saves stops being what limits the query.
 * <p>
 * Reduction is judged by {@link #reducesRowsWhenDistributed}, which unlike the gather rule also looks inside an
 * unresolved fragment. A fan-in producer carries its pushed-down aggregation as a logical plan, so judging it by its
 * physical nodes would find no reduction in any of them.
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
        if (splits.size() <= 1 && context.producerCount() <= 1) {
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

        boolean hasPipelineBreaker = reducesRowsWhenDistributed(plan);
        boolean manySplits = splits.size() > nodes.size();

        if (hasPipelineBreaker || manySplits) {
            boolean allHaveSize = true;
            for (ExternalSplit split : splits) {
                if (split.estimatedSizeInBytes() <= 0) {
                    allHaveSize = false;
                    break;
                }
            }
            if (allHaveSize) {
                return WeightedRoundRobinStrategy.assignByWeight(splits, nodes, context.producerIndex());
            }
            return RoundRobinStrategy.assignRoundRobin(splits, nodes, context.producerIndex());
        }

        return ExternalDistributionPlan.LOCAL;
    }

    /**
     * Whether distributing this read would have a data node reduce rows before shipping them back.
     *
     * <p>Deliberately broader than {@link ExternalDistributionStrategy#needsGatherBoundary}, and asking a different
     * question. That rule decides a correctness matter for a read that stays put: whether its operators may be
     * replicated across the parallel drivers of a single node. It has to stay narrow, because it also governs whether
     * a local read keeps its exchange. This one only decides whether distributing pays, so it can safely answer yes
     * more often: distributing is correct either way, since a data node plans and gathers its own slice.
     *
     * <p>What it adds is the fan-in producer, which arrives here as an unresolved {@link FragmentExec} still holding
     * its pushed-down aggregation as a logical {@link Aggregate}. No {@code AggregateExec} exists in the tree yet, so
     * reading the physical nodes alone reports no reduction for precisely the producers that a query over many
     * datasets consists of, and every one of them would be read on the coordinator.
     */
    private static boolean reducesRowsWhenDistributed(PhysicalPlan plan) {
        return ExternalDistributionStrategy.needsGatherBoundary(plan)
            || plan.anyMatch(
                node -> node instanceof FragmentExec fragment
                    && fragment.fragment().anyMatch(inner -> inner instanceof Aggregate || inner instanceof TopN)
            );
    }

    private static boolean isLimitOnly(PhysicalPlan plan) {
        boolean hasLimit = plan.anyMatch(n -> n instanceof LimitExec);
        boolean hasAgg = plan.anyMatch(n -> n instanceof AggregateExec);
        boolean hasTopN = plan.anyMatch(n -> n instanceof TopNExec);
        return hasLimit && hasAgg == false && hasTopN == false;
    }
}
