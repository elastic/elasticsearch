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
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.util.List;
import java.util.function.Predicate;

/**
 * Adaptive distribution strategy for external sources.
 * <p>
 * Distributes when the plan contains pipeline breakers (aggregations, TopN)
 * and there are multiple splits, or when the split count exceeds the number
 * of eligible remote workers. Stays on the coordinator for LIMIT-only plans, and for a single split when that split
 * is the query's only external read. An empty eligible-worker set, including an index-only cluster, returns
 * {@code LOCAL} so the coordinator runs the scan itself.
 * <p>
 * A fan-in with several producers treats a single-split producer as distributable: placing every small
 * producer on the coordinator would pile those reads onto one node. Reduction is judged by
 * {@link #reducesRowsWhenDistributed}, which also looks inside an unresolved fragment so a pushed-down
 * aggregation still counts.
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
     * Broader than {@link ExternalDistributionStrategy#needsGatherBoundary}: that rule is a correctness
     * check for a local read's operators, this one only decides whether distributing pays. A fan-in
     * producer still holds its pushed-down aggregation as a logical {@link Aggregate} inside
     * {@link FragmentExec}, so the physical tree alone would report no reduction.
     */
    private static boolean reducesRowsWhenDistributed(PhysicalPlan plan) {
        return ExternalDistributionStrategy.needsGatherBoundary(plan) || fragmentHolds(plan, AdaptiveStrategy::reducesRows);
    }

    /**
     * The pushed-down operators that shrink a producer's output before it crosses the fan-in. A plain {@code Limit} is
     * absent on purpose: it does reduce rows, but a limit-only read is cheapest where the limit is applied once, which
     * is what {@link #isLimitOnly} keeps on the coordinator.
     */
    private static boolean reducesRows(LogicalPlan node) {
        return node instanceof Aggregate || node instanceof TopN || node instanceof TopNBy || node instanceof LimitBy;
    }

    /**
     * A fan-in producer carries its pushed-down operators as a logical plan inside an unresolved {@link FragmentExec},
     * so the physical tree alone answers no for every one of them.
     */
    private static boolean fragmentHolds(PhysicalPlan plan, Predicate<LogicalPlan> predicate) {
        return plan.anyMatch(node -> node instanceof FragmentExec fragment && fragment.fragment().anyMatch(predicate));
    }

    private static boolean isLimitOnly(PhysicalPlan plan) {
        boolean hasLimit = plan.anyMatch(n -> n instanceof LimitExec) || fragmentHolds(plan, node -> node instanceof Limit);
        boolean hasAgg = plan.anyMatch(n -> n instanceof AggregateExec);
        boolean hasTopN = plan.anyMatch(n -> n instanceof TopNExec);
        return hasLimit && hasAgg == false && hasTopN == false && fragmentHolds(plan, AdaptiveStrategy::reducesRows) == false;
    }
}
