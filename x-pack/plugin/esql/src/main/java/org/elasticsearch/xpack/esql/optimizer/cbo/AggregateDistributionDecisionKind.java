/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.EstimateView;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.NamedEstimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Chooses whether a grouped aggregation combines its partial results on the coordinator or hash-partitions them across the data
 * nodes.
 */
public final class AggregateDistributionDecisionKind implements DecisionKind {
    public static final AggregateDistributionDecisionKind INSTANCE = new AggregateDistributionDecisionKind();
    public static final String NAME = "aggregate_distribution";

    private static final List<String> DOMAIN = List.of(AggMode.COORDINATOR.name(), AggMode.DISTRIBUTED.name());

    private AggregateDistributionDecisionKind() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean appliesTo(LogicalPlan plan) {
        return plan instanceof Aggregate aggregate && CostBasedOptimizer.Search.supportedAggregate(aggregate);
    }

    @Override
    public List<String> domain(LogicalPlan plan) {
        return DOMAIN;
    }

    @Override
    public AggKey key(LogicalPlan plan) {
        return AggKey.of(((Aggregate) plan).aggregates());
    }

    @Override
    public List<Attribute> anchor(LogicalPlan plan) {
        return Aggregate.output(((Aggregate) plan).aggregates());
    }

    @Override
    public List<NamedEstimate> inputs(LogicalPlan plan, Function<LogicalPlan, PlanStats> stats) {
        Aggregate aggregate = (Aggregate) plan;
        PlanStats input = stats.apply(aggregate.child());
        CostBasedOptimizer.GroupEstimate groups = CostBasedOptimizer.Search.groupEstimate(aggregate, input);
        EstimateView groupCount = groups.unknown()
            ? EstimateView.of(Estimate.UNKNOWN)
            : new EstimateView(groups.value(), groups.upperBound(), groups.provenance());
        return List.of(
            NamedEstimate.of("input_rows", input == null ? Estimate.UNKNOWN : input.rows()),
            new NamedEstimate("group_count", groupCount)
        );
    }

    @Override
    public Optional<Observation> observe(PhysicalPlan plan, Object key) {
        List<Observation> found = new ArrayList<>(1);
        plan.forEachDown(AggregateExec.class, aggregate -> {
            if (found.isEmpty() == false || AggKey.of(aggregate.aggregates()).equals(key) == false) {
                return;
            }
            if (aggregate.getMode() == AggregatorMode.FINAL) {
                boolean hashed = aggregate.child() instanceof ShuffleExchangeExec exchange
                    && exchange.distribution() == ExchangeDistribution.HASH;
                AggMode effective = hashed ? AggMode.DISTRIBUTED : AggMode.COORDINATOR;
                found.add(new Observation(effective.name(), aggregate.nodeString(), JoinStrategyDecisionKind.COORDINATOR_PLAN));
            } else if (aggregate.getMode() == AggregatorMode.SINGLE) {
                AggMode effective = runsOnHashPartitions(aggregate.child()) ? AggMode.DISTRIBUTED : AggMode.COORDINATOR;
                found.add(new Observation(effective.name(), aggregate.nodeString(), JoinStrategyDecisionKind.COORDINATOR_PLAN));
            }
        });
        return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
    }

    /**
     * Whether a single-phase aggregate runs in place on hash partitions, because its input is already partitioned by a hash
     * exchange or a partitioned join below it. The first exchange or join reached through unary nodes decides.
     */
    private static boolean runsOnHashPartitions(PhysicalPlan plan) {
        PhysicalPlan node = plan;
        while (true) {
            if (node instanceof ShuffleExchangeExec exchange) {
                return exchange.distribution() == ExchangeDistribution.HASH;
            }
            if (node instanceof DistributedHashJoinExec join) {
                return join.mode() == DistributedHashJoinExec.Mode.PARTITIONED;
            }
            if (node instanceof MergeExec merge) {
                return merge.children().isEmpty() == false
                    && merge.children().stream().allMatch(AggregateDistributionDecisionKind::runsOnHashPartitions);
            }
            if (node instanceof UnaryExec unary && node instanceof ExchangeExec == false) {
                node = unary.child();
                continue;
            }
            return false;
        }
    }
}
