/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.ColumnStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.StatsDeriver;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ShuffleRuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Keeps physical distribution choices separate from logical plans so downstream planners can consume decisions without mutating plan nodes.
 */
public final class CostBasedOptimizer {
    /**
     * Searches the logical tree for eligible physical choices while keeping optimizer decisions out of plan nodes.
     */
    public PlanDecisions optimize(LogicalPlan plan, CboContext context) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(context);

        if (eligible(plan, context) == false) {
            return PlanDecisions.EMPTY;
        }

        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>(new StatsDeriver().derive(plan));
        return search(plan, context, statistics);
    }

    /** Runs the same search against supplied estimates so distribution choices can be tested independently. */
    PlanDecisions optimizeWithStatistics(LogicalPlan plan, CboContext context, Map<LogicalPlan, PlanStats> statistics) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(context);
        Objects.requireNonNull(statistics);
        if (eligible(plan, context) == false) {
            return PlanDecisions.EMPTY;
        }
        return search(plan, context, new IdentityHashMap<>(statistics));
    }

    private static PlanDecisions search(LogicalPlan plan, CboContext context, Map<LogicalPlan, PlanStats> statistics) {
        try {
            return new Search(plan, context, statistics).run();
        } catch (SearchAborted e) {
            return PlanDecisions.EMPTY;
        }
    }

    private static boolean eligible(LogicalPlan plan, CboContext context) {
        Configuration configuration = context.configuration();
        QueryPragmas pragmas = configuration.pragmas();
        return pragmas.cbo()
            && pragmas.shuffleEnabled()
            && ShuffleGate.allowed(configuration, plan)
            && context.minimumVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION);
    }

    private static final class Search {
        private final CboContext context;
        private final Configuration configuration;
        private final QueryPragmas pragmas;
        private final Map<LogicalPlan, PlanStats> statistics;
        private final IdentityHashMap<LogicalPlan, Group> memo = new IdentityHashMap<>();
        private final int workerLanes;
        private final boolean shuffleAllowed;
        private final Group root;
        private int evaluatedCandidates;

        private Search(LogicalPlan plan, CboContext context, Map<LogicalPlan, PlanStats> statistics) {
            this.context = context;
            this.configuration = context.configuration();
            this.pragmas = configuration.pragmas();
            this.statistics = statistics;
            this.workerLanes = positiveProduct(context.workerNodes(), Math.max(1, pragmas.shufflePartitionsPerNode()));
            this.root = build(plan);
            this.shuffleAllowed = pragmas.shuffleEnabled()
                && ShuffleGate.allowed(configuration, plan)
                && context.minimumVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION);
        }

        private PlanDecisions run() {
            List<Winner> winners = optimize(root, SingleRequirement.INSTANCE);
            Winner best = cheapest(winners);
            return best == null ? PlanDecisions.EMPTY : decisions(best);
        }

        private Group build(LogicalPlan plan) {
            Group existing = memo.get(plan);
            if (existing != null) {
                return existing;
            }
            List<Group> children = new ArrayList<>(plan.children().size());
            for (LogicalPlan child : plan.children()) {
                children.add(build(child));
            }
            Group group = new Group(plan, children, statistics.get(plan));
            memo.put(plan, group);
            return group;
        }

        private List<Winner> optimize(Group group, Requirement required) {
            List<Winner> cached = group.winners.get(required);
            if (cached != null) {
                return cached;
            }

            List<Winner> nativeCandidates = candidates(group);
            List<Winner> feasible = new ArrayList<>(nativeCandidates.size());
            List<Winner> rejectedForRisk = new ArrayList<>();
            for (Winner candidate : nativeCandidates) {
                Winner enforced = enforce(candidate, required);
                if (fitRisk(enforced) == null) {
                    feasible.add(enforced);
                } else {
                    rejectedForRisk.add(enforced);
                }
            }

            if (feasible.isEmpty() && rejectedForRisk.isEmpty() == false) {
                List<Winner> fallback = safeFallback(group, rejectedForRisk);
                for (Winner winner : fallback) {
                    feasible.add(withReason(winner, "safe fallback after fit-risk pruning"));
                }
            }

            List<Winner> winners = cheapestPerDelivered(feasible);
            List<Winner> result = List.copyOf(winners);
            group.winners.put(required, result);
            return result;
        }

        private List<Winner> candidates(Group group) {
            if (group.nativeCandidates != null) {
                return group.nativeCandidates;
            }
            if (group.building) {
                throw new IllegalStateException("logical plan contains a cycle");
            }
            group.building = true;
            try {
                List<Winner> result = new ArrayList<>();
                for (Alternative alternative : alternatives(group)) {
                    List<List<Winner>> childOptions = new ArrayList<>(group.children.size());
                    boolean hasEmptyChild = false;
                    for (int i = 0; i < group.children.size(); i++) {
                        List<Winner> childWinners = optimize(group.children.get(i), alternative.childRequirements().get(i));
                        if (childWinners.isEmpty()) {
                            hasEmptyChild = true;
                            break;
                        }
                        childOptions.add(childWinners);
                    }
                    if (hasEmptyChild) {
                        continue;
                    }
                    combinations(group, alternative, childOptions, 0, new ArrayList<>(group.children.size()), result);
                }
                group.nativeCandidates = List.copyOf(result);
                return group.nativeCandidates;
            } finally {
                group.building = false;
            }
        }

        private void combinations(
            Group group,
            Alternative alternative,
            List<List<Winner>> childOptions,
            int childIndex,
            List<Winner> chosenChildren,
            List<Winner> result
        ) {
            if (childIndex == childOptions.size()) {
                if (evaluatedCandidates >= CostConstants.MAX_CANDIDATES) {
                    throw new SearchAborted();
                }
                evaluatedCandidates++;
                Evaluation evaluation = evaluate(group, alternative, chosenChildren);
                if (evaluation == null) {
                    return;
                }
                Cost cost = childCost(group, alternative, chosenChildren).plus(evaluation.localCost());
                int shuffleEdges = evaluation.shuffleEdges();
                for (Winner child : chosenChildren) {
                    shuffleEdges += child.shuffleEdges();
                }
                result.add(
                    new Winner(
                        group,
                        alternative,
                        chosenChildren,
                        evaluation.delivered(),
                        evaluation.delivered(),
                        evaluation.lanes(),
                        cost,
                        shuffleEdges,
                        evaluation.decisionApplicable(),
                        evaluation.reason()
                    )
                );
                return;
            }

            for (Winner winner : childOptions.get(childIndex)) {
                chosenChildren.add(winner);
                combinations(group, alternative, childOptions, childIndex + 1, chosenChildren, result);
                chosenChildren.remove(chosenChildren.size() - 1);
            }
        }

        private Cost childCost(Group group, Alternative alternative, List<Winner> children) {
            Cost result = Cost.ZERO;
            boolean lookupIndexJoin = alternative.kind() == OperatorKind.JOIN
                && alternative.joinStrategy() == JoinStrategySelector.JoinStrategy.LOOKUP
                && Mapper.isIndexModeLookup(((Join) group.plan).right());
            for (int i = 0; i < children.size(); i++) {
                if (lookupIndexJoin && i == 1) {
                    continue;
                }
                result = result.plus(children.get(i).cost());
            }
            return result;
        }

        private List<Alternative> alternatives(Group group) {
            LogicalPlan plan = group.plan;
            if (plan instanceof EsRelation || plan instanceof ExternalRelation) {
                return List.of(alternative(OperatorKind.SOURCE, List.of(), "SOURCE"));
            }
            if (plan instanceof LocalRelation) {
                return List.of(alternative(OperatorKind.SINGLE, List.of(), "SINGLE"));
            }
            if (plan instanceof Join join) {
                return joinAlternatives(group, join);
            }
            if (plan instanceof Aggregate aggregate) {
                if (supportedAggregate(aggregate)) {
                    return aggregateAlternatives(group, aggregate);
                }
                return List.of(opaque(group));
            }
            if (plan.children().isEmpty()) {
                return List.of(alternative(OperatorKind.SINGLE, List.of(), "SINGLE"));
            }
            if (plan instanceof UnaryPlan unary) {
                if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                    return List.of(opaque(group));
                }
                if (unary instanceof Limit || unary instanceof TopN) {
                    return List.of(alternative(OperatorKind.STREAMING_UNARY, anyRequirements(group), "UNARY"));
                }
                if (unary instanceof PipelineBreaker) {
                    return List.of(opaque(group));
                }
                return List.of(alternative(OperatorKind.STREAMING_UNARY, anyRequirements(group), "UNARY"));
            }
            return List.of(opaque(group));
        }

        private List<Alternative> joinAlternatives(Group group, Join join) {
            if (join instanceof SubqueryLookupJoin) {
                return subqueryJoinAlternatives(group, join);
            }
            if (Mapper.isIndexModeLookup(join.right())) {
                return lookupJoinAlternatives(group, join);
            }
            return List.of(joinAlternative(join, JoinStrategySelector.JoinStrategy.LOCAL, false, List.of("LOCAL"), "incumbent strategy"));
        }

        private List<Alternative> subqueryJoinAlternatives(Group group, Join join) {
            JoinStrategySelector.JoinStrategy incumbent = JoinStrategySelector.JoinStrategy.MATERIALIZED;
            if (pragmas.shuffleJoinExplicit() && pragmas.shuffleJoin() == false) {
                return List.of(joinAlternative(join, incumbent, true, List.of("MATERIALIZED"), "shuffle_join explicitly disables shuffle"));
            }

            boolean eligible = shuffleJoinEligible(join);
            boolean buildKnown = estimateBytes(group.children.get(1).stats).isUnknown() == false;
            if (pragmas.shuffleJoinStrategyExplicit()) {
                JoinStrategySelector.JoinStrategy requested = requestedShuffleStrategy();
                if (eligible && requested != null) {
                    return List.of(joinAlternative(join, requested, true, List.of(requested.name()), "forced by shuffle_join_strategy"));
                }
                return List.of(joinAlternative(join, incumbent, false, List.of(incumbent.name()), "shuffle strategy is not eligible"));
            }

            if (eligible == false) {
                return List.of(joinAlternative(join, incumbent, false, List.of(incumbent.name()), "shuffle strategy is not eligible"));
            }
            if (pragmas.shuffleJoinExplicit()) {
                if (buildKnown == false) {
                    return List.of(
                        joinAlternative(
                            join,
                            JoinStrategySelector.JoinStrategy.PARTITIONED,
                            false,
                            List.of("PARTITIONED"),
                            "gated: unknown build-side bytes; partitioned is the safe shuffle choice"
                        )
                    );
                }
                List<String> choices = List.of("BROADCAST", "PARTITIONED");
                return List.of(
                    joinAlternative(join, JoinStrategySelector.JoinStrategy.BROADCAST, false, choices, "cost-based shuffle choice"),
                    joinAlternative(join, JoinStrategySelector.JoinStrategy.PARTITIONED, false, choices, "cost-based shuffle choice")
                );
            }
            if (buildKnown == false) {
                return List.of(joinAlternative(join, incumbent, false, List.of(incumbent.name()), "gated: unknown build-side bytes"));
            }
            List<String> choices = List.of("MATERIALIZED", "BROADCAST", "PARTITIONED");
            return List.of(
                joinAlternative(join, incumbent, false, choices, "cost-based choice"),
                joinAlternative(join, JoinStrategySelector.JoinStrategy.BROADCAST, false, choices, "cost-based choice"),
                joinAlternative(join, JoinStrategySelector.JoinStrategy.PARTITIONED, false, choices, "cost-based choice")
            );
        }

        private List<Alternative> lookupJoinAlternatives(Group group, Join join) {
            JoinStrategySelector.JoinStrategy incumbent = JoinStrategySelector.JoinStrategy.LOOKUP;
            if (pragmas.shuffleJoinExplicit() && pragmas.shuffleJoin() == false) {
                return List.of(joinAlternative(join, incumbent, true, List.of("LOOKUP"), "shuffle_join explicitly disables shuffle"));
            }

            boolean eligible = shuffleJoinEligible(join);
            if (pragmas.shuffleJoinExplicit()) {
                if (eligible) {
                    return List.of(
                        joinAlternative(
                            join,
                            JoinStrategySelector.JoinStrategy.PARTITIONED,
                            true,
                            List.of("PARTITIONED", "LOOKUP"),
                            "shuffle_join selects partitioned lookup"
                        ),
                        lookupFallbackAlternative(join)
                    );
                }
                return List.of(
                    joinAlternative(join, incumbent, false, List.of("LOOKUP"), "requested shuffle strategy is unavailable for lookup joins")
                );
            }
            if (pragmas.shuffleJoinStrategyExplicit()) {
                JoinStrategySelector.JoinStrategy requested = requestedShuffleStrategy();
                if (eligible && requested == JoinStrategySelector.JoinStrategy.PARTITIONED) {
                    return List.of(
                        joinAlternative(
                            join,
                            JoinStrategySelector.JoinStrategy.PARTITIONED,
                            true,
                            List.of("PARTITIONED", "LOOKUP"),
                            "forced by shuffle_join_strategy"
                        ),
                        lookupFallbackAlternative(join)
                    );
                }
                return List.of(
                    joinAlternative(join, incumbent, false, List.of("LOOKUP"), "requested shuffle strategy is unavailable for lookup joins")
                );
            }

            if (eligible == false) {
                return List.of(joinAlternative(join, incumbent, false, List.of("LOOKUP"), "shuffle strategy is not eligible"));
            }
            boolean buildKnown = estimateBytes(group.children.get(1).stats).isUnknown() == false;
            if (buildKnown == false) {
                return List.of(joinAlternative(join, incumbent, false, List.of("LOOKUP"), "gated: unknown build-side bytes"));
            }
            List<String> choices = List.of("LOOKUP", "PARTITIONED");
            return List.of(
                joinAlternative(join, incumbent, false, choices, "cost-based choice"),
                joinAlternative(join, JoinStrategySelector.JoinStrategy.PARTITIONED, false, choices, "cost-based choice")
            );
        }

        private List<Alternative> aggregateAlternatives(Group group, Aggregate aggregate) {
            if (pragmas.shuffleAggExplicit()) {
                AggMode forced = pragmas.shuffleAgg() ? AggMode.DISTRIBUTED : AggMode.COORDINATOR;
                if (forced == AggMode.DISTRIBUTED && aggregateShuffleEligible(aggregate) == false) {
                    return List.of(
                        aggregateAlternative(
                            aggregate,
                            AggMode.COORDINATOR,
                            false,
                            List.of("COORDINATOR"),
                            "distributed aggregation is not eligible"
                        )
                    );
                }
                return List.of(aggregateAlternative(aggregate, forced, true, List.of(forced.name()), "forced by shuffle_agg"));
            }

            if (aggregateShuffleEligible(aggregate) == false) {
                return List.of(
                    aggregateAlternative(
                        aggregate,
                        AggMode.COORDINATOR,
                        false,
                        List.of("COORDINATOR"),
                        "distributed aggregation is not eligible"
                    )
                );
            }
            if (aggregateEvidenceKnown(group, aggregate) == false) {
                return List.of(
                    aggregateAlternative(
                        aggregate,
                        AggMode.COORDINATOR,
                        false,
                        List.of("COORDINATOR"),
                        "gated: unknown input rows or grouping NDV"
                    )
                );
            }
            List<String> choices = List.of("COORDINATOR", "DISTRIBUTED");
            return List.of(
                aggregateAlternative(aggregate, AggMode.COORDINATOR, false, choices, "cost-based choice"),
                aggregateAlternative(aggregate, AggMode.DISTRIBUTED, false, choices, "cost-based choice")
            );
        }

        private boolean supportedAggregate(Aggregate aggregate) {
            return aggregate.getClass() == Aggregate.class
                && aggregate.groupings().isEmpty() == false
                && ShuffleRuleUtils.hasSupportedGroupings(aggregate.groupings())
                && ShuffleRuleUtils.hasSupportedGroupKeyTypes(aggregate.groupings())
                && groupAttributes(aggregate) != null;
        }

        private boolean aggregateShuffleEligible(Aggregate aggregate) {
            return supportedAggregate(aggregate) && shuffleJoinGate();
        }

        private boolean aggregateEvidenceKnown(Group group, Aggregate aggregate) {
            PlanStats input = group.children.get(0).stats;
            Estimate rows = estimateRows(input);
            return rows.isUnknown() == false && groupEstimate(aggregate, input).unknown() == false;
        }

        private boolean shuffleJoinEligible(Join join) {
            return shuffleJoinGate() && Mapper.distributedJoinEligible(join);
        }

        private boolean shuffleJoinGate() {
            return shuffleAllowed;
        }

        private JoinStrategySelector.JoinStrategy requestedShuffleStrategy() {
            return switch (pragmas.shuffleJoinStrategy()) {
                case "partitioned" -> JoinStrategySelector.JoinStrategy.PARTITIONED;
                case "broadcast" -> JoinStrategySelector.JoinStrategy.BROADCAST;
                default -> throw new IllegalArgumentException(
                    "unsupported [shuffle_join_strategy] value [" + pragmas.shuffleJoinStrategy() + "]"
                );
            };
        }

        private Alternative opaque(Group group) {
            List<Requirement> requirements = new ArrayList<>(group.children.size());
            for (int i = 0; i < group.children.size(); i++) {
                requirements.add(SingleRequirement.INSTANCE);
            }
            return alternative(OperatorKind.OPAQUE, requirements, "SINGLE");
        }

        private List<Requirement> anyRequirements(Group group) {
            List<Requirement> requirements = new ArrayList<>(group.children.size());
            for (int i = 0; i < group.children.size(); i++) {
                requirements.add(AnyRequirement.INSTANCE);
            }
            return requirements;
        }

        private Alternative alternative(OperatorKind kind, List<Requirement> requirements, String label) {
            return new Alternative(kind, label, requirements, null, null, false, false, List.of(label), "property-preserving operator");
        }

        private Alternative joinAlternative(
            Join join,
            JoinStrategySelector.JoinStrategy strategy,
            boolean forced,
            List<String> choices,
            String reason
        ) {
            Requirement leftRequirement = AnyRequirement.INSTANCE;
            Requirement rightRequirement = AnyRequirement.INSTANCE;
            switch (strategy) {
                case MATERIALIZED -> rightRequirement = SingleRequirement.INSTANCE;
                case BROADCAST -> rightRequirement = BroadcastRequirement.INSTANCE;
                case PARTITIONED -> {
                    leftRequirement = new HashRequirement(ids(join.config().leftFields()));
                    rightRequirement = new HashRequirement(ids(join.config().rightFields()));
                }
                case LOOKUP, LOCAL -> {
                    leftRequirement = AnyRequirement.INSTANCE;
                    rightRequirement = AnyRequirement.INSTANCE;
                }
            }
            return new Alternative(
                OperatorKind.JOIN,
                strategy.name(),
                List.of(leftRequirement, rightRequirement),
                strategy,
                null,
                forced,
                false,
                choices,
                reason
            );
        }

        private Alternative lookupFallbackAlternative(Join join) {
            return new Alternative(
                OperatorKind.JOIN,
                JoinStrategySelector.JoinStrategy.LOOKUP.name(),
                List.of(AnyRequirement.INSTANCE, AnyRequirement.INSTANCE),
                JoinStrategySelector.JoinStrategy.LOOKUP,
                null,
                true,
                true,
                List.of("PARTITIONED", "LOOKUP"),
                "lookup remains the fallback when the left side is not a fragment"
            );
        }

        private Alternative aggregateAlternative(Aggregate aggregate, AggMode mode, boolean forced, List<String> choices, String reason) {
            return new Alternative(
                OperatorKind.AGGREGATE,
                mode.name(),
                List.of(AnyRequirement.INSTANCE),
                null,
                mode,
                forced,
                false,
                choices,
                reason
            );
        }

        private Evaluation evaluate(Group group, Alternative alternative, List<Winner> children) {
            return switch (alternative.kind()) {
                case SOURCE -> new Evaluation(
                    SourcePartitioning.INSTANCE,
                    context.workerNodes(),
                    Cost.ZERO,
                    0,
                    false,
                    alternative.reason()
                );
                case SINGLE -> new Evaluation(SinglePartitioning.INSTANCE, 1, Cost.ZERO, 0, false, alternative.reason());
                case OPAQUE -> new Evaluation(SinglePartitioning.INSTANCE, 1, Cost.ZERO, 0, false, alternative.reason());
                case STREAMING_UNARY -> evaluateUnary(group, (UnaryPlan) group.plan, children.get(0));
                case JOIN -> evaluateJoin(group, (Join) group.plan, alternative, children);
                case AGGREGATE -> evaluateAggregate(group, (Aggregate) group.plan, alternative, children.get(0));
            };
        }

        private Evaluation evaluateUnary(Group group, UnaryPlan unary, Winner child) {
            if (unary instanceof Limit limit) {
                if (limit.local() && child.delivered() instanceof SourcePartitioning) {
                    return new Evaluation(
                        SourcePartitioning.INSTANCE,
                        child.lanes(),
                        Cost.ZERO,
                        0,
                        false,
                        "local limit remains in the source fragment"
                    );
                }
                return makeSingle(group.children.get(0), child, "limit requires single distribution");
            }
            if (unary instanceof TopN topN) {
                if (topN.local() && child.delivered() instanceof SourcePartitioning) {
                    return new Evaluation(
                        SourcePartitioning.INSTANCE,
                        child.lanes(),
                        Cost.ZERO,
                        0,
                        false,
                        "local top-n remains in the source fragment"
                    );
                }
                return makeSingle(group.children.get(0), child, "top-n requires single distribution");
            }
            if (child.delivered() instanceof SourcePartitioning) {
                return new Evaluation(
                    SourcePartitioning.INSTANCE,
                    child.lanes(),
                    Cost.ZERO,
                    0,
                    false,
                    "operator remains in the source fragment"
                );
            }
            if (streamingOutsideFragment(unary)) {
                Delivered delivered = translateUp(child.delivered(), unary);
                return new Evaluation(
                    delivered,
                    lanes(delivered, child.lanes()),
                    Cost.ZERO,
                    0,
                    false,
                    "streaming operator preserves child distribution"
                );
            }
            return makeSingle(group.children.get(0), child, "opaque unary operator requires single distribution");
        }

        private Evaluation makeSingle(Group childGroup, Winner child, String reason) {
            if (child.delivered() instanceof SinglePartitioning) {
                return new Evaluation(SinglePartitioning.INSTANCE, 1, Cost.ZERO, 0, false, reason);
            }
            Cost gather = gatherCost(childGroup.stats, child.lanes());
            return new Evaluation(SinglePartitioning.INSTANCE, 1, gather, 0, false, reason);
        }

        private Evaluation evaluateJoin(Group group, Join join, Alternative alternative, List<Winner> children) {
            Winner left = children.get(0);
            Winner right = children.get(1);
            JoinStrategySelector.JoinStrategy strategy = alternative.joinStrategy();
            PlanStats leftStats = group.children.get(0).stats;
            PlanStats rightStats = group.children.get(1).stats;
            Delivered output;
            Cost localCost;
            long outputLanes;

            switch (strategy) {
                case MATERIALIZED -> {
                    output = keepIfPresent(left.delivered(), join.output());
                    outputLanes = lanes(output, left.lanes());
                    localCost = materializedJoinCost(leftStats, rightStats);
                }
                case BROADCAST -> {
                    output = keepIfPresent(left.delivered(), join.output());
                    outputLanes = lanes(output, left.lanes());
                    localCost = broadcastJoinCost(leftStats, rightStats);
                }
                case PARTITIONED -> {
                    if (Mapper.isIndexModeLookup(join.right()) && left.nativeDelivered() instanceof SourcePartitioning == false) {
                        return null;
                    }
                    if ((left.delivered() instanceof HashPartitioning) == false) {
                        return null;
                    }
                    if ((right.delivered() instanceof HashPartitioning) == false) {
                        return null;
                    }
                    HashPartitioning leftHash = (HashPartitioning) left.delivered();
                    List<NameId> keys = ids(join.config().leftFields());
                    output = keepIfPresent(new HashPartitioning(keys, leftHash.exact()), join.output());
                    outputLanes = workerLanes;
                    localCost = partitionedJoinCost(leftStats, rightStats);
                }
                case LOOKUP -> {
                    if (alternative.lookupFallback() && left.nativeDelivered() instanceof SourcePartitioning) {
                        return null;
                    }
                    output = localJoinOutput(join, left);
                    outputLanes = lanes(output, left.lanes());
                    localCost = lookupJoinCost(leftStats);
                    if (left.delivered() instanceof SinglePartitioning == false && join.executesOn() == ExecuteLocation.COORDINATOR) {
                        localCost = localCost.plus(gatherCost(leftStats, left.lanes()));
                    }
                }
                case LOCAL -> {
                    output = localJoinOutput(join, left);
                    outputLanes = lanes(output, left.lanes());
                    localCost = localJoinCost(leftStats, rightStats);
                    if (left.delivered() instanceof SinglePartitioning == false && join.executesOn() == ExecuteLocation.COORDINATOR) {
                        localCost = localCost.plus(gatherCost(leftStats, left.lanes()));
                    }
                }
                default -> throw new IllegalStateException("Unsupported join strategy [" + strategy + "]");
            }

            boolean decisionApplicable = true;
            String reason = alternative.reason();
            return new Evaluation(output, outputLanes, localCost, 0, decisionApplicable, reason);
        }

        private Delivered localJoinOutput(Join join, Winner left) {
            if (join.executesOn() == ExecuteLocation.COORDINATOR) {
                return SinglePartitioning.INSTANCE;
            }
            return keepIfPresent(left.delivered(), join.output());
        }

        private Evaluation evaluateAggregate(Group group, Aggregate aggregate, Alternative alternative, Winner child) {
            PlanStats inputStats = group.children.get(0).stats;
            List<Attribute> groupAttributes = groupAttributes(aggregate);
            if (child.delivered() instanceof SinglePartitioning) {
                return new Evaluation(
                    SinglePartitioning.INSTANCE,
                    1,
                    aggregateInPlaceCost(inputStats, child.lanes(), aggregate),
                    0,
                    false,
                    "aggregate input is already single"
                );
            }
            if (child.delivered() instanceof HashPartitioning hash
                && hash.exact()
                && groupAttributes != null
                && containsIds(ids(groupAttributes), hash.keys())) {
                Delivered output = keepIfPresent(hash, aggregate.output());
                return new Evaluation(
                    output,
                    lanes(output, child.lanes()),
                    aggregateInPlaceCost(inputStats, child.lanes(), aggregate),
                    0,
                    false,
                    "exact input hash is covered by the grouping keys"
                );
            }

            if (alternative.aggregateMode() == AggMode.DISTRIBUTED
                && groupAttributes == null
                && child.delivered() instanceof SourcePartitioning == false) {
                return new Evaluation(
                    SinglePartitioning.INSTANCE,
                    1,
                    aggregateCoordinatorCost(inputStats, child, aggregate),
                    0,
                    false,
                    "grouping attributes are unavailable for a distributed exchange"
                );
            }

            if (alternative.aggregateMode() == AggMode.COORDINATOR) {
                return new Evaluation(
                    SinglePartitioning.INSTANCE,
                    1,
                    aggregateCoordinatorCost(inputStats, child, aggregate),
                    0,
                    true,
                    alternative.reason()
                );
            }

            Delivered output = aggregateHash(aggregate, groupAttributes);
            return new Evaluation(
                output,
                workerLanes,
                aggregateDistributedCost(inputStats, child, aggregate),
                1,
                true,
                alternative.reason()
            );
        }

        private Cost aggregateInPlaceCost(PlanStats inputStats, long lanes, Aggregate aggregate) {
            double rows = estimateValue(estimateRows(inputStats), CostConstants.UNKNOWN_ROWS);
            GroupEstimate groups = groupEstimate(aggregate, inputStats);
            double work = rows * CostConstants.AGGREGATE_WORK_PER_ROW;
            double states = groups.unknown() ? 0 : groups.value() * CostConstants.AGGREGATE_STATE_BYTES;
            return executionCost(work, lanes, 0, 0, 0, states / Math.max(1, context.workerNodes()), states);
        }

        private Cost aggregateCoordinatorCost(PlanStats inputStats, Winner child, Aggregate aggregate) {
            AggregateEstimate estimate = aggregateEstimate(inputStats, child, aggregate);
            double initialWork = estimate.inputRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double finalWork = estimate.partialRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double intermediateBytes = estimate.partialRows() * estimate.intermediateRowBytes();
            Cost gather = gatherCost(intermediateBytes, child.lanes());
            double stateBytes = estimate.groups().unknown() ? 0 : estimate.groups().value() * CostConstants.AGGREGATE_STATE_BYTES;
            Cost initial = executionCost(initialWork, child.lanes(), 0, 0, 0, 0, 0);
            Cost finalCost = executionCost(finalWork, 1, 0, 0, 0, stateBytes, stateBytes);
            return initial.plus(gather).plus(finalCost);
        }

        private Cost aggregateDistributedCost(PlanStats inputStats, Winner child, Aggregate aggregate) {
            AggregateEstimate estimate = aggregateEstimate(inputStats, child, aggregate);
            double initialWork = estimate.inputRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double finalWork = estimate.partialRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double intermediateBytes = estimate.partialRows() * estimate.intermediateRowBytes();
            Cost exchange = hashCost(intermediateBytes, workerLanes);
            double stateBytes = estimate.groups().unknown() ? 0 : estimate.groups().value() * CostConstants.AGGREGATE_STATE_BYTES;
            Cost initial = executionCost(initialWork, child.lanes(), 0, 0, 0, 0, 0);
            Cost finalCost = executionCost(
                finalWork,
                workerLanes,
                CostConstants.STAGE_STARTUP_SECONDS,
                0,
                0,
                stateBytes / Math.max(1, workerLanes),
                stateBytes / Math.max(1, workerLanes)
            );
            return initial.plus(exchange).plus(finalCost);
        }

        private AggregateEstimate aggregateEstimate(PlanStats inputStats, Winner child, Aggregate aggregate) {
            double inputRows = estimateValue(estimateRows(inputStats), CostConstants.UNKNOWN_ROWS);
            GroupEstimate groups = groupEstimate(aggregate, inputStats);
            double groupCount = groups.unknown() ? Math.max(1, inputRows) : groups.value();
            double reductionLanes = pragmas.nodeLevelReduction() ? context.workerNodes() : Math.max(1, child.lanes());
            double partialRows = Math.min(inputRows, reductionLanes * Math.min(inputRows / reductionLanes, groupCount));
            double intermediateRowBytes = Math.max(
                CostConstants.MIN_INTERMEDIATE_ROW_BYTES,
                estimateValue(estimateAverageRowBytes(inputStats), CostConstants.DEFAULT_ROW_BYTES)
            );
            return new AggregateEstimate(inputRows, partialRows, intermediateRowBytes, groups);
        }

        private Cost materializedJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double rows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double bytes = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            long copies = context.workerNodes();
            double networkBytes = bytes * copies;
            double coordinatorBytes = bytes * copies;
            double work = rows * CostConstants.BUILD_WORK_PER_ROW * copies + probeRows * CostConstants.PROBE_WORK_PER_ROW;
            return executionCost(
                work,
                Math.max(1, workerLanes),
                CostConstants.REPLAN_SECONDS,
                networkBytes,
                coordinatorBytes,
                bytes * CostConstants.HASH_TABLE_OVERHEAD,
                bytes * CostConstants.HASH_TABLE_OVERHEAD
            );
        }

        private Cost broadcastJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double rows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            double work = rows * CostConstants.BUILD_WORK_PER_ROW * context.workerNodes() + probeRows * CostConstants.PROBE_WORK_PER_ROW;
            double memoryPerLane = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES)
                * CostConstants.HASH_TABLE_OVERHEAD;
            double memoryPerNode = memoryPerLane * Math.max(1, pragmas.shufflePartitionsPerNode());
            return executionCost(work, Math.max(1, workerLanes), CostConstants.STAGE_STARTUP_SECONDS, 0, 0, memoryPerNode, memoryPerLane);
        }

        private Cost partitionedJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double buildRows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            int lanes = Math.max(1, workerLanes);
            double work = buildRows * CostConstants.BUILD_WORK_PER_ROW + probeRows * CostConstants.PROBE_WORK_PER_ROW;
            double skewedLaneFraction = Math.max(1.0 / lanes, CostConstants.HOT_PARTITION_FRACTION);
            double laneMemory = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES) * CostConstants.HASH_TABLE_OVERHEAD
                * skewedLaneFraction;
            double nodeMemory = laneMemory * Math.max(1, pragmas.shufflePartitionsPerNode());
            return executionCost(work, lanes, CostConstants.STAGE_STARTUP_SECONDS, 0, 0, nodeMemory, laneMemory);
        }

        private Cost lookupJoinCost(PlanStats leftStats) {
            double rows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            double work = rows * CostConstants.LOOKUP_WORK_PER_ROW;
            return executionCost(work, context.workerNodes(), 0, 0, 0, 0, 0);
        }

        private Cost localJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double leftRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            double rightRows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double work = leftRows * CostConstants.PROBE_WORK_PER_ROW + rightRows * CostConstants.BUILD_WORK_PER_ROW;
            return executionCost(work, 1, 0, 0, 0, 0, 0);
        }

        private Cost executionCost(
            double work,
            long parallelism,
            double startupSeconds,
            double networkBytes,
            double coordinatorBytes,
            double peakNodeBytes,
            double peakLaneBytes
        ) {
            double latency = startupSeconds + safeNonNegative(work) * CostConstants.CPU_SECONDS_PER_WORK_UNIT / Math.max(1, parallelism)
                + safeNonNegative(networkBytes) / CostConstants.NETWORK_BYTES_PER_SECOND + safeNonNegative(coordinatorBytes)
                    / CostConstants.COORDINATOR_BYTES_PER_SECOND;
            return new Cost(
                latency,
                safeNonNegative(work),
                safeNonNegative(networkBytes),
                safeNonNegative(coordinatorBytes),
                safeNonNegative(peakNodeBytes),
                safeNonNegative(peakLaneBytes)
            );
        }

        private Cost gatherCost(PlanStats stats, long producerLanes) {
            return gatherCost(estimateValue(estimateBytes(stats), CostConstants.UNKNOWN_BYTES), producerLanes);
        }

        private Cost gatherCost(double bytes, long producerLanes) {
            double network = safeNonNegative(bytes) * Math.max(0, 1 - 1.0 / Math.max(1, producerLanes));
            double coordinator = safeNonNegative(bytes);
            double work = safeNonNegative(bytes) * CostConstants.SERIALIZATION_WORK_PER_BYTE;
            return executionCost(work, Math.max(1, producerLanes), CostConstants.STAGE_STARTUP_SECONDS, network, coordinator, bytes, bytes);
        }

        private Cost hashCost(double bytes, long consumers) {
            double network = safeNonNegative(bytes) * Math.max(0, 1 - 1.0 / Math.max(1, consumers));
            double work = safeNonNegative(bytes) * CostConstants.SERIALIZATION_WORK_PER_BYTE;
            return executionCost(
                work,
                Math.max(1, consumers),
                CostConstants.STAGE_STARTUP_SECONDS,
                network,
                0,
                bytes / Math.max(1, context.workerNodes()),
                bytes / Math.max(1, consumers)
            );
        }

        private Cost broadcastCost(PlanStats stats, long consumers) {
            double bytes = estimateValue(estimateBytes(stats), CostConstants.UNKNOWN_BYTES);
            double network = bytes * context.workerNodes();
            double work = network * CostConstants.SERIALIZATION_WORK_PER_BYTE;
            return executionCost(
                work,
                Math.max(1, consumers),
                CostConstants.STAGE_STARTUP_SECONDS,
                network,
                0,
                bytes * Math.max(1, pragmas.shufflePartitionsPerNode()),
                bytes
            );
        }

        private Winner enforce(Winner winner, Requirement required) {
            if (satisfies(winner.delivered(), required)) {
                return winner;
            }
            if (required instanceof SingleRequirement) {
                return addEnforcer(
                    winner,
                    Enforcer.GATHER,
                    SinglePartitioning.INSTANCE,
                    1,
                    gatherCost(winner.group().stats, winner.lanes())
                );
            }
            if (required instanceof HashRequirement hash) {
                if (containsIds(ids(winner.group().plan.output()), hash.keys()) == false) {
                    return addEnforcer(
                        winner,
                        Enforcer.GATHER,
                        SinglePartitioning.INSTANCE,
                        1,
                        gatherCost(winner.group().stats, winner.lanes())
                    );
                }
                return addEnforcer(
                    winner,
                    Enforcer.HASH,
                    new HashPartitioning(hash.keys(), false),
                    workerLanes,
                    hashCost(estimateValue(estimateBytes(winner.group().stats), CostConstants.UNKNOWN_BYTES), workerLanes)
                );
            }
            if (required instanceof BroadcastRequirement) {
                return addEnforcer(
                    winner,
                    Enforcer.BROADCAST,
                    ArbitraryPartitioning.INSTANCE,
                    workerLanes,
                    broadcastCost(winner.group().stats, workerLanes)
                );
            }
            throw new IllegalStateException("ANY requirement cannot need an enforcer");
        }

        private Winner addEnforcer(Winner winner, Enforcer enforcer, Delivered delivered, long lanes, Cost cost) {
            int shuffleEdges = winner.shuffleEdges() + (enforcer == Enforcer.GATHER ? 0 : 1);
            return new Winner(
                winner.group(),
                winner.alternative(),
                winner.children(),
                winner.nativeDelivered(),
                delivered,
                lanes,
                winner.cost().plus(cost),
                shuffleEdges,
                winner.decisionApplicable(),
                winner.reason()
            );
        }

        private String fitRisk(Winner winner) {
            Alternative alternative = winner.alternative();
            if (alternative.forced() || winner.decisionApplicable() == false) {
                return null;
            }
            if (alternative.kind() == OperatorKind.JOIN) {
                if (alternative.joinStrategy() == JoinStrategySelector.JoinStrategy.MATERIALIZED) {
                    Estimate bytes = estimateBytes(winner.group().children.get(1).stats);
                    if (bytes.probabilityAtMost(context.materializeLimitBytes()) < CostConstants.MIN_FIT_PROBABILITY) {
                        return "materialized build side does not meet the fit threshold";
                    }
                }
                if (alternative.joinStrategy() == JoinStrategySelector.JoinStrategy.BROADCAST) {
                    Estimate bytes = estimateBytes(winner.group().children.get(1).stats);
                    double laneBudget = CostConstants.BROADCAST_MEMORY_BYTES / Math.max(1, pragmas.shufflePartitionsPerNode());
                    double buildBudget = laneBudget / CostConstants.HASH_TABLE_OVERHEAD;
                    if (bytes.probabilityAtMost(buildBudget) < CostConstants.MIN_FIT_PROBABILITY) {
                        return "broadcast build side does not meet the memory fit threshold";
                    }
                }
            }
            if (alternative.kind() == OperatorKind.AGGREGATE && alternative.aggregateMode() == AggMode.COORDINATOR) {
                Aggregate aggregate = (Aggregate) winner.group().plan;
                PlanStats input = winner.group().children.get(0).stats;
                GroupEstimate groups = groupEstimate(aggregate, input);
                if (groups.unknown() == false
                    && groups.upperBound() * CostConstants.AGGREGATE_STATE_BYTES > CostConstants.COORDINATOR_MEMORY_BYTES) {
                    return "coordinator aggregate state exceeds the memory fit threshold";
                }
            }
            return null;
        }

        private List<Winner> safeFallback(Group group, List<Winner> rejected) {
            List<Winner> safe = new ArrayList<>();
            if (group.plan instanceof Join join) {
                JoinStrategySelector.JoinStrategy fallback = Mapper.isIndexModeLookup(join.right())
                    ? JoinStrategySelector.JoinStrategy.PARTITIONED
                    : join instanceof SubqueryLookupJoin ? JoinStrategySelector.JoinStrategy.PARTITIONED
                    : null;
                if (fallback != null) {
                    addMatching(safe, rejected, fallback);
                }
                if (safe.isEmpty()) {
                    JoinStrategySelector.JoinStrategy incumbent = incumbent(join);
                    addMatching(safe, rejected, incumbent);
                }
            } else if (group.plan instanceof Aggregate) {
                addMatching(safe, rejected, AggMode.DISTRIBUTED);
                if (safe.isEmpty()) {
                    addMatching(safe, rejected, AggMode.COORDINATOR);
                }
            }
            if (safe.isEmpty()) {
                safe.addAll(rejected);
            }
            return safe;
        }

        private void addMatching(List<Winner> target, List<Winner> candidates, JoinStrategySelector.JoinStrategy strategy) {
            for (Winner candidate : candidates) {
                if (candidate.alternative().joinStrategy() == strategy) {
                    target.add(candidate);
                }
            }
        }

        private void addMatching(List<Winner> target, List<Winner> candidates, AggMode mode) {
            for (Winner candidate : candidates) {
                if (candidate.alternative().aggregateMode() == mode) {
                    target.add(candidate);
                }
            }
        }

        private List<Winner> cheapestPerDelivered(List<Winner> candidates) {
            Map<Delivered, Winner> winners = new LinkedHashMap<>();
            for (Winner candidate : candidates) {
                Winner current = winners.get(candidate.delivered());
                if (current == null || cheaper(candidate, current)) {
                    winners.put(candidate.delivered(), candidate);
                }
            }
            return new ArrayList<>(winners.values());
        }

        private Winner cheapest(List<Winner> candidates) {
            Winner best = null;
            for (Winner candidate : candidates) {
                if (best == null || cheaper(candidate, best)) {
                    best = candidate;
                }
            }
            return best;
        }

        private boolean cheaper(Winner left, Winner right) {
            double leftCost = left.cost().objective(CostConstants.WORK_WEIGHT);
            double rightCost = right.cost().objective(CostConstants.WORK_WEIGHT);
            if (Double.isFinite(leftCost) == false || Double.isFinite(rightCost) == false) {
                if (leftCost != rightCost) {
                    return leftCost < rightCost;
                }
            } else {
                double relativeDifference = Math.abs(leftCost - rightCost) / Math.max(
                    Math.max(Math.abs(leftCost), Math.abs(rightCost)),
                    1e-12
                );
                if (relativeDifference > CostConstants.TIE_EPSILON) {
                    return leftCost < rightCost;
                }
            }
            boolean leftIncumbent = incumbent(left);
            boolean rightIncumbent = incumbent(right);
            if (leftIncumbent != rightIncumbent) {
                return leftIncumbent;
            }
            if (left.cost().peakLaneBytes() != right.cost().peakLaneBytes()) {
                return left.cost().peakLaneBytes() < right.cost().peakLaneBytes();
            }
            if (left.cost().peakNodeBytes() != right.cost().peakNodeBytes()) {
                return left.cost().peakNodeBytes() < right.cost().peakNodeBytes();
            }
            return left.shuffleEdges() < right.shuffleEdges();
        }

        private JoinStrategySelector.JoinStrategy incumbent(Join join) {
            if (join instanceof SubqueryLookupJoin) {
                return JoinStrategySelector.JoinStrategy.MATERIALIZED;
            }
            if (Mapper.isIndexModeLookup(join.right())) {
                return JoinStrategySelector.JoinStrategy.LOOKUP;
            }
            return JoinStrategySelector.JoinStrategy.LOCAL;
        }

        private boolean incumbent(Winner winner) {
            Alternative alternative = winner.alternative();
            if (alternative.kind() == OperatorKind.JOIN) {
                return alternative.joinStrategy() == incumbent((Join) winner.group().plan);
            }
            return alternative.kind() == OperatorKind.AGGREGATE && alternative.aggregateMode() == AggMode.COORDINATOR;
        }

        private Winner withReason(Winner winner, String reason) {
            return new Winner(
                winner.group(),
                winner.alternative(),
                winner.children(),
                winner.nativeDelivered(),
                winner.delivered(),
                winner.lanes(),
                winner.cost(),
                winner.shuffleEdges(),
                winner.decisionApplicable(),
                reason
            );
        }

        private PlanDecisions decisions(Winner rootWinner) {
            Map<JoinKey, JoinStrategySelector.JoinStrategy> joins = new HashMap<>();
            Map<AggKey, AggMode> aggregates = new HashMap<>();
            Set<JoinKey> conflictingJoins = new LinkedHashSet<>();
            Set<AggKey> conflictingAggregates = new LinkedHashSet<>();
            List<DecisionTrace> traces = new ArrayList<>();
            Set<Winner> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            collectDecisions(rootWinner, visited, joins, aggregates, conflictingJoins, conflictingAggregates, traces);
            return joins.isEmpty() && aggregates.isEmpty() && traces.isEmpty()
                ? PlanDecisions.EMPTY
                : new PlanDecisions(joins, aggregates, traces);
        }

        private void collectDecisions(
            Winner winner,
            Set<Winner> visited,
            Map<JoinKey, JoinStrategySelector.JoinStrategy> joins,
            Map<AggKey, AggMode> aggregates,
            Set<JoinKey> conflictingJoins,
            Set<AggKey> conflictingAggregates,
            List<DecisionTrace> traces
        ) {
            if (visited.add(winner) == false) {
                return;
            }
            Alternative alternative = winner.alternative();
            if (winner.decisionApplicable() && alternative.kind() == OperatorKind.JOIN) {
                Join join = (Join) winner.group().plan;
                JoinKey key = JoinKey.of(join);
                if (conflictingJoins.contains(key) == false) {
                    JoinStrategySelector.JoinStrategy previous = joins.putIfAbsent(key, alternative.joinStrategy());
                    if (previous != null && previous != alternative.joinStrategy()) {
                        joins.remove(key);
                        conflictingJoins.add(key);
                        traces.add(
                            new DecisionTrace(
                                join.getClass().getSimpleName(),
                                "omitted",
                                List.of(),
                                "conflicting decisions share the same join key"
                            )
                        );
                    } else {
                        traces.add(
                            new DecisionTrace(
                                join.getClass().getSimpleName(),
                                alternative.joinStrategy().name(),
                                alternative.alternatives(),
                                winner.reason()
                            )
                        );
                    }
                }
            }
            if (winner.decisionApplicable() && alternative.kind() == OperatorKind.AGGREGATE) {
                Aggregate aggregate = (Aggregate) winner.group().plan;
                AggKey key = AggKey.of(aggregate.aggregates());
                if (conflictingAggregates.contains(key) == false) {
                    AggMode previous = aggregates.putIfAbsent(key, alternative.aggregateMode());
                    if (previous != null && previous != alternative.aggregateMode()) {
                        aggregates.remove(key);
                        conflictingAggregates.add(key);
                        traces.add(
                            new DecisionTrace(
                                aggregate.getClass().getSimpleName(),
                                "omitted",
                                List.of(),
                                "conflicting decisions share the same aggregate key"
                            )
                        );
                    } else {
                        traces.add(
                            new DecisionTrace(
                                aggregate.getClass().getSimpleName(),
                                alternative.aggregateMode().name(),
                                alternative.alternatives(),
                                winner.reason()
                            )
                        );
                    }
                }
            }
            for (Winner child : winner.children()) {
                collectDecisions(child, visited, joins, aggregates, conflictingJoins, conflictingAggregates, traces);
            }
        }

        private GroupEstimate groupEstimate(Aggregate aggregate, PlanStats inputStats) {
            Estimate rowsEstimate = estimateRows(inputStats);
            if (rowsEstimate.isUnknown()) {
                return GroupEstimate.UNKNOWN;
            }
            double rows = estimateValue(rowsEstimate, CostConstants.UNKNOWN_ROWS);
            double rowsUpper = estimateUpperBound(rowsEstimate, CostConstants.UNKNOWN_ROWS);
            double value = 1;
            double upperBound = 1;
            Set<NameId> grouped = new LinkedHashSet<>();
            for (Expression grouping : aggregate.groupings()) {
                Attribute attribute = Expressions.attribute(grouping);
                if (attribute == null) {
                    return GroupEstimate.UNKNOWN;
                }
                if (grouped.add(attribute.id()) == false) {
                    continue;
                }
                ColumnStats column = inputStats == null ? null : inputStats.column(attribute);
                Estimate ndv = column == null ? Estimate.UNKNOWN : column.ndv();
                if (ndv.isUnknown()) {
                    return GroupEstimate.UNKNOWN;
                }
                value = safeMultiply(value, Math.max(1, estimateValue(ndv, 1)));
                upperBound = safeMultiply(upperBound, Math.max(1, estimateUpperBound(ndv, 1)));
            }
            return new GroupEstimate(Math.min(rows, value), Math.min(rowsUpper, upperBound), false);
        }

        private List<Attribute> groupAttributes(Aggregate aggregate) {
            List<Attribute> attributes = new ArrayList<>(aggregate.groupings().size());
            for (Expression grouping : aggregate.groupings()) {
                Attribute attribute = Expressions.attribute(grouping);
                if (attribute == null) {
                    return null;
                }
                attributes.add(attribute);
            }
            return attributes;
        }

        private Delivered aggregateHash(Aggregate aggregate, List<Attribute> groupAttributes) {
            if (groupAttributes == null || groupAttributes.isEmpty()) {
                return ArbitraryPartitioning.INSTANCE;
            }
            List<NameId> outputKeys = new ArrayList<>(groupAttributes.size());
            for (Attribute group : groupAttributes) {
                Attribute output = findById(aggregate.output(), group.id());
                if (output == null) {
                    return ArbitraryPartitioning.INSTANCE;
                }
                outputKeys.add(output.id());
            }
            return new HashPartitioning(outputKeys, true);
        }

        private static Delivered translateUp(Delivered delivered, UnaryPlan unary) {
            if ((delivered instanceof HashPartitioning) == false) {
                return delivered;
            }
            HashPartitioning hash = (HashPartitioning) delivered;
            List<NameId> translated = new ArrayList<>(hash.keys().size());
            for (NameId key : hash.keys()) {
                NameId outputId = null;
                if (unary instanceof Project project) {
                    for (NamedExpression projection : project.projections()) {
                        Attribute output = projection.toAttribute();
                        if (output.id().equals(key)) {
                            outputId = output.id();
                            break;
                        }
                        if (projection instanceof Alias alias
                            && alias.child() instanceof Attribute attribute
                            && attribute.id().equals(key)) {
                            outputId = output.id();
                            break;
                        }
                    }
                } else {
                    Attribute output = findById(unary.output(), key);
                    if (output != null) {
                        outputId = output.id();
                    }
                }
                if (outputId == null) {
                    return ArbitraryPartitioning.INSTANCE;
                }
                translated.add(outputId);
            }
            return new HashPartitioning(translated, hash.exact());
        }

        private static boolean streamingOutsideFragment(UnaryPlan unary) {
            return unary instanceof Filter
                || unary instanceof Project
                || unary instanceof Eval
                || unary instanceof MvExpand
                || unary instanceof RegexExtract
                || unary instanceof CompoundOutputEval
                || unary instanceof Sample
                || unary instanceof Enrich enrich && enrich.mode() != Enrich.Mode.COORDINATOR;
        }

        private static Delivered keepIfPresent(Delivered delivered, List<Attribute> output) {
            if (delivered instanceof HashPartitioning hash && containsIds(ids(output), hash.keys()) == false) {
                return ArbitraryPartitioning.INSTANCE;
            }
            return delivered;
        }

        private static boolean satisfies(Delivered delivered, Requirement required) {
            if (required instanceof AnyRequirement) {
                return true;
            }
            if (required instanceof SingleRequirement) {
                return delivered instanceof SinglePartitioning;
            }
            if (required instanceof BroadcastRequirement) {
                return false;
            }
            HashRequirement hash = (HashRequirement) required;
            return delivered instanceof HashPartitioning partitioning && sameIds(partitioning.keys(), hash.keys());
        }

        private static boolean containsIds(List<NameId> attributes, List<NameId> required) {
            for (NameId key : required) {
                if (attributes.contains(key) == false) {
                    return false;
                }
            }
            return true;
        }

        private static Attribute findById(List<Attribute> attributes, NameId id) {
            for (Attribute attribute : attributes) {
                if (attribute.id().equals(id)) {
                    return attribute;
                }
            }
            return null;
        }

        private static List<NameId> ids(List<Attribute> attributes) {
            return attributes.stream().map(Attribute::id).toList();
        }

        private static boolean sameIds(List<NameId> left, List<NameId> right) {
            return left.equals(right);
        }

        private static long lanes(Delivered delivered, long fallback) {
            if (delivered instanceof SourcePartitioning) {
                return fallback;
            }
            if (delivered instanceof SinglePartitioning) {
                return 1;
            }
            return fallback;
        }

        private static Estimate estimateRows(PlanStats stats) {
            return stats == null ? Estimate.UNKNOWN : stats.rows();
        }

        private static Estimate estimateBytes(PlanStats stats) {
            return stats == null ? Estimate.UNKNOWN : stats.bytes();
        }

        private static Estimate estimateAverageRowBytes(PlanStats stats) {
            return stats == null ? Estimate.UNKNOWN : stats.avgRowBytes();
        }

        private static double estimateValue(Estimate estimate, double fallback) {
            if (estimate == null) {
                return fallback;
            }
            return safeNonNegative(estimate.valueOr(fallback));
        }

        private static double estimateUpperBound(Estimate estimate, double fallback) {
            if (estimate == null) {
                return fallback;
            }
            double upperBound = estimate.upperBound();
            return Double.isNaN(upperBound) ? fallback : safeNonNegative(upperBound);
        }

        private static double safeMultiply(double left, double right) {
            if (left == 0 || right == 0) {
                return 0;
            }
            double result = left * right;
            return Double.isFinite(result) ? result : Double.MAX_VALUE;
        }

        private static double safeNonNegative(double value) {
            if (Double.isNaN(value) || value < 0) {
                return 0;
            }
            return value;
        }

        private static int positiveProduct(int left, int right) {
            long result = (long) Math.max(1, left) * Math.max(1, right);
            return (int) Math.min(Integer.MAX_VALUE, result);
        }
    }

    private enum OperatorKind {
        SOURCE,
        SINGLE,
        STREAMING_UNARY,
        OPAQUE,
        JOIN,
        AGGREGATE
    }

    private enum Enforcer {
        GATHER,
        HASH,
        BROADCAST
    }

    private sealed interface Requirement permits AnyRequirement, SingleRequirement, HashRequirement, BroadcastRequirement {}

    private enum AnyRequirement implements Requirement {
        INSTANCE
    }

    private enum SingleRequirement implements Requirement {
        INSTANCE
    }

    private enum BroadcastRequirement implements Requirement {
        INSTANCE
    }

    private record HashRequirement(List<NameId> keys) implements Requirement {
        private HashRequirement {
            keys = List.copyOf(keys);
        }
    }

    private sealed interface Delivered permits SourcePartitioning, SinglePartitioning, HashPartitioning, ArbitraryPartitioning {}

    private enum SourcePartitioning implements Delivered {
        INSTANCE
    }

    private enum SinglePartitioning implements Delivered {
        INSTANCE
    }

    private enum ArbitraryPartitioning implements Delivered {
        INSTANCE
    }

    private record HashPartitioning(List<NameId> keys, boolean exact) implements Delivered {
        private HashPartitioning {
            keys = List.copyOf(keys);
        }
    }

    private static final class Group {
        private final LogicalPlan plan;
        private final List<Group> children;
        private final PlanStats stats;
        private final Map<Requirement, List<Winner>> winners = new HashMap<>();
        private List<Winner> nativeCandidates;
        private boolean building;

        private Group(LogicalPlan plan, List<Group> children, PlanStats stats) {
            this.plan = plan;
            this.children = List.copyOf(children);
            this.stats = stats;
        }
    }

    private record Alternative(
        OperatorKind kind,
        String label,
        List<Requirement> childRequirements,
        JoinStrategySelector.JoinStrategy joinStrategy,
        AggMode aggregateMode,
        boolean forced,
        boolean lookupFallback,
        List<String> alternatives,
        String reason
    ) {
        private Alternative {
            childRequirements = List.copyOf(childRequirements);
            alternatives = List.copyOf(alternatives);
        }

    }

    private record Evaluation(
        Delivered delivered,
        long lanes,
        Cost localCost,
        int shuffleEdges,
        boolean decisionApplicable,
        String reason
    ) {}

    private record Winner(
        Group group,
        Alternative alternative,
        List<Winner> children,
        Delivered nativeDelivered,
        Delivered delivered,
        long lanes,
        Cost cost,
        int shuffleEdges,
        boolean decisionApplicable,
        String reason
    ) {
        private Winner {
            children = List.copyOf(children);
        }
    }

    private record AggregateEstimate(double inputRows, double partialRows, double intermediateRowBytes, GroupEstimate groups) {}

    private record GroupEstimate(double value, double upperBound, boolean unknown) {
        private static final GroupEstimate UNKNOWN = new GroupEstimate(0, 0, true);
    }

    private record Cost(
        double latency,
        double work,
        double networkBytes,
        double coordinatorBytes,
        double peakNodeBytes,
        double peakLaneBytes
    ) {
        private static final Cost ZERO = new Cost(0, 0, 0, 0, 0, 0);

        private Cost plus(Cost other) {
            return new Cost(
                latency + other.latency,
                work + other.work,
                networkBytes + other.networkBytes,
                coordinatorBytes + other.coordinatorBytes,
                Math.max(peakNodeBytes, other.peakNodeBytes),
                Math.max(peakLaneBytes, other.peakLaneBytes)
            );
        }

        private double objective(double workWeight) {
            return latency + workWeight * work;
        }
    }

    private static final class CostConstants {
        private static final int MAX_CANDIDATES = 10_000;
        private static final double TIE_EPSILON = 0.05;
        private static final double MIN_FIT_PROBABILITY = 0.99;
        private static final double WORK_WEIGHT = 1e-9;
        private static final double CPU_SECONDS_PER_WORK_UNIT = 1e-6;
        private static final double BUILD_WORK_PER_ROW = 2;
        private static final double PROBE_WORK_PER_ROW = 1;
        private static final double LOOKUP_WORK_PER_ROW = 4;
        private static final double AGGREGATE_WORK_PER_ROW = 3;
        private static final double SERIALIZATION_WORK_PER_BYTE = 0.01;
        private static final double NETWORK_BYTES_PER_SECOND = 256d * 1024 * 1024;
        private static final double COORDINATOR_BYTES_PER_SECOND = 512d * 1024 * 1024;
        private static final double STAGE_STARTUP_SECONDS = 0.01;
        private static final double REPLAN_SECONDS = 0.005;
        private static final double HASH_TABLE_OVERHEAD = 2;
        private static final double HOT_PARTITION_FRACTION = 0.1;
        private static final double BROADCAST_MEMORY_BYTES = 128d * 1024 * 1024;
        private static final double COORDINATOR_MEMORY_BYTES = 512d * 1024 * 1024;
        private static final double AGGREGATE_STATE_BYTES = 64;
        private static final double UNKNOWN_ROWS = 100_000;
        private static final double UNKNOWN_BYTES = 64d * 1024 * 1024;
        private static final double DEFAULT_ROW_BYTES = 32;
        private static final double MIN_INTERMEDIATE_ROW_BYTES = 16;
    }

    private static final class SearchAborted extends RuntimeException {
        private SearchAborted() {
            super(null, null, false, false);
        }
    }
}
