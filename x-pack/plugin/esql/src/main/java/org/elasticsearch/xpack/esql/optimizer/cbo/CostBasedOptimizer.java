/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostTerm;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostVector;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionMode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.EstimateView;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Exclusion;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.ExclusionCode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.FitCheck;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.FitMethod;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Side;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.ColumnStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;
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
    /** Decision kinds the search reports on, in lookup order. */
    static final List<DecisionKind> KINDS = List.of(JoinStrategyDecisionKind.INSTANCE, AggregateDistributionDecisionKind.INSTANCE);

    /**
     * Searches the logical tree for eligible physical choices while keeping optimizer decisions out of plan nodes.
     */
    public PlanDecisions optimize(LogicalPlan plan, CboContext context) {
        return optimize(plan, context, ExplainLevel.NONE, RunInfo.INITIAL);
    }

    /**
     * Searches like {@link #optimize(LogicalPlan, CboContext)} and, unless {@code level} is {@link ExplainLevel#NONE}, attaches a
     * report that explains each decision. The decisions themselves never depend on {@code level}.
     */
    public PlanDecisions optimize(LogicalPlan plan, CboContext context, ExplainLevel level, RunInfo run) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(context);
        if (context.configuration().pragmas().cbo() == false) {
            return PlanDecisions.EMPTY;
        }
        String ineligible = ineligibleReason(plan, context);
        if (ineligible != null) {
            return ExplainBuilder.ineligible(level, run, ineligible);
        }
        long start = System.nanoTime();
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>(new StatsDeriver().derive(plan));
        return search(plan, context, statistics, level, run, start);
    }

    /** Runs the same search against supplied estimates so distribution choices can be tested independently. */
    PlanDecisions optimizeWithStatistics(LogicalPlan plan, CboContext context, Map<LogicalPlan, PlanStats> statistics) {
        return optimizeWithStatistics(plan, context, statistics, ExplainLevel.NONE, RunInfo.INITIAL);
    }

    /** Runs the same search against supplied estimates, with a report at the requested level. */
    PlanDecisions optimizeWithStatistics(
        LogicalPlan plan,
        CboContext context,
        Map<LogicalPlan, PlanStats> statistics,
        ExplainLevel level,
        RunInfo run
    ) {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(context);
        Objects.requireNonNull(statistics);
        if (context.configuration().pragmas().cbo() == false) {
            return PlanDecisions.EMPTY;
        }
        String ineligible = ineligibleReason(plan, context);
        if (ineligible != null) {
            return ExplainBuilder.ineligible(level, run, ineligible);
        }
        return search(plan, context, new IdentityHashMap<>(statistics), level, run, System.nanoTime());
    }

    private static PlanDecisions search(
        LogicalPlan plan,
        CboContext context,
        Map<LogicalPlan, PlanStats> statistics,
        ExplainLevel level,
        RunInfo run,
        long startNanos
    ) {
        Search search = new Search(plan, context, statistics, null, null);
        Winner best;
        try {
            best = search.solve();
        } catch (SearchAborted e) {
            return ExplainBuilder.aborted(
                level,
                run,
                search,
                "candidate budget [" + CostConstants.MAX_CANDIDATES + "] exceeded; rule-based planning applies",
                startNanos
            );
        }
        if (best == null) {
            return ExplainBuilder.aborted(level, run, search, "no complete plan was found; rule-based planning applies", startNanos);
        }
        Decided decided = search.decisions(best);
        if (level == ExplainLevel.NONE) {
            return decided.joins().isEmpty() && decided.aggregates().isEmpty()
                ? PlanDecisions.EMPTY
                : new PlanDecisions(decided.joins(), decided.aggregates(), null, List.of());
        }
        return new ExplainBuilder(search, best, decided, level, run, startNanos).build();
    }

    /** Returns why the search cannot run for this plan, or {@code null} when it can. */
    private static String ineligibleReason(LogicalPlan plan, CboContext context) {
        Configuration configuration = context.configuration();
        if (configuration.pragmas().shuffleEnabled() == false) {
            return "shuffle execution is disabled";
        }
        if (ShuffleGate.allowed(configuration, plan) == false) {
            return "the shuffle gate rejects this query";
        }
        if (context.minimumVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION) == false) {
            return "the minimum transport version does not support shuffle execution";
        }
        return null;
    }

    /** Returns the decision kind that applies to {@code plan}, or {@code null} if the node is not a decision point. */
    static DecisionKind kindOf(LogicalPlan plan) {
        for (DecisionKind kind : KINDS) {
            if (kind.appliesTo(plan)) {
                return kind;
            }
        }
        return null;
    }

    /** The physical choices of one solved plan, plus keys that two decision points chose differently. */
    record Decided(
        Map<JoinKey, JoinStrategySelector.JoinStrategy> joins,
        Map<AggKey, AggMode> aggregates,
        Set<JoinKey> conflictingJoins,
        Set<AggKey> conflictingAggregates
    ) {}

    static final class Search {
        private static final String BUILD_BYTES_UNKNOWN = "build-side bytes are unknown";
        private static final String SHUFFLE_UNAVAILABLE = "shuffle execution is unavailable for this query";

        private final CboContext context;
        private final Configuration configuration;
        private final QueryPragmas pragmas;
        private final Map<LogicalPlan, PlanStats> statistics;
        private final IdentityHashMap<LogicalPlan, Group> memo = new IdentityHashMap<>();
        private final int workerLanes;
        private final boolean shuffleAllowed;
        private final LogicalPlan plan;
        private final Group root;
        /** Logical node whose alternatives are restricted to {@link #constrainedLabel}; it also skips fit pruning. */
        private final LogicalPlan constrainedPlan;
        private final String constrainedLabel;
        private int evaluatedCandidates;

        private Search(
            LogicalPlan plan,
            CboContext context,
            Map<LogicalPlan, PlanStats> statistics,
            LogicalPlan constrainedPlan,
            String constrainedLabel
        ) {
            this.context = context;
            this.configuration = context.configuration();
            this.pragmas = configuration.pragmas();
            this.statistics = statistics;
            this.plan = plan;
            this.constrainedPlan = constrainedPlan;
            this.constrainedLabel = constrainedLabel;
            this.workerLanes = positiveProduct(context.workerNodes(), Math.max(1, pragmas.shufflePartitionsPerNode()));
            this.root = build(plan);
            this.shuffleAllowed = pragmas.shuffleEnabled()
                && ShuffleGate.allowed(configuration, plan)
                && context.minimumVersion().supports(ExchangeDistribution.ESQL_SHUFFLE_STAGE_EXECUTION);
        }

        /**
         * Creates a search over the same plan and statistics in which {@code group} may only use the alternative labeled
         * {@code label}. Used to cost counterfactual plans.
         */
        Search constrained(Group group, String label) {
            return new Search(plan, context, statistics, group.plan, label);
        }

        /** Returns the cheapest complete plan, or {@code null} if no plan satisfies the root requirement. */
        Winner solve() {
            return cheapest(optimize(root, SingleRequirement.INSTANCE));
        }

        CboContext context() {
            return context;
        }

        int groupCount() {
            return memo.size();
        }

        int evaluatedCandidates() {
            return evaluatedCandidates;
        }

        PlanStats stats(LogicalPlan node) {
            return statistics.get(node);
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
                if (prunedByFit(enforced)) {
                    rejectedForRisk.add(enforced);
                } else {
                    feasible.add(enforced);
                }
            }

            if (feasible.isEmpty() && rejectedForRisk.isEmpty() == false) {
                for (Winner winner : safeFallback(group, rejectedForRisk)) {
                    feasible.add(winner.asFitFallback());
                }
            }

            List<Winner> winners = cheapestPerProperties(feasible);
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
                for (Alternative alternative : enumeration(group).alternatives()) {
                    if (group.plan == constrainedPlan && alternative.label().equals(constrainedLabel) == false) {
                        continue;
                    }
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
                Cost cost = childCost(group, alternative, chosenChildren).plus(evaluation.local().total());
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
                        evaluation.scanFragment(),
                        evaluation.delivered(),
                        evaluation.lanes(),
                        evaluation.local(),
                        null,
                        Cost.ZERO,
                        cost,
                        shuffleEdges,
                        evaluation.decisionApplicable(),
                        false
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

        /** Returns the alternatives this group may use, with the mode and exclusions that explain the list. */
        Enumeration enumeration(Group group) {
            if (group.enumeration == null) {
                group.enumeration = enumerate(group);
            }
            return group.enumeration;
        }

        private Enumeration enumerate(Group group) {
            LogicalPlan plan = group.plan;
            if (plan instanceof EsRelation || plan instanceof ExternalRelation) {
                return Enumeration.only(alternative(OperatorKind.SOURCE, List.of(), "SOURCE"));
            }
            if (plan instanceof LocalRelation) {
                return Enumeration.only(alternative(OperatorKind.SINGLE, List.of(), "SINGLE"));
            }
            if (plan instanceof Join join) {
                return joinAlternatives(group, join);
            }
            if (plan instanceof Aggregate aggregate) {
                if (supportedAggregate(aggregate)) {
                    return aggregateAlternatives(group, aggregate);
                }
                return Enumeration.only(opaque(group));
            }
            if (plan.children().isEmpty()) {
                return Enumeration.only(alternative(OperatorKind.SINGLE, List.of(), "SINGLE"));
            }
            if (plan instanceof UnaryPlan unary) {
                if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                    return Enumeration.only(opaque(group));
                }
                if (unary instanceof Limit || unary instanceof TopN) {
                    return Enumeration.only(alternative(OperatorKind.STREAMING_UNARY, anyRequirements(group), "UNARY"));
                }
                if (unary instanceof PipelineBreaker) {
                    return Enumeration.only(opaque(group));
                }
                return Enumeration.only(alternative(OperatorKind.STREAMING_UNARY, anyRequirements(group), "UNARY"));
            }
            return Enumeration.only(opaque(group));
        }

        private Enumeration joinAlternatives(Group group, Join join) {
            if (join instanceof SubqueryLookupJoin) {
                return subqueryJoinAlternatives(group, join);
            }
            if (Mapper.isIndexModeLookup(join.right())) {
                return lookupJoinAlternatives(group, join);
            }
            return Enumeration.only(joinAlternative(join, JoinStrategySelector.JoinStrategy.LOCAL, false));
        }

        private Enumeration subqueryJoinAlternatives(Group group, Join join) {
            JoinStrategySelector.JoinStrategy materialized = JoinStrategySelector.JoinStrategy.MATERIALIZED;
            JoinStrategySelector.JoinStrategy broadcast = JoinStrategySelector.JoinStrategy.BROADCAST;
            JoinStrategySelector.JoinStrategy partitioned = JoinStrategySelector.JoinStrategy.PARTITIONED;
            if (pragmas.shuffleJoinExplicit() && pragmas.shuffleJoin() == false) {
                String pragma = "shuffle_join=false";
                return new Enumeration(
                    List.of(joinAlternative(join, materialized, true)),
                    List.of(
                        excluded(broadcast, ExclusionCode.PRAGMA_DISABLED, pragma),
                        excluded(partitioned, ExclusionCode.PRAGMA_DISABLED, pragma)
                    ),
                    DecisionMode.FORCED,
                    pragma
                );
            }

            boolean eligible = shuffleJoinEligible(join);
            boolean buildKnown = estimateBytes(group.children.get(1).stats).isUnknown() == false;
            if (pragmas.shuffleJoinStrategyExplicit()) {
                JoinStrategySelector.JoinStrategy requested = requestedShuffleStrategy();
                if (eligible && requested != null) {
                    String pragma = "shuffle_join_strategy=" + pragmas.shuffleJoinStrategy();
                    List<Exclusion> excluded = new ArrayList<>();
                    for (JoinStrategySelector.JoinStrategy other : List.of(materialized, broadcast, partitioned)) {
                        if (other != requested) {
                            excluded.add(excluded(other, ExclusionCode.PRAGMA_FORCED, pragma));
                        }
                    }
                    return new Enumeration(List.of(joinAlternative(join, requested, true)), excluded, DecisionMode.FORCED, pragma);
                }
                return shuffleJoinUnavailable(join, materialized, List.of(broadcast, partitioned));
            }

            if (eligible == false) {
                return shuffleJoinUnavailable(join, materialized, List.of(broadcast, partitioned));
            }
            if (pragmas.shuffleJoinExplicit()) {
                String pragma = "shuffle_join=true";
                if (buildKnown == false) {
                    return new Enumeration(
                        List.of(joinAlternative(join, partitioned, false)),
                        List.of(
                            excluded(materialized, ExclusionCode.PRAGMA_FORCED, pragma),
                            excluded(broadcast, ExclusionCode.UNKNOWN_STATISTICS, BUILD_BYTES_UNKNOWN)
                        ),
                        DecisionMode.GATED,
                        BUILD_BYTES_UNKNOWN
                    );
                }
                return new Enumeration(
                    List.of(joinAlternative(join, broadcast, false), joinAlternative(join, partitioned, false)),
                    List.of(excluded(materialized, ExclusionCode.PRAGMA_FORCED, pragma)),
                    DecisionMode.COST_BASED,
                    null
                );
            }
            if (buildKnown == false) {
                return new Enumeration(
                    List.of(joinAlternative(join, materialized, false)),
                    List.of(
                        excluded(broadcast, ExclusionCode.UNKNOWN_STATISTICS, BUILD_BYTES_UNKNOWN),
                        excluded(partitioned, ExclusionCode.UNKNOWN_STATISTICS, BUILD_BYTES_UNKNOWN)
                    ),
                    DecisionMode.GATED,
                    BUILD_BYTES_UNKNOWN
                );
            }
            return new Enumeration(
                List.of(
                    joinAlternative(join, materialized, false),
                    joinAlternative(join, broadcast, false),
                    joinAlternative(join, partitioned, false)
                ),
                List.of(),
                DecisionMode.COST_BASED,
                null
            );
        }

        private Enumeration lookupJoinAlternatives(Group group, Join join) {
            JoinStrategySelector.JoinStrategy lookup = JoinStrategySelector.JoinStrategy.LOOKUP;
            JoinStrategySelector.JoinStrategy partitioned = JoinStrategySelector.JoinStrategy.PARTITIONED;
            if (pragmas.shuffleJoinExplicit() && pragmas.shuffleJoin() == false) {
                String pragma = "shuffle_join=false";
                return new Enumeration(
                    List.of(joinAlternative(join, lookup, true)),
                    List.of(excluded(partitioned, ExclusionCode.PRAGMA_DISABLED, pragma)),
                    DecisionMode.FORCED,
                    pragma
                );
            }

            boolean eligible = shuffleJoinEligible(join);
            if (pragmas.shuffleJoinExplicit()) {
                if (eligible) {
                    return new Enumeration(
                        List.of(joinAlternative(join, partitioned, true), lookupFallbackAlternative()),
                        List.of(),
                        DecisionMode.FORCED,
                        "shuffle_join=true"
                    );
                }
                return shuffleJoinUnavailable(join, lookup, List.of(partitioned));
            }
            if (pragmas.shuffleJoinStrategyExplicit()) {
                JoinStrategySelector.JoinStrategy requested = requestedShuffleStrategy();
                if (eligible && requested == partitioned) {
                    return new Enumeration(
                        List.of(joinAlternative(join, partitioned, true), lookupFallbackAlternative()),
                        List.of(),
                        DecisionMode.FORCED,
                        "shuffle_join_strategy=" + pragmas.shuffleJoinStrategy()
                    );
                }
                if (requested == JoinStrategySelector.JoinStrategy.BROADCAST) {
                    String reason = "shuffle_join_strategy=broadcast does not apply to lookup joins";
                    return new Enumeration(
                        List.of(joinAlternative(join, lookup, false)),
                        List.of(excluded(partitioned, ExclusionCode.NOT_ELIGIBLE, reason)),
                        DecisionMode.INELIGIBLE,
                        reason
                    );
                }
                return shuffleJoinUnavailable(join, lookup, List.of(partitioned));
            }

            if (eligible == false) {
                return shuffleJoinUnavailable(join, lookup, List.of(partitioned));
            }
            boolean buildKnown = estimateBytes(group.children.get(1).stats).isUnknown() == false;
            if (buildKnown == false) {
                return new Enumeration(
                    List.of(joinAlternative(join, lookup, false)),
                    List.of(excluded(partitioned, ExclusionCode.UNKNOWN_STATISTICS, BUILD_BYTES_UNKNOWN)),
                    DecisionMode.GATED,
                    BUILD_BYTES_UNKNOWN
                );
            }
            return new Enumeration(
                List.of(joinAlternative(join, lookup, false), joinAlternative(join, partitioned, false)),
                List.of(),
                DecisionMode.COST_BASED,
                null
            );
        }

        /** Keeps the incumbent when no shuffle strategy can run this join. */
        private Enumeration shuffleJoinUnavailable(
            Join join,
            JoinStrategySelector.JoinStrategy incumbent,
            List<JoinStrategySelector.JoinStrategy> shuffleStrategies
        ) {
            ExclusionCode code = shuffleAllowed ? ExclusionCode.NOT_ELIGIBLE : ExclusionCode.SHUFFLE_UNAVAILABLE;
            String reason = shuffleAllowed ? "the join shape does not support a distributed hash join" : SHUFFLE_UNAVAILABLE;
            List<Exclusion> excluded = new ArrayList<>(shuffleStrategies.size());
            for (JoinStrategySelector.JoinStrategy strategy : shuffleStrategies) {
                excluded.add(excluded(strategy, code, reason));
            }
            return new Enumeration(List.of(joinAlternative(join, incumbent, false)), excluded, DecisionMode.INELIGIBLE, reason);
        }

        private Enumeration aggregateAlternatives(Group group, Aggregate aggregate) {
            boolean eligible = aggregateShuffleEligible(aggregate);
            ExclusionCode unavailableCode = shuffleAllowed ? ExclusionCode.NOT_ELIGIBLE : ExclusionCode.SHUFFLE_UNAVAILABLE;
            String unavailableReason = shuffleAllowed ? "the aggregate does not support distributed execution" : SHUFFLE_UNAVAILABLE;
            if (pragmas.shuffleAggExplicit()) {
                AggMode forced = pragmas.shuffleAgg() ? AggMode.DISTRIBUTED : AggMode.COORDINATOR;
                if (forced == AggMode.DISTRIBUTED && eligible == false) {
                    return new Enumeration(
                        List.of(aggregateAlternative(AggMode.COORDINATOR, false)),
                        List.of(new Exclusion(AggMode.DISTRIBUTED.name(), unavailableCode, unavailableReason)),
                        DecisionMode.INELIGIBLE,
                        unavailableReason
                    );
                }
                String pragma = "shuffle_agg=" + pragmas.shuffleAgg();
                Exclusion other = forced == AggMode.DISTRIBUTED
                    ? new Exclusion(AggMode.COORDINATOR.name(), ExclusionCode.PRAGMA_FORCED, pragma)
                    : new Exclusion(AggMode.DISTRIBUTED.name(), ExclusionCode.PRAGMA_DISABLED, pragma);
                return new Enumeration(List.of(aggregateAlternative(forced, true)), List.of(other), DecisionMode.FORCED, pragma);
            }

            if (eligible == false) {
                return new Enumeration(
                    List.of(aggregateAlternative(AggMode.COORDINATOR, false)),
                    List.of(new Exclusion(AggMode.DISTRIBUTED.name(), unavailableCode, unavailableReason)),
                    DecisionMode.INELIGIBLE,
                    unavailableReason
                );
            }
            if (aggregateEvidenceKnown(group, aggregate) == false) {
                // Both modes are enumerated, but evaluateAggregate keeps only the one gatedMode picks for each child.
                return new Enumeration(
                    List.of(aggregateAlternative(AggMode.COORDINATOR, false), aggregateAlternative(AggMode.DISTRIBUTED, false)),
                    List.of(),
                    DecisionMode.GATED,
                    "input rows are unknown"
                );
            }
            return new Enumeration(
                List.of(aggregateAlternative(AggMode.COORDINATOR, false), aggregateAlternative(AggMode.DISTRIBUTED, false)),
                List.of(),
                DecisionMode.COST_BASED,
                null
            );
        }

        private static Exclusion excluded(JoinStrategySelector.JoinStrategy strategy, ExclusionCode code, String reason) {
            return new Exclusion(strategy.name(), code, reason);
        }

        static boolean supportedAggregate(Aggregate aggregate) {
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
            return new Alternative(kind, label, requirements, null, null, false, false);
        }

        private Alternative joinAlternative(Join join, JoinStrategySelector.JoinStrategy strategy, boolean forced) {
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
                false
            );
        }

        /** Lookup stays available when a forced partitioned lookup join has no scan fragment on its left side. */
        private Alternative lookupFallbackAlternative() {
            return new Alternative(
                OperatorKind.JOIN,
                JoinStrategySelector.JoinStrategy.LOOKUP.name(),
                List.of(AnyRequirement.INSTANCE, AnyRequirement.INSTANCE),
                JoinStrategySelector.JoinStrategy.LOOKUP,
                null,
                true,
                true
            );
        }

        private Alternative aggregateAlternative(AggMode mode, boolean forced) {
            return new Alternative(OperatorKind.AGGREGATE, mode.name(), List.of(AnyRequirement.INSTANCE), null, mode, forced, false);
        }

        private Evaluation evaluate(Group group, Alternative alternative, List<Winner> children) {
            return switch (alternative.kind()) {
                case SOURCE -> new Evaluation(SourcePartitioning.INSTANCE, context.workerNodes(), CostBreakdown.EMPTY, 0, false, true);
                case SINGLE, OPAQUE -> new Evaluation(SinglePartitioning.INSTANCE, 1, CostBreakdown.EMPTY, 0, false);
                case STREAMING_UNARY -> evaluateUnary(group, (UnaryPlan) group.plan, children.get(0));
                case JOIN -> evaluateJoin(group, (Join) group.plan, alternative, children);
                case AGGREGATE -> evaluateAggregate(group, (Aggregate) group.plan, alternative, children.get(0));
            };
        }

        private Evaluation evaluateUnary(Group group, UnaryPlan unary, Winner child) {
            if (unary instanceof Limit limit) {
                if (limit.local() && child.delivered() instanceof SourcePartitioning) {
                    return new Evaluation(SourcePartitioning.INSTANCE, child.lanes(), CostBreakdown.EMPTY, 0, false, child.scanFragment());
                }
                return makeSingle(group.children.get(0), child);
            }
            if (unary instanceof TopN topN) {
                if (topN.local() && child.delivered() instanceof SourcePartitioning) {
                    return new Evaluation(SourcePartitioning.INSTANCE, child.lanes(), CostBreakdown.EMPTY, 0, false, child.scanFragment());
                }
                return makeSingle(group.children.get(0), child);
            }
            if (child.delivered() instanceof SourcePartitioning) {
                return new Evaluation(SourcePartitioning.INSTANCE, child.lanes(), CostBreakdown.EMPTY, 0, false, child.scanFragment());
            }
            if (streamingOutsideFragment(unary)) {
                Delivered delivered = translateUp(child.delivered(), unary);
                return new Evaluation(delivered, lanes(delivered, child.lanes()), CostBreakdown.EMPTY, 0, false);
            }
            return makeSingle(group.children.get(0), child);
        }

        private Evaluation makeSingle(Group childGroup, Winner child) {
            if (child.delivered() instanceof SinglePartitioning) {
                return new Evaluation(SinglePartitioning.INSTANCE, 1, CostBreakdown.EMPTY, 0, false);
            }
            Part gather = new Part(CostTerm.EXCHANGE_GATHER, Side.INPUT, gatherCost(childGroup.stats, child.lanes()), 0);
            return new Evaluation(SinglePartitioning.INSTANCE, 1, CostBreakdown.of(gather), 0, false);
        }

        private Evaluation evaluateJoin(Group group, Join join, Alternative alternative, List<Winner> children) {
            Winner left = children.get(0);
            Winner right = children.get(1);
            JoinStrategySelector.JoinStrategy strategy = alternative.joinStrategy();
            PlanStats leftStats = group.children.get(0).stats;
            PlanStats rightStats = group.children.get(1).stats;
            Delivered output;
            CostBreakdown local;
            long outputLanes;

            switch (strategy) {
                case MATERIALIZED -> {
                    output = keepIfPresent(left.delivered(), join.output());
                    outputLanes = lanes(output, left.lanes());
                    local = materializedJoinCost(leftStats, rightStats);
                }
                case BROADCAST -> {
                    output = keepIfPresent(left.delivered(), join.output());
                    outputLanes = lanes(output, left.lanes());
                    local = broadcastJoinCost(leftStats, rightStats);
                }
                case PARTITIONED -> {
                    if (Mapper.isIndexModeLookup(join.right()) && left.scanFragment() == false) {
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
                    local = partitionedJoinCost(leftStats, rightStats);
                }
                case LOOKUP -> {
                    if (alternative.lookupFallback() && left.scanFragment()) {
                        return null;
                    }
                    output = localJoinOutput(join, left);
                    outputLanes = lanes(output, left.lanes());
                    local = withCoordinatorGather(join, left, leftStats, lookupJoinCost(leftStats));
                }
                case LOCAL -> {
                    output = localJoinOutput(join, left);
                    outputLanes = lanes(output, left.lanes());
                    local = withCoordinatorGather(join, left, leftStats, localJoinCost(leftStats, rightStats));
                }
                default -> throw new IllegalStateException("Unsupported join strategy [" + strategy + "]");
            }
            return new Evaluation(output, outputLanes, local, 0, true, foldsIntoFragment(join, strategy, left));
        }

        /** Mirrors {@link Mapper}: a non-distributed join over a scan fragment runs inside it, unless it executes on the coordinator. */
        private static boolean foldsIntoFragment(Join join, JoinStrategySelector.JoinStrategy strategy, Winner left) {
            return switch (strategy) {
                case MATERIALIZED, LOOKUP, LOCAL -> join.executesOn() == ExecuteLocation.REMOTE
                    || (left.scanFragment() && join.executesOn() != ExecuteLocation.COORDINATOR);
                case BROADCAST, PARTITIONED -> false;
            };
        }

        /** Adds the gather of the left input when a coordinator-only join receives distributed rows. */
        private CostBreakdown withCoordinatorGather(Join join, Winner left, PlanStats leftStats, Part joinPart) {
            if (left.delivered() instanceof SinglePartitioning == false && join.executesOn() == ExecuteLocation.COORDINATOR) {
                return CostBreakdown.of(joinPart, new Part(CostTerm.EXCHANGE_GATHER, Side.LEFT, gatherCost(leftStats, left.lanes()), 0));
            }
            return CostBreakdown.of(joinPart);
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
                    CostBreakdown.of(aggregateInPlaceCost(inputStats, child.lanes(), aggregate)),
                    0,
                    false
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
                    CostBreakdown.of(aggregateInPlaceCost(inputStats, child.lanes(), aggregate)),
                    0,
                    false
                );
            }

            if (enumeration(group).mode() == DecisionMode.GATED && alternative.aggregateMode() != gatedMode(child)) {
                return null;
            }

            if (alternative.aggregateMode() == AggMode.DISTRIBUTED
                && groupAttributes == null
                && child.delivered() instanceof SourcePartitioning == false) {
                return new Evaluation(SinglePartitioning.INSTANCE, 1, aggregateCoordinatorCost(inputStats, child, aggregate), 0, false);
            }

            if (alternative.aggregateMode() == AggMode.COORDINATOR) {
                return new Evaluation(SinglePartitioning.INSTANCE, 1, aggregateCoordinatorCost(inputStats, child, aggregate), 0, true);
            }

            Delivered output = aggregateHash(aggregate, groupAttributes);
            return new Evaluation(output, workerLanes, aggregateDistributedCost(inputStats, child, aggregate), 1, true);
        }

        /**
         * The mode a gated aggregate uses when its input statistics are unknown, so cost cannot compare the two. The input's
         * existing distribution decides instead. Input straight from the source stays on the coordinator, as rule-based
         * planning does. Input a shuffle below has already spread across the worker lanes stays distributed: gathering it
         * would undo that shuffle and funnel every row through one node, which is the risk the shuffle was paying to avoid.
         */
        static AggMode gatedMode(Winner child) {
            return switch (child.delivered()) {
                case SourcePartitioning source -> AggMode.COORDINATOR;
                case HashPartitioning hash -> AggMode.DISTRIBUTED;
                case ArbitraryPartitioning arbitrary -> AggMode.DISTRIBUTED;
                case SinglePartitioning single -> throw new IllegalStateException("bug: aggregate over a single lane runs in place");
            };
        }

        /** The reason a gated aggregate kept the mode {@link #gatedMode} picked for its input. */
        String gatedReason(Winner winner) {
            String reason = enumeration(winner.group()).reason();
            return gatedMode(winner.children().get(0)) == AggMode.DISTRIBUTED
                ? reason + " and a shuffle below already spread the input across the workers"
                : reason + " and the input comes straight from the source";
        }

        private Part aggregateInPlaceCost(PlanStats inputStats, long lanes, Aggregate aggregate) {
            double rows = estimateValue(estimateRows(inputStats), CostConstants.UNKNOWN_ROWS);
            GroupEstimate groups = groupEstimate(aggregate, inputStats);
            double work = rows * CostConstants.AGGREGATE_WORK_PER_ROW;
            double states = groups.unknown() ? 0 : groups.value() * CostConstants.AGGREGATE_STATE_BYTES;
            Cost cost = executionCost(work, lanes, 0, 0, 0, states / Math.max(1, context.workerNodes()), states);
            return new Part(CostTerm.FINAL_AGGREGATE, null, cost, -1);
        }

        private CostBreakdown aggregateCoordinatorCost(PlanStats inputStats, Winner child, Aggregate aggregate) {
            AggregateEstimate estimate = aggregateEstimate(inputStats, child, aggregate);
            double initialWork = estimate.inputRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double finalWork = estimate.partialRows() * CostConstants.AGGREGATE_WORK_PER_ROW;
            double intermediateBytes = estimate.partialRows() * estimate.intermediateRowBytes();
            Cost gather = gatherCost(intermediateBytes, child.lanes());
            double stateBytes = estimate.groups().unknown() ? 0 : estimate.groups().value() * CostConstants.AGGREGATE_STATE_BYTES;
            Cost initial = executionCost(initialWork, child.lanes(), 0, 0, 0, 0, 0);
            Cost finalCost = executionCost(finalWork, 1, 0, 0, 0, stateBytes, stateBytes);
            return CostBreakdown.of(
                new Part(CostTerm.PARTIAL_AGGREGATE, null, initial, -1),
                new Part(CostTerm.EXCHANGE_GATHER, Side.INTERMEDIATE, gather, -1),
                new Part(CostTerm.FINAL_AGGREGATE, null, finalCost, -1)
            );
        }

        private CostBreakdown aggregateDistributedCost(PlanStats inputStats, Winner child, Aggregate aggregate) {
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
            return CostBreakdown.of(
                new Part(CostTerm.PARTIAL_AGGREGATE, null, initial, -1),
                new Part(CostTerm.EXCHANGE_HASH, Side.INTERMEDIATE, exchange, -1),
                new Part(CostTerm.FINAL_AGGREGATE, null, finalCost, -1)
            );
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

        private CostBreakdown materializedJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double rows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double bytes = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            long copies = context.workerNodes();
            int parallelism = Math.max(1, workerLanes);
            double copiedBytes = bytes * copies;
            double tableBytes = bytes * CostConstants.HASH_TABLE_OVERHEAD;
            return CostBreakdown.of(
                new Part(CostTerm.REPLAN, null, executionCost(0, parallelism, CostConstants.REPLAN_SECONDS, 0, 0, 0, 0), -1),
                new Part(CostTerm.MATERIALIZE, Side.RIGHT, executionCost(0, parallelism, 0, copiedBytes, copiedBytes, 0, 0), -1),
                new Part(
                    CostTerm.HASH_BUILD,
                    Side.RIGHT,
                    executionCost(rows * CostConstants.BUILD_WORK_PER_ROW * copies, parallelism, 0, 0, 0, tableBytes, tableBytes),
                    -1
                ),
                new Part(
                    CostTerm.HASH_PROBE,
                    Side.LEFT,
                    executionCost(probeRows * CostConstants.PROBE_WORK_PER_ROW, parallelism, 0, 0, 0, 0, 0),
                    -1
                )
            );
        }

        private CostBreakdown broadcastJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double rows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            int parallelism = Math.max(1, workerLanes);
            double buildWork = rows * CostConstants.BUILD_WORK_PER_ROW * context.workerNodes();
            // All lanes on a node probe one shared hash table, so the node holds a single copy regardless of lanes per node.
            double tableBytes = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES) * CostConstants.HASH_TABLE_OVERHEAD;
            return CostBreakdown.of(
                new Part(
                    CostTerm.HASH_BUILD,
                    Side.RIGHT,
                    executionCost(buildWork, parallelism, CostConstants.STAGE_STARTUP_SECONDS, 0, 0, tableBytes, tableBytes),
                    -1
                ),
                new Part(
                    CostTerm.HASH_PROBE,
                    Side.LEFT,
                    executionCost(probeRows * CostConstants.PROBE_WORK_PER_ROW, parallelism, 0, 0, 0, 0, 0),
                    -1
                )
            );
        }

        private CostBreakdown partitionedJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double buildRows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double probeRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            int lanes = Math.max(1, workerLanes);
            double skewedLaneFraction = Math.max(1.0 / lanes, CostConstants.HOT_PARTITION_FRACTION);
            double laneMemory = estimateValue(estimateBytes(rightStats), CostConstants.UNKNOWN_BYTES) * CostConstants.HASH_TABLE_OVERHEAD
                * skewedLaneFraction;
            double nodeMemory = laneMemory * Math.max(1, pragmas.shufflePartitionsPerNode());
            return CostBreakdown.of(
                new Part(
                    CostTerm.HASH_BUILD,
                    Side.RIGHT,
                    executionCost(
                        buildRows * CostConstants.BUILD_WORK_PER_ROW,
                        lanes,
                        CostConstants.STAGE_STARTUP_SECONDS,
                        0,
                        0,
                        nodeMemory,
                        laneMemory
                    ),
                    -1
                ),
                new Part(
                    CostTerm.HASH_PROBE,
                    Side.LEFT,
                    executionCost(probeRows * CostConstants.PROBE_WORK_PER_ROW, lanes, 0, 0, 0, 0, 0),
                    -1
                )
            );
        }

        private Part lookupJoinCost(PlanStats leftStats) {
            double rows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            double work = rows * CostConstants.LOOKUP_WORK_PER_ROW;
            return new Part(CostTerm.LOOKUP, Side.RIGHT, executionCost(work, context.workerNodes(), 0, 0, 0, 0, 0), -1);
        }

        private Part localJoinCost(PlanStats leftStats, PlanStats rightStats) {
            double leftRows = estimateValue(estimateRows(leftStats), CostConstants.UNKNOWN_ROWS);
            double rightRows = estimateValue(estimateRows(rightStats), CostConstants.UNKNOWN_ROWS);
            double work = leftRows * CostConstants.PROBE_WORK_PER_ROW + rightRows * CostConstants.BUILD_WORK_PER_ROW;
            return new Part(CostTerm.LOCAL_JOIN, null, executionCost(work, 1, 0, 0, 0, 0, 0), -1);
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
                // Each node receives one copy, consumed by a single lane that builds the shared hash table.
                bytes,
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
                winner.scanFragment(),
                delivered,
                lanes,
                winner.local(),
                enforcer,
                cost,
                winner.cost().plus(cost),
                shuffleEdges,
                winner.decisionApplicable(),
                winner.fitFallback()
            );
        }

        /**
         * Returns whether a cost-based decision must drop this candidate because a memory fit check failed. Forced alternatives,
         * decisions with a single legal alternative, and the constrained group of a counterfactual search are never pruned.
         */
        private boolean prunedByFit(Winner winner) {
            Alternative alternative = winner.alternative();
            if (alternative.forced() || winner.decisionApplicable() == false) {
                return false;
            }
            Group group = winner.group();
            if (group.plan == constrainedPlan || enumeration(group).mode() != DecisionMode.COST_BASED) {
                return false;
            }
            for (FitCheck check : fitChecks(group, alternative)) {
                if (check.passed() == false) {
                    return true;
                }
            }
            return false;
        }

        /** Evaluates the memory fit checks that apply to {@code alternative}, whether or not they pass. */
        List<FitCheck> fitChecks(Group group, Alternative alternative) {
            if (alternative.kind() == OperatorKind.JOIN) {
                if (alternative.joinStrategy() == JoinStrategySelector.JoinStrategy.MATERIALIZED) {
                    Estimate bytes = estimateBytes(group.children.get(1).stats);
                    return List.of(probabilityCheck("materialize_build_bytes", bytes, context.materializeLimitBytes()));
                }
                if (alternative.joinStrategy() == JoinStrategySelector.JoinStrategy.BROADCAST) {
                    Estimate bytes = estimateBytes(group.children.get(1).stats);
                    // The build is shared by all lanes on a node, so the whole node budget applies to one table.
                    double buildBudget = CostConstants.BROADCAST_MEMORY_BYTES / CostConstants.HASH_TABLE_OVERHEAD;
                    return List.of(probabilityCheck("broadcast_build_per_node", bytes, buildBudget));
                }
            }
            if (alternative.kind() == OperatorKind.AGGREGATE && alternative.aggregateMode() == AggMode.COORDINATOR) {
                Aggregate aggregate = (Aggregate) group.plan;
                GroupEstimate groups = groupEstimate(aggregate, group.children.get(0).stats);
                EstimateView demand = groups.unknown()
                    ? EstimateView.of(Estimate.UNKNOWN)
                    : new EstimateView(
                        groups.value() * CostConstants.AGGREGATE_STATE_BYTES,
                        groups.upperBound() * CostConstants.AGGREGATE_STATE_BYTES,
                        groups.provenance()
                    );
                boolean passed = groups.unknown()
                    || (groups.upperBound() * CostConstants.AGGREGATE_STATE_BYTES > CostConstants.COORDINATOR_MEMORY_BYTES) == false;
                return List.of(
                    new FitCheck(
                        "coordinator_aggregate_state",
                        FitMethod.UPPER_BOUND,
                        demand,
                        (long) CostConstants.COORDINATOR_MEMORY_BYTES,
                        null,
                        null,
                        passed
                    )
                );
            }
            return List.of();
        }

        private static FitCheck probabilityCheck(String name, Estimate demand, double budgetBytes) {
            double probability = demand.probabilityAtMost(budgetBytes);
            return new FitCheck(
                name,
                FitMethod.PROBABILITY,
                EstimateView.of(demand),
                (long) budgetBytes,
                probability,
                CostConstants.MIN_FIT_PROBABILITY,
                (probability < CostConstants.MIN_FIT_PROBABILITY) == false
            );
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

        /**
         * Keeps, per set of physical properties a parent can observe, the candidate with the lowest score and the one the tie-break
         * rules prefer, if different. Pruning on fewer properties would drop the only candidate some parent alternative accepts.
         * Keeping the lowest one makes every group's lowest score exact, since a parent's lowest plan is built from its children's
         * lowest plans; keeping only the preferred one would let tie margins compound up the tree.
         */
        private List<Winner> cheapestPerProperties(List<Winner> candidates) {
            Map<Properties, List<Winner>> byProperties = new LinkedHashMap<>();
            for (Winner candidate : candidates) {
                byProperties.computeIfAbsent(new Properties(candidate.delivered(), candidate.scanFragment()), k -> new ArrayList<>())
                    .add(candidate);
            }
            List<Winner> winners = new ArrayList<>(byProperties.size());
            for (List<Winner> sameProperties : byProperties.values()) {
                Winner lowest = lowest(sameProperties);
                Winner preferred = preferred(sameProperties, lowest);
                winners.add(preferred);
                if (lowest != preferred) {
                    winners.add(lowest);
                }
            }
            return winners;
        }

        private record Properties(Delivered delivered, boolean scanFragment) {}

        private Winner cheapest(List<Winner> candidates) {
            return candidates.isEmpty() ? null : preferred(candidates, lowest(candidates));
        }

        private static Winner lowest(List<Winner> candidates) {
            Winner lowest = null;
            for (Winner candidate : candidates) {
                if (lowest == null
                    || candidate.cost().objective(CostConstants.WORK_WEIGHT) < lowest.cost().objective(CostConstants.WORK_WEIGHT)) {
                    lowest = candidate;
                }
            }
            return lowest;
        }

        /**
         * Returns the candidate that {@link #cheaper}'s secondary rules prefer among those whose score ties {@code lowest}'s. A
         * pairwise scan would depend on the candidates' order, because ties are not transitive: two candidates can each tie a third
         * while one beats the other. Candidates that tie the lowest score also tie each other, so only the secondary rules decide
         * among them, and the choice never loses to another candidate by more than the tie margin.
         */
        private Winner preferred(List<Winner> candidates, Winner lowest) {
            Winner best = null;
            for (Winner candidate : candidates) {
                if (compareScores(candidate.cost(), lowest.cost()) == 0 && (best == null || cheaper(candidate, best))) {
                    best = candidate;
                }
            }
            return best;
        }

        /**
         * Compares two costs by score. Scores within {@link CostConstants#TIE_EPSILON} of each other, relative to the larger one,
         * are a tie and compare as {@code 0}; ties are broken by {@link #cheaper}'s secondary rules.
         */
        static int compareScores(Cost left, Cost right) {
            double leftCost = left.objective(CostConstants.WORK_WEIGHT);
            double rightCost = right.objective(CostConstants.WORK_WEIGHT);
            if (Double.isFinite(leftCost) == false || Double.isFinite(rightCost) == false) {
                if (leftCost != rightCost) {
                    return leftCost < rightCost ? -1 : 1;
                }
                return 0;
            }
            double relativeDifference = Math.abs(leftCost - rightCost) / Math.max(Math.max(Math.abs(leftCost), Math.abs(rightCost)), 1e-12);
            if (relativeDifference > CostConstants.TIE_EPSILON) {
                return leftCost < rightCost ? -1 : 1;
            }
            return 0;
        }

        private boolean cheaper(Winner left, Winner right) {
            int byScore = compareScores(left.cost(), right.cost());
            if (byScore != 0) {
                return byScore < 0;
            }
            int byPreference = Integer.compare(subtreePreference(left), subtreePreference(right));
            if (byPreference != 0) {
                return byPreference < 0;
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

        /**
         * Sums {@link #tiePreference} over every operator in {@code winner}'s plan. Plans that differ in a join or aggregate below
         * the root are compared above it, where that operator's own rank is no longer in view, so the rank has to travel with
         * the plan.
         */
        int subtreePreference(Winner winner) {
            int preference = tiePreference(winner);
            for (Winner child : winner.children()) {
                preference += subtreePreference(child);
            }
            return preference;
        }

        /**
         * Ranks {@code winner} for breaking a score tie; the lower rank wins. A subquery join ranks broadcast first: like
         * materializing, it ships one copy of the build side to every node, but it streams that copy from the producers
         * instead of collecting it on the coordinator and planning the query a second time. Materializing ranks next because
         * it is the incumbent. Every other join or aggregate ranks the incumbent first, and operators without a decision rank 0.
         */
        int tiePreference(Winner winner) {
            Alternative alternative = winner.alternative();
            if (alternative.kind() == OperatorKind.JOIN && winner.group().plan instanceof SubqueryLookupJoin) {
                return switch (alternative.joinStrategy()) {
                    case BROADCAST -> 0;
                    case MATERIALIZED -> 1;
                    case PARTITIONED, LOOKUP, LOCAL -> 2;
                };
            }
            if (alternative.kind() == OperatorKind.JOIN || alternative.kind() == OperatorKind.AGGREGATE) {
                return incumbent(winner) ? 0 : 1;
            }
            return 0;
        }

        /** Returns whether {@code winner} uses the strategy that rule-based planning would pick for its group. */
        boolean incumbent(Winner winner) {
            Alternative alternative = winner.alternative();
            if (alternative.kind() == OperatorKind.JOIN) {
                return alternative.joinStrategy() == incumbent((Join) winner.group().plan);
            }
            return alternative.kind() == OperatorKind.AGGREGATE && alternative.aggregateMode() == AggMode.COORDINATOR;
        }

        /** Collects the published choices of a solved plan and the keys that two decision points chose differently. */
        Decided decisions(Winner rootWinner) {
            Map<JoinKey, JoinStrategySelector.JoinStrategy> joins = new HashMap<>();
            Map<AggKey, AggMode> aggregates = new HashMap<>();
            Set<JoinKey> conflictingJoins = new LinkedHashSet<>();
            Set<AggKey> conflictingAggregates = new LinkedHashSet<>();
            Set<Winner> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            collectDecisions(rootWinner, visited, joins, aggregates, conflictingJoins, conflictingAggregates);
            return new Decided(joins, aggregates, conflictingJoins, conflictingAggregates);
        }

        private void collectDecisions(
            Winner winner,
            Set<Winner> visited,
            Map<JoinKey, JoinStrategySelector.JoinStrategy> joins,
            Map<AggKey, AggMode> aggregates,
            Set<JoinKey> conflictingJoins,
            Set<AggKey> conflictingAggregates
        ) {
            if (visited.add(winner) == false) {
                return;
            }
            Alternative alternative = winner.alternative();
            if (winner.decisionApplicable() && alternative.kind() == OperatorKind.JOIN) {
                JoinKey key = JoinKey.of((Join) winner.group().plan);
                if (conflictingJoins.contains(key) == false) {
                    JoinStrategySelector.JoinStrategy previous = joins.putIfAbsent(key, alternative.joinStrategy());
                    if (previous != null && previous != alternative.joinStrategy()) {
                        joins.remove(key);
                        conflictingJoins.add(key);
                    }
                }
            }
            if (winner.decisionApplicable() && alternative.kind() == OperatorKind.AGGREGATE) {
                AggKey key = AggKey.of(((Aggregate) winner.group().plan).aggregates());
                if (conflictingAggregates.contains(key) == false) {
                    AggMode previous = aggregates.putIfAbsent(key, alternative.aggregateMode());
                    if (previous != null && previous != alternative.aggregateMode()) {
                        aggregates.remove(key);
                        conflictingAggregates.add(key);
                    }
                }
            }
            for (Winner child : winner.children()) {
                collectDecisions(child, visited, joins, aggregates, conflictingJoins, conflictingAggregates);
            }
        }

        /**
         * Estimates the number of groups of {@code aggregate} as the product of the grouping columns' NDVs, capped by the input rows.
         * A column without an NDV contributes the input rows, the most groups it can produce, with {@link Provenance#DEFAULT}.
         * That lets the coordinator fit check reject a coordinator aggregation that could hold too many groups.
         * Its provenance is the weakest of the inputs, and at least {@link Provenance#DERIVED} when several columns are combined.
         */
        static GroupEstimate groupEstimate(Aggregate aggregate, PlanStats inputStats) {
            Estimate rowsEstimate = estimateRows(inputStats);
            if (rowsEstimate.isUnknown()) {
                return GroupEstimate.UNKNOWN;
            }
            double rows = estimateValue(rowsEstimate, CostConstants.UNKNOWN_ROWS);
            double rowsUpper = estimateUpperBound(rowsEstimate, CostConstants.UNKNOWN_ROWS);
            Provenance provenance = rowsEstimate.provenance();
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
                    // A column has at most one distinct value per row, so the input rows bound its NDV.
                    provenance = weaker(provenance, Provenance.DEFAULT);
                    value = safeMultiply(value, Math.max(1, rows));
                    upperBound = safeMultiply(upperBound, Math.max(1, rowsUpper));
                    continue;
                }
                provenance = weaker(provenance, ndv.provenance());
                value = safeMultiply(value, Math.max(1, estimateValue(ndv, 1)));
                upperBound = safeMultiply(upperBound, Math.max(1, estimateUpperBound(ndv, 1)));
            }
            if (grouped.size() > 1) {
                provenance = weaker(provenance, Provenance.DERIVED);
            }
            return new GroupEstimate(Math.min(rows, value), Math.min(rowsUpper, upperBound), provenance);
        }

        static Provenance weaker(Provenance left, Provenance right) {
            return left.ordinal() >= right.ordinal() ? left : right;
        }

        static List<Attribute> groupAttributes(Aggregate aggregate) {
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

    enum OperatorKind {
        SOURCE,
        SINGLE,
        STREAMING_UNARY,
        OPAQUE,
        JOIN,
        AGGREGATE
    }

    enum Enforcer {
        GATHER,
        HASH,
        BROADCAST
    }

    sealed interface Requirement permits AnyRequirement, SingleRequirement, HashRequirement, BroadcastRequirement {}

    enum AnyRequirement implements Requirement {
        INSTANCE
    }

    enum SingleRequirement implements Requirement {
        INSTANCE
    }

    enum BroadcastRequirement implements Requirement {
        INSTANCE
    }

    record HashRequirement(List<NameId> keys) implements Requirement {
        HashRequirement {
            keys = List.copyOf(keys);
        }
    }

    sealed interface Delivered permits SourcePartitioning, SinglePartitioning, HashPartitioning, ArbitraryPartitioning {}

    enum SourcePartitioning implements Delivered {
        INSTANCE
    }

    enum SinglePartitioning implements Delivered {
        INSTANCE
    }

    enum ArbitraryPartitioning implements Delivered {
        INSTANCE
    }

    record HashPartitioning(List<NameId> keys, boolean exact) implements Delivered {
        HashPartitioning {
            keys = List.copyOf(keys);
        }
    }

    static final class Group {
        final LogicalPlan plan;
        final List<Group> children;
        final PlanStats stats;
        private final Map<Requirement, List<Winner>> winners = new HashMap<>();
        private List<Winner> nativeCandidates;
        private Enumeration enumeration;
        private boolean building;

        private Group(LogicalPlan plan, List<Group> children, PlanStats stats) {
            this.plan = plan;
            this.children = List.copyOf(children);
            this.stats = stats;
        }
    }

    /**
     * The legal alternatives of a group, the strategies that were considered and dropped before costing, and how the choice among
     * the remaining alternatives is made.
     */
    record Enumeration(List<Alternative> alternatives, List<Exclusion> excluded, DecisionMode mode, @Nullable String reason) {
        Enumeration {
            alternatives = List.copyOf(alternatives);
            excluded = List.copyOf(excluded);
        }

        static Enumeration only(Alternative alternative) {
            return new Enumeration(List.of(alternative), List.of(), DecisionMode.INELIGIBLE, null);
        }
    }

    record Alternative(
        OperatorKind kind,
        String label,
        List<Requirement> childRequirements,
        JoinStrategySelector.JoinStrategy joinStrategy,
        AggMode aggregateMode,
        boolean forced,
        boolean lookupFallback
    ) {
        Alternative {
            childRequirements = List.copyOf(childRequirements);
        }
    }

    /**
     * One labelled term of an operator's local cost. {@code edge} is the index of the child whose output the part moves, or
     * {@code -1} when the part runs inside the operator.
     */
    record Part(CostTerm term, @Nullable Side side, Cost cost, int edge) {}

    /** The local cost of an operator as an ordered list of parts; its total is the sum of the parts. */
    record CostBreakdown(List<Part> parts) {
        static final CostBreakdown EMPTY = new CostBreakdown(List.of());

        CostBreakdown {
            parts = List.copyOf(parts);
        }

        static CostBreakdown of(Part... parts) {
            return new CostBreakdown(List.of(parts));
        }

        Cost total() {
            Cost total = Cost.ZERO;
            for (Part part : parts) {
                total = total.plus(part.cost());
            }
            return total;
        }
    }

    /**
     * The native properties and local cost of one alternative over chosen children. {@code scanFragment} mirrors whether {@link Mapper}
     * folds the subtree into one {@code FragmentExec}: a partitioned lookup join requires that of its left side, and the delivered
     * partitioning alone cannot tell, because a broadcast join keeps its probe's source partitioning without being a fragment.
     */
    record Evaluation(
        Delivered delivered,
        long lanes,
        CostBreakdown local,
        int shuffleEdges,
        boolean decisionApplicable,
        boolean scanFragment
    ) {
        Evaluation(Delivered delivered, long lanes, CostBreakdown local, int shuffleEdges, boolean decisionApplicable) {
            this(delivered, lanes, local, shuffleEdges, decisionApplicable, false);
        }
    }

    /**
     * The cheapest plan found for a group under one set of physical properties. {@code local} is the operator's own cost, {@code enforcer}
     * the exchange added on top to meet a parent's requirement, and {@code cost} the total of the subtree including both.
     * {@code scanFragment} is whether the native plan maps to a single data-node scan fragment, see {@link Evaluation}.
     */
    record Winner(
        Group group,
        Alternative alternative,
        List<Winner> children,
        Delivered nativeDelivered,
        boolean scanFragment,
        Delivered delivered,
        long lanes,
        CostBreakdown local,
        @Nullable Enforcer enforcer,
        Cost enforcerCost,
        Cost cost,
        int shuffleEdges,
        boolean decisionApplicable,
        boolean fitFallback
    ) {
        Winner {
            children = List.copyOf(children);
        }

        Winner asFitFallback() {
            return new Winner(
                group,
                alternative,
                children,
                nativeDelivered,
                scanFragment,
                delivered,
                lanes,
                local,
                enforcer,
                enforcerCost,
                cost,
                shuffleEdges,
                decisionApplicable,
                true
            );
        }
    }

    private record AggregateEstimate(double inputRows, double partialRows, double intermediateRowBytes, GroupEstimate groups) {}

    record GroupEstimate(double value, double upperBound, Provenance provenance) {
        static final GroupEstimate UNKNOWN = new GroupEstimate(0, 0, Provenance.UNKNOWN);

        boolean unknown() {
            return provenance == Provenance.UNKNOWN;
        }
    }

    record Cost(double latency, double work, double networkBytes, double coordinatorBytes, double peakNodeBytes, double peakLaneBytes) {
        static final Cost ZERO = new Cost(0, 0, 0, 0, 0, 0);

        Cost plus(Cost other) {
            return new Cost(
                latency + other.latency,
                work + other.work,
                networkBytes + other.networkBytes,
                coordinatorBytes + other.coordinatorBytes,
                Math.max(peakNodeBytes, other.peakNodeBytes),
                Math.max(peakLaneBytes, other.peakLaneBytes)
            );
        }

        double objective(double workWeight) {
            return latency + workWeight * work;
        }

        CostVector toVector() {
            return new CostVector(latency * 1e9, work, networkBytes, coordinatorBytes, peakNodeBytes, peakLaneBytes);
        }
    }

    static final class CostConstants {
        static final int MAX_CANDIDATES = 10_000;
        static final double TIE_EPSILON = 0.05;
        static final double MIN_FIT_PROBABILITY = 0.99;
        static final double WORK_WEIGHT = CostVector.WORK_WEIGHT;
        static final double CPU_SECONDS_PER_WORK_UNIT = 1e-6;
        static final double BUILD_WORK_PER_ROW = 2;
        static final double PROBE_WORK_PER_ROW = 1;
        static final double LOOKUP_WORK_PER_ROW = 4;
        static final double AGGREGATE_WORK_PER_ROW = 3;
        static final double SERIALIZATION_WORK_PER_BYTE = 0.01;
        static final double NETWORK_BYTES_PER_SECOND = 256d * 1024 * 1024;
        static final double COORDINATOR_BYTES_PER_SECOND = 512d * 1024 * 1024;
        static final double STAGE_STARTUP_SECONDS = 0.01;
        static final double REPLAN_SECONDS = 0.005;
        static final double HASH_TABLE_OVERHEAD = 2;
        static final double HOT_PARTITION_FRACTION = 0.1;
        static final double BROADCAST_MEMORY_BYTES = 128d * 1024 * 1024;
        static final double COORDINATOR_MEMORY_BYTES = 512d * 1024 * 1024;
        static final double AGGREGATE_STATE_BYTES = 64;
        static final double UNKNOWN_ROWS = 100_000;
        static final double UNKNOWN_BYTES = 64d * 1024 * 1024;
        static final double DEFAULT_ROW_BYTES = 32;
        static final double MIN_INTERMEDIATE_ROW_BYTES = 16;

        private CostConstants() {}
    }

    static final class SearchAborted extends RuntimeException {
        SearchAborted() {
            super(null, null, false, false);
        }
    }
}
