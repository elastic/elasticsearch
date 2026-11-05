/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.CostConstants;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.AlternativeReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.AlternativeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostComponent;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostTerm;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostVector;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionMode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Exclusion;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.ExclusionCode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OutcomeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Side;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.TieBreakRule;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.ColumnStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector.JoinStrategy;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Checks the decision report that {@link CostBasedOptimizer} attaches when an explain level is requested. Every scenario also runs
 * {@link #explain}, which asserts the report invariants from the design (I1 to I6, I8, I10) at both explain levels.
 */
public class CostBasedOptimizerExplainTests extends ESTestCase {
    private static final long MATERIALIZE_LIMIT_BYTES = 128L * 1024 * 1024;
    private static final Set<String> COST_VECTOR_FIELDS = Set.of(
        "score",
        "latency_nanos",
        "work_units",
        "network_in_bytes",
        "coordinator_in_bytes",
        "peak_node_in_bytes",
        "peak_lane_in_bytes"
    );

    public void testLookupJoinUnderAggregate() {
        Source joinSource = new Source(2, 2, "LOOKUP JOIN customers ON key");
        LookupUnderAggregate shape = lookupUnderAggregate(joinSource, 100_000_000, 50_000_000, 10_000_000, true);

        PlanDecisions decisions = explain(shape.aggregate(), shape.statistics(), Settings.EMPTY);
        RunReport report = decisions.report();

        assertEquals(AggMode.DISTRIBUTED, decisions.aggregate(shape.aggregate().aggregates()).orElseThrow());
        assertEquals(JoinStrategy.PARTITIONED, decisions.join(shape.join()).orElseThrow());
        assertEquals(RunStatus.OK, report.status());
        assertEquals(2, report.decisions().size());
        assertEquals(2, report.search().counterfactualSearches());

        DecisionReport aggregate = report.decisions().get(0);
        assertEquals("d1", aggregate.id());
        assertEquals(AggregateDistributionDecisionKind.NAME, aggregate.kind());
        assertEquals("Aggregate", aggregate.node());
        assertNull(aggregate.source());
        assertEquals(DecisionMode.COST_BASED, aggregate.mode());
        assertEquals("DISTRIBUTED", aggregate.chosen().name());
        assertEquals("hash(" + shape.probeKey() + ")", aggregate.chosen().delivered());
        assertEquals("single", aggregate.required());
        assertEquals(List.of("key{r}"), aggregate.anchor());
        assertEquals(
            List.of(
                component(CostTerm.PARTIAL_AGGREGATE, null),
                component(CostTerm.EXCHANGE_HASH, Side.INTERMEDIATE),
                component(CostTerm.FINAL_AGGREGATE, null),
                component(CostTerm.EXCHANGE_GATHER, Side.OUTPUT)
            ),
            terms(aggregate.chosen().components())
        );
        assertEquals(1, aggregate.alternatives().size());
        AlternativeReport coordinator = aggregate.alternatives().get(0);
        assertEquals("COORDINATOR", coordinator.name());
        assertEquals(AlternativeStatus.REJECTED_FIT, coordinator.status());
        assertEquals("single", coordinator.delivered());
        assertThat(coordinator.delta().score(), greaterThan(0d));
        assertEquals(List.of("coordinator_aggregate_state"), failedChecks(coordinator));
        assertEquals(
            "DISTRIBUTED chosen by cost. COORDINATOR rejected: coordinator_aggregate_state failed (demand 610.3mb > budget 512mb).",
            aggregate.summary()
        );

        DecisionReport join = report.decisions().get(1);
        assertEquals("d2", join.id());
        assertEquals(JoinStrategyDecisionKind.NAME, join.kind());
        assertEquals("LookupJoin", join.node());
        assertEquals(joinSource.text(), join.source().text());
        assertEquals(joinSource.lineNumber(), join.source().line());
        assertEquals(joinSource.columnNumber(), join.source().column());
        assertEquals(DecisionMode.COST_BASED, join.mode());
        assertEquals("PARTITIONED", join.chosen().name());
        assertEquals(List.of("key{r}", "key{r}"), join.anchor());
        assertEquals(
            List.of(
                component(CostTerm.HASH_BUILD, Side.RIGHT),
                component(CostTerm.HASH_PROBE, Side.LEFT),
                component(CostTerm.EXCHANGE_HASH, Side.LEFT),
                component(CostTerm.EXCHANGE_HASH, Side.RIGHT)
            ),
            terms(join.chosen().components())
        );
        AlternativeReport lookup = join.alternatives().get(0);
        assertEquals("LOOKUP", lookup.name());
        assertEquals(AlternativeStatus.LOST_ON_COST, lookup.status());
        assertEquals(List.of(), lookup.otherDecisions());
        assertThat(join.summary(), startsWith("PARTITIONED chosen by cost. LOOKUP scores "));

        assertEquals(
            List.of(
                new PlanDecisions.DecisionPoint(
                    "d1",
                    AggregateDistributionDecisionKind.INSTANCE,
                    AggKey.of(shape.aggregate().aggregates()),
                    "DISTRIBUTED"
                ),
                new PlanDecisions.DecisionPoint("d2", JoinStrategyDecisionKind.INSTANCE, JoinKey.of(shape.join()), "PARTITIONED")
            ),
            decisions.points()
        );
    }

    public void testExplainText() {
        Source joinSource = new Source(2, 2, "LOOKUP JOIN customers ON key");
        LookupUnderAggregate shape = lookupUnderAggregate(joinSource, 100_000_000, 50_000_000, 10_000_000, true);

        RunReport report = explain(shape.aggregate(), shape.statistics(), Settings.EMPTY).report();

        String expected = """
            run 0 (initial) status=ok decisions=2 candidates=9 counterfactuals=2
            plan <cost>
            Exchange[GATHER]
            \\_Aggregate d1=DISTRIBUTED cost_based basis=exact
              |  vs COORDINATOR score=<score> rejected_fit: coordinator_aggregate_state demand=610.3mb>512mb
              \\_LookupJoin[LOOKUP JOIN customers ON key @%d:%d] d2=PARTITIONED cost_based basis=exact
                |  vs LOOKUP score=<score>
                |_Exchange[HASH %s]
                | \\_EsRelation[probe] rows=100.0M bytes=7.4gb exact [+1 ops]
                \\_Exchange[HASH %s]
                  \\_EsRelation[customers] rows=50.0M bytes=4.6gb exact""".formatted(
            joinSource.lineNumber(),
            joinSource.columnNumber(),
            shape.probeKey(),
            shape.lookupKey()
        );
        assertEquals(expected, withoutNumbers(report.explain()));
    }

    public void testFitRejectedAlternativeCanScoreLower() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(800_000, 80_000_000, 100));

        RunReport report = explain(join, statistics, pragmas(QueryPragmas.SHUFFLE_JOIN.getKey(), true)).report();

        DecisionReport decision = report.decisions().get(0);
        assertEquals(DecisionMode.COST_BASED, decision.mode());
        assertEquals("PARTITIONED", decision.chosen().name());
        assertEquals(List.of(new Exclusion("MATERIALIZED", ExclusionCode.PRAGMA_FORCED, "shuffle_join=true")), decision.excluded());
        AlternativeReport broadcast = decision.alternatives().get(0);
        assertEquals("BROADCAST", broadcast.name());
        assertEquals(AlternativeStatus.REJECTED_FIT, broadcast.status());
        assertThat(broadcast.delta().score(), lessThan(0d));
        assertEquals(List.of("broadcast_build_per_node"), failedChecks(broadcast));
        assertFalse(decision.searchAnomaly());
        assertEquals(
            "PARTITIONED chosen by cost. BROADCAST rejected: broadcast_build_per_node failed (p=0.00 < 0.99).",
            decision.summary()
        );
    }

    public void testGatedWhenBuildBytesAreUnknown() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000, 8_000_000, 80));
        statistics.put(join.right(), stats(Estimate.UNKNOWN, Estimate.UNKNOWN, Estimate.UNKNOWN));

        DecisionReport decision = explain(join, statistics, Settings.EMPTY).report().decisions().get(0);

        assertEquals(DecisionMode.GATED, decision.mode());
        assertEquals("MATERIALIZED", decision.chosen().name());
        assertEquals(List.of(), decision.alternatives());
        assertEquals(
            List.of(ExclusionCode.UNKNOWN_STATISTICS, ExclusionCode.UNKNOWN_STATISTICS),
            decision.excluded().stream().map(Exclusion::code).toList()
        );
        assertEquals(Provenance.UNKNOWN, decision.estimateBasis());
        assertThat(decision.summary(), startsWith("MATERIALIZED kept: "));
        assertThat(decision.summary(), endsWith(", so BROADCAST and PARTITIONED were not costed."));
    }

    public void testGatedAggregateStaysDistributedAfterShuffle() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        Attribute person = attribute("person");
        EsRelation probe = new EsRelation(
            Source.EMPTY,
            "probe",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(),
            List.of(leftKey, person)
        );
        SubqueryLookupJoin join = subqueryJoin(probe, relation("build", rightKey), leftKey, rightKey);
        Aggregate aggregate = new Aggregate(Source.EMPTY, join, List.of(person), List.of(person));
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(50_000_000, 4_000_000_000d, 80));

        PlanDecisions decisions = explain(aggregate, statistics, Settings.EMPTY);

        assertEquals(JoinStrategy.PARTITIONED, decisions.join(join).orElseThrow());
        assertEquals(AggMode.DISTRIBUTED, decisions.aggregate(aggregate.aggregates()).orElseThrow());
        DecisionReport decision = decisions.report().decisions().get(0);
        assertEquals(AggregateDistributionDecisionKind.NAME, decision.kind());
        assertEquals(DecisionMode.GATED, decision.mode());
        assertEquals(List.of(), decision.alternatives());
        String reason = "input rows are unknown and a shuffle below already spread the input across the workers";
        assertEquals(List.of(new Exclusion("COORDINATOR", ExclusionCode.UNKNOWN_STATISTICS, reason)), decision.excluded());
        assertEquals("DISTRIBUTED kept: " + reason + ", so COORDINATOR was not costed.", decision.summary());
    }

    public void testGatedAggregateOverSourceStaysOnCoordinator() {
        Attribute key = attribute("key");
        EsRelation source = relation("source", key);
        Aggregate aggregate = new Aggregate(Source.EMPTY, source, List.of(key), List.of(key));
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(source, stats(Estimate.UNKNOWN, Estimate.UNKNOWN, Estimate.UNKNOWN));

        PlanDecisions decisions = explain(aggregate, statistics, Settings.EMPTY);

        assertEquals(AggMode.COORDINATOR, decisions.aggregate(aggregate.aggregates()).orElseThrow());
        DecisionReport decision = decisions.report().decisions().get(0);
        assertEquals(DecisionMode.GATED, decision.mode());
        String reason = "input rows are unknown and the input comes straight from the source";
        assertEquals(List.of(new Exclusion("DISTRIBUTED", ExclusionCode.UNKNOWN_STATISTICS, reason)), decision.excluded());
    }

    public void testForcedByPragma() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(100, 1_024, 16));

        DecisionReport decision = explain(join, statistics, pragmas(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast")).report()
            .decisions()
            .get(0);

        assertEquals(DecisionMode.FORCED, decision.mode());
        assertEquals("BROADCAST", decision.chosen().name());
        assertEquals(List.of(), decision.alternatives());
        String reason = "shuffle_join_strategy=broadcast";
        assertEquals(
            List.of(
                new Exclusion("MATERIALIZED", ExclusionCode.PRAGMA_FORCED, reason),
                new Exclusion("PARTITIONED", ExclusionCode.PRAGMA_FORCED, reason)
            ),
            decision.excluded()
        );
        assertEquals("BROADCAST forced by shuffle_join_strategy=broadcast.", decision.summary());
    }

    public void testIneligibleDecision() {
        LookupUnderAggregate shape = lookupUnderAggregate(Source.EMPTY, 1_000_000, 1_000, 1_000, false);

        RunReport report = explain(shape.aggregate(), shape.statistics(), pragmas(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast"))
            .report();

        DecisionReport join = report.decisions().stream().filter(d -> d.kind().equals(JoinStrategyDecisionKind.NAME)).findFirst().get();
        assertEquals(DecisionMode.INELIGIBLE, join.mode());
        assertEquals("LOOKUP", join.chosen().name());
        String reason = "shuffle_join_strategy=broadcast does not apply to lookup joins";
        assertEquals(List.of(new Exclusion("PARTITIONED", ExclusionCode.NOT_ELIGIBLE, reason)), join.excluded());
        assertEquals("LOOKUP is the only legal strategy: " + reason + ".", join.summary());
    }

    public void testIneligibleRun() {
        LookupUnderAggregate shape = lookupUnderAggregate(Source.EMPTY, 1_000_000, 1_000, 1_000, false);
        Settings noShuffle = pragmas(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 0);

        PlanDecisions decisions = explain(shape.aggregate(), shape.statistics(), noShuffle);

        RunReport report = decisions.report();
        assertEquals(RunStatus.INELIGIBLE, report.status());
        assertEquals("shuffle execution is disabled", report.statusReason());
        assertNull(report.planCost());
        assertEquals(List.of(), report.decisions());
        assertEquals(Map.of(), decisions.joins());
        assertEquals(Map.of(), decisions.aggregates());
        assertEquals("run 0 (initial) status=ineligible: shuffle execution is disabled", report.explain());
    }

    public void testTieReportsTieBreakRule() {
        LookupUnderAggregate shape = lookupUnderAggregate(Source.EMPTY, 3_000_000, 1_000, 30_000, false);

        DecisionReport aggregate = explain(shape.aggregate(), shape.statistics(), Settings.EMPTY).report().decisions().get(0);

        assertEquals("COORDINATOR", aggregate.chosen().name());
        AlternativeReport distributed = aggregate.alternatives().get(0);
        assertEquals("DISTRIBUTED", distributed.name());
        assertEquals(AlternativeStatus.LOST_ON_TIE, distributed.status());
        assertEquals(TieBreakRule.INCUMBENT, distributed.tieBreak().rule());
        assertEquals(CostConstants.TIE_EPSILON * 100, distributed.tieBreak().epsilonPercent(), 0);
        assertThat(Math.abs(distributed.delta().percent()), lessThanOrEqualTo(CostConstants.TIE_EPSILON * 100));
        assertThat(aggregate.summary(), endsWith(", tie broken by incumbent."));
    }

    /**
     * Materializing folds the join into the scan fragment while broadcasting does not, so the search keeps both plans and
     * settles the tie above the join, at the aggregate.
     */
    public void testSmallBuildTieBreaksTowardBroadcastBelowAggregate() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        SubqueryLookupJoin join = subqueryJoin(relation("probe", leftKey), relation("build", rightKey), leftKey, rightKey);
        Aggregate aggregate = new Aggregate(Source.EMPTY, join, List.of(leftKey), List.of(leftKey));
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(100, 1_024, 16));

        PlanDecisions decisions = explain(aggregate, statistics, Settings.EMPTY);

        assertEquals(JoinStrategy.BROADCAST, decisions.join(join).orElseThrow());
        DecisionReport decision = decisions.report()
            .decisions()
            .stream()
            .filter(report -> report.kind().equals(JoinStrategyDecisionKind.NAME))
            .findFirst()
            .orElseThrow();
        assertEquals("BROADCAST", decision.chosen().name());
        AlternativeReport materialized = decision.alternatives()
            .stream()
            .filter(alternative -> alternative.name().equals("MATERIALIZED"))
            .findFirst()
            .orElseThrow();
        assertEquals(AlternativeStatus.LOST_ON_TIE, materialized.status());
        assertEquals(TieBreakRule.BROADCAST_PREFERRED, materialized.tieBreak().rule());
    }

    public void testCouplingShowsOtherDecisions() {
        LookupUnderAggregate shape = lookupUnderAggregate(Source.EMPTY, 10_000_000, 1_000, 100_000, false);

        RunReport report = explain(shape.aggregate(), shape.statistics(), Settings.EMPTY).report();

        DecisionReport join = report.decisions().get(1);
        assertEquals("PARTITIONED", join.chosen().name());
        AlternativeReport lookup = join.alternatives().get(0);
        assertEquals("LOOKUP", lookup.name());
        assertEquals(List.of("d1=COORDINATOR"), lookup.otherDecisions().stream().map(d -> d.id() + "=" + d.chosen()).toList());
        assertEquals("DISTRIBUTED", report.decisions().get(0).chosen().name());
        assertThat(report.explain(), containsString(" with d1=COORDINATOR\n"));
    }

    public void testAbortedSearchHasNoDecisions() {
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        // A balanced tree of 2047 joins evaluates more candidates than the budget while keeping the recursion shallow.
        LogicalPlan plan = bushyJoins(11, statistics);

        PlanDecisions decisions = explain(plan, statistics, Settings.EMPTY);

        RunReport report = decisions.report();
        assertEquals(RunStatus.ABORTED, report.status());
        assertThat(report.statusReason(), containsString("candidate budget [" + CostConstants.MAX_CANDIDATES + "] exceeded"));
        assertEquals(CostConstants.MAX_CANDIDATES, report.search().candidates());
        assertEquals(List.of(), report.decisions());
        assertEquals(Map.of(), decisions.joins());
    }

    public void testBasicLevelDoesNotCostAlternatives() {
        LookupUnderAggregate shape = lookupUnderAggregate(Source.EMPTY, 100_000_000, 50_000_000, 10_000_000, false);

        RunReport report = optimize(shape.aggregate(), shape.statistics(), Settings.EMPTY, ExplainLevel.BASIC).report();

        assertEquals(0, report.search().counterfactualSearches());
        DecisionReport aggregate = report.decisions().get(0);
        assertEquals(AlternativeStatus.REJECTED_FIT, aggregate.alternatives().get(0).status());
        assertNull(aggregate.alternatives().get(0).planCost());
        DecisionReport join = report.decisions().get(1);
        assertEquals(AlternativeStatus.NOT_COSTED, join.alternatives().get(0).status());
        assertEquals("PARTITIONED chosen by cost. LOOKUP was not costed.", join.summary());
    }

    public void testDecisionAndCounterfactualCaps() {
        int joins = ExplainBuilder.MAX_DECISIONS + 6;
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        Attribute key = attribute("key");
        LogicalPlan plan = relation("probe", key);
        statistics.put(plan, stats(1_000_000, 80_000_000, 80));
        for (int i = 0; i < joins; i++) {
            Attribute lookupKey = attribute("key");
            EsRelation lookup = relation("lookup_" + i, IndexMode.LOOKUP, lookupKey);
            statistics.put(lookup, stats(1_000, 100_000, 100));
            plan = new LookupJoin(Source.EMPTY, plan, lookup, JoinTypes.LEFT, List.of(key), List.of(lookupKey), null, ExecuteLocation.ANY);
            statistics.put(plan, stats(1_000_000, 80_000_000, 80));
        }

        RunReport report = explain(plan, statistics, Settings.EMPTY).report();

        assertEquals(ExplainBuilder.MAX_DECISIONS, report.decisions().size());
        assertEquals(6, report.truncatedDecisions());
        assertEquals(ExplainBuilder.MAX_COUNTERFACTUAL_SEARCHES, report.search().counterfactualSearches());
        DecisionReport last = report.decisions().get(ExplainBuilder.MAX_DECISIONS - 1);
        assertEquals(AlternativeStatus.NOT_COSTED, last.alternatives().get(0).status());
        assertThat(report.explain(), containsString("[not reported, decision cap reached]"));
    }

    public void testExplainTextIsTruncated() {
        Attribute leftKey = attribute("probe_key");
        Attribute rightKey = attribute("build_key");
        String longName = "x".repeat(ExplainBuilder.MAX_EXPLAIN_CHARS);
        SubqueryLookupJoin join = subqueryJoin(relation(longName, leftKey), relation("build", rightKey), leftKey, rightKey);
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        statistics.put(join.left(), stats(100_000_000, 8_000_000_000d, 80));
        statistics.put(join.right(), stats(100, 1_024, 16));

        RunReport report = explain(join, statistics, Settings.EMPTY).report();

        assertEquals(ExplainBuilder.MAX_EXPLAIN_CHARS, report.explain().length());
        assertThat(report.explain(), endsWith(ExplainBuilder.TRUNCATED_MARKER));
    }

    /**
     * A partitioned lookup-index join must read its probe side from a scan fragment. On its own the inner join prefers BROADCAST,
     * which keeps the probe partitioned by source but runs outside any fragment, so the search must keep the costlier MATERIALIZED
     * inner plan, which folds into the fragment, for the outer join to be partitioned.
     */
    public void testPartitionedLookupJoinKeepsInnerJoinInScanFragment() {
        Attribute key = attribute("key");
        Attribute buildKey = attribute("build_key");
        Attribute lookupKey = attribute("key");
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        EsRelation probe = relation("probe", key);
        statistics.put(probe, stats(1_000_000, 80_000_000, 80, key, Estimate.exact(1_000_000)));
        EsRelation build = relation("build", buildKey);
        statistics.put(build, stats(100_000, 8_000_000, 80, buildKey, Estimate.exact(100_000)));
        SubqueryLookupJoin inner = subqueryJoin(probe, build, key, buildKey);
        statistics.put(inner, stats(1_000_000, 100_000_000, 100, key, Estimate.exact(1_000_000)));
        EsRelation lookup = relation("lookup", IndexMode.LOOKUP, lookupKey);
        statistics.put(lookup, stats(1_000, 100_000, 100));
        LookupJoin outer = new LookupJoin(
            Source.EMPTY,
            inner,
            lookup,
            JoinTypes.LEFT,
            List.of(key),
            List.of(lookupKey),
            null,
            ExecuteLocation.ANY
        );
        statistics.put(outer, stats(1_000_000, 120_000_000, 120, key, Estimate.exact(1_000_000)));

        assertEquals(JoinStrategy.BROADCAST, explain(inner, statistics, Settings.EMPTY).join(inner).orElseThrow());
        PlanDecisions decisions = explain(outer, statistics, Settings.EMPTY);
        assertEquals(JoinStrategy.PARTITIONED, decisions.join(outer).orElseThrow());
        assertEquals(JoinStrategy.MATERIALIZED, decisions.join(inner).orElseThrow());
    }

    /**
     * Score ties are not transitive, so each group keeps its exact lowest-cost plan next to the one the tie-break rules prefer.
     * Keeping only the preferred plan lets tie margins compound across the two joins until an alternative beats the chosen plan by
     * more than the tie margin.
     */
    public void testTieMarginsDoNotCompoundAcrossJoins() {
        Attribute key = attribute("key");
        Attribute lookupKey = attribute("key");
        Attribute buildKey = attribute("key");
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        EsRelation probe = relation("probe", key);
        statistics.put(probe, stats(100_000, 2_900_000, 29, key, Estimate.exact(10)));
        EsRelation lookup = relation("lookup", IndexMode.LOOKUP, lookupKey);
        statistics.put(lookup, stats(100_000, 13_200_000, 132, lookupKey, Estimate.UNKNOWN));
        LookupJoin lookupJoin = new LookupJoin(
            Source.EMPTY,
            probe,
            lookup,
            JoinTypes.LEFT,
            List.of(key),
            List.of(lookupKey),
            null,
            ExecuteLocation.ANY
        );
        statistics.put(lookupJoin, stats(Estimate.UNKNOWN, Estimate.UNKNOWN, Estimate.UNKNOWN));
        EsRelation build = relation("build", buildKey);
        statistics.put(build, stats(10_000, 1_730_000, 173, buildKey, Estimate.UNKNOWN));
        SubqueryLookupJoin join = subqueryJoin(lookupJoin, build, key, buildKey);
        statistics.put(join, stats(10_000, 770_000, 77, key, Estimate.UNKNOWN));

        PlanDecisions decisions = explain(join, statistics, Settings.EMPTY);

        assertEquals(JoinStrategy.BROADCAST, decisions.join(join).orElseThrow());
        assertEquals(JoinStrategy.LOOKUP, decisions.join(lookupJoin).orElseThrow());
        for (DecisionReport decision : decisions.report().decisions()) {
            assertFalse(decision.id(), decision.searchAnomaly());
        }
    }

    /** I9 and every structural invariant over random plan shapes, statistics, and join pragmas. */
    public void testRandomizedInvariants() {
        for (int i = 0; i < 25; i++) {
            Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
            LogicalPlan plan = randomPlan(randomIntBetween(1, 3), statistics);
            Settings pragmas = randomBoolean() ? Settings.EMPTY : pragmas(QueryPragmas.SHUFFLE_JOIN.getKey(), true);

            RunReport report = explain(plan, statistics, pragmas).report();

            if (report != null) {
                for (DecisionReport decision : report.decisions()) {
                    assertFalse(Strings.toString(report, true, true), decision.searchAnomaly());
                }
            }
        }
    }

    /**
     * Runs the search at every explain level and checks the invariants that hold for any plan. Returns the {@code FULL} result.
     */
    private static PlanDecisions explain(LogicalPlan plan, Map<LogicalPlan, PlanStats> statistics, Settings pragmas) {
        PlanDecisions none = optimize(plan, statistics, pragmas, ExplainLevel.NONE);
        PlanDecisions basic = optimize(plan, statistics, pragmas, ExplainLevel.BASIC);
        PlanDecisions full = optimize(plan, statistics, pragmas, ExplainLevel.FULL);

        assertNull(none.report());
        // I10: the explain level never changes a decision.
        assertEquals(none.joins(), basic.joins());
        assertEquals(none.joins(), full.joins());
        assertEquals(none.aggregates(), basic.aggregates());
        assertEquals(none.aggregates(), full.aggregates());
        // I8
        if (basic.report().search() != null) {
            assertEquals(0, basic.report().search().counterfactualSearches());
        }
        assertInvariants(plan, basic);
        assertInvariants(plan, full);
        return full;
    }

    private static void assertInvariants(LogicalPlan plan, PlanDecisions decisions) {
        RunReport report = decisions.report();
        assertCostVectorsComplete(report);
        if (report.status() != RunStatus.OK) {
            assertEquals(List.of(), report.decisions());
            assertEquals(List.of(), decisions.points());
            return;
        }
        assertEquals(report.decisions().size(), decisions.points().size());
        List<LogicalPlan> preOrder = new ArrayList<>();
        plan.forEachDown(node -> {
            if (CostBasedOptimizer.kindOf(node) != null) {
                preOrder.add(node);
            }
        });
        int previous = -1;
        List<CostComponent> chosenComponents = new ArrayList<>();
        for (int i = 0; i < report.decisions().size(); i++) {
            DecisionReport decision = report.decisions().get(i);
            PlanDecisions.DecisionPoint point = decisions.points().get(i);
            // I6: ids are unique, sequential, and follow pre-order.
            assertEquals("d" + (i + 1), decision.id());
            assertEquals(decision.id(), point.id());
            assertEquals(decision.chosen().name(), point.chosen());
            assertEquals(point.kind().name(), decision.kind());
            // Stacked nodes can share a key, so match each point to the next node in pre-order with its key.
            int position = indexOfKey(preOrder, point, previous + 1);
            previous = position;
            assertChosenMatchesDecision(decisions, decision, point);

            // I1: chosen, alternatives, and exclusions partition the domain.
            List<String> names = new ArrayList<>();
            names.add(decision.chosen().name());
            decision.alternatives().forEach(a -> names.add(a.name()));
            decision.excluded().forEach(e -> names.add(e.name()));
            assertEquals(decision.id() + " names overlap: " + names, names.size(), new HashSet<>(names).size());
            assertEquals(new HashSet<>(point.kind().domain(preOrder.get(position))), new HashSet<>(names));

            for (AlternativeReport alternative : decision.alternatives()) {
                assertAlternative(report, decision, alternative);
            }
            chosenComponents.addAll(decision.chosen().components());
        }
        // I4: decisions own disjoint parts of the plan, so their components never add up to more than it.
        assertComponentsWithin(report.planCost(), chosenComponents);
    }

    private static void assertAlternative(RunReport report, DecisionReport decision, AlternativeReport alternative) {
        String context = decision.id() + "/" + alternative.name() + "\n" + Strings.toString(report, true, true);
        if (alternative.planCost() == null) {
            assertNull(context, alternative.delta());
            assertNull(context, alternative.tieBreak());
            return;
        }
        // I2
        double base = report.planCost().score();
        double score = alternative.planCost().score();
        assertEquals(context, score - base, alternative.delta().score(), 1e-9 * Math.max(1, Math.abs(base)));
        // I3
        double gap = Math.abs(score - base) / Math.max(Math.max(Math.abs(score), Math.abs(base)), 1e-12);
        switch (alternative.status()) {
            case LOST_ON_COST -> {
                assertThat(context, score, greaterThan(base));
                assertThat(context, gap, greaterThan(CostConstants.TIE_EPSILON));
                assertNull(context, alternative.tieBreak());
            }
            case LOST_ON_TIE -> {
                assertThat(context, gap, lessThanOrEqualTo(CostConstants.TIE_EPSILON));
                assertNotNull(context, alternative.tieBreak());
            }
            case REJECTED_FIT -> assertNull(context, alternative.tieBreak());
            case NO_PLAN, NOT_COSTED -> fail(context + " has a plan cost but status " + alternative.status());
        }
        assertComponentsWithin(alternative.planCost(), alternative.components());
    }

    private static void assertChosenMatchesDecision(PlanDecisions decisions, DecisionReport decision, PlanDecisions.DecisionPoint point) {
        Map<?, ?> published = switch (point.key()) {
            case JoinKey key -> decisions.joins();
            case AggKey key -> decisions.aggregates();
            default -> throw new AssertionError("unexpected decision key " + point.key());
        };
        if (decision.outcome() != null && decision.outcome().status() == OutcomeStatus.CONFLICT) {
            assertFalse(decision.id(), published.containsKey(point.key()));
        } else {
            assertEquals(decision.id(), point.chosen(), ((Enum<?>) published.get(point.key())).name());
        }
    }

    private static int indexOfKey(List<LogicalPlan> preOrder, PlanDecisions.DecisionPoint point, int from) {
        for (int i = from; i < preOrder.size(); i++) {
            LogicalPlan node = preOrder.get(i);
            if (CostBasedOptimizer.kindOf(node) == point.kind() && point.kind().key(node).equals(point.key())) {
                return i;
            }
        }
        throw new AssertionError("no plan node in pre-order for decision " + point.id());
    }

    private static void assertComponentsWithin(CostVector planCost, List<CostComponent> components) {
        CostVector sum = new CostVector(0, 0, 0, 0, 0, 0);
        for (CostComponent component : components) {
            sum = sum.plus(component.cost());
        }
        double tolerance = 1 + 1e-9;
        assertThat(sum.latencyNanos(), lessThanOrEqualTo(planCost.latencyNanos() * tolerance));
        assertThat(sum.workUnits(), lessThanOrEqualTo(planCost.workUnits() * tolerance));
        assertThat(sum.networkInBytes(), lessThanOrEqualTo(planCost.networkInBytes() * tolerance));
        assertThat(sum.coordinatorInBytes(), lessThanOrEqualTo(planCost.coordinatorInBytes() * tolerance));
        assertThat(sum.peakNodeInBytes(), lessThanOrEqualTo(planCost.peakNodeInBytes() * tolerance));
        assertThat(sum.peakLaneInBytes(), lessThanOrEqualTo(planCost.peakLaneInBytes() * tolerance));
    }

    /** I5: every rendered cost vector carries all of its fields. */
    private static void assertCostVectorsComplete(RunReport report) {
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(report), false);
        walk(map);
    }

    private static void walk(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (map.containsKey("latency_nanos")) {
                Set<String> keys = new HashSet<>();
                map.keySet().forEach(k -> keys.add((String) k));
                assertTrue("incomplete cost vector " + map, keys.containsAll(COST_VECTOR_FIELDS));
            }
            map.values().forEach(CostBasedOptimizerExplainTests::walk);
        } else if (value instanceof List<?> list) {
            list.forEach(CostBasedOptimizerExplainTests::walk);
        }
    }

    /** Replaces the cost numbers in explain text, which move whenever a cost constant is tuned, with placeholders. */
    private static String withoutNumbers(String explain) {
        return explain.replaceAll("(?m)^plan score=.*$", "plan <cost>").replaceAll("score=[-0-9.]+ \\([-+][0-9.]+%\\)", "score=<score>");
    }

    private record LookupUnderAggregate(
        Aggregate aggregate,
        LookupJoin join,
        Attribute probeKey,
        Attribute lookupKey,
        Map<LogicalPlan, PlanStats> statistics
    ) {}

    /**
     * {@code STATS BY key} over {@code probe | LOOKUP JOIN customers ON key}, optionally with a filter on the probe side so the
     * explain text has a collapsed node.
     */
    private static LookupUnderAggregate lookupUnderAggregate(
        Source joinSource,
        double probeRows,
        double buildRows,
        double groups,
        boolean filterProbe
    ) {
        Attribute key = attribute("key");
        Attribute lookupKey = attribute("key");
        Map<LogicalPlan, PlanStats> statistics = new IdentityHashMap<>();
        EsRelation probe = relation("probe", key);
        PlanStats probeStats = stats(probeRows, probeRows * 80, 80, key, Estimate.exact(groups));
        statistics.put(probe, probeStats);
        LogicalPlan left = probe;
        if (filterProbe) {
            left = new Filter(Source.EMPTY, probe, Literal.TRUE);
            statistics.put(left, probeStats);
        }
        EsRelation lookup = relation("customers", IndexMode.LOOKUP, lookupKey);
        statistics.put(lookup, stats(buildRows, buildRows * 100, 100));
        LookupJoin join = new LookupJoin(
            joinSource,
            left,
            lookup,
            JoinTypes.LEFT,
            List.of(key),
            List.of(lookupKey),
            null,
            ExecuteLocation.ANY
        );
        statistics.put(join, stats(probeRows, probeRows * 100, 100, key, Estimate.exact(groups)));
        Aggregate aggregate = new Aggregate(Source.EMPTY, join, List.of(key), List.of(key));
        return new LookupUnderAggregate(aggregate, join, key, lookupKey, statistics);
    }

    private static LogicalPlan bushyJoins(int depth, Map<LogicalPlan, PlanStats> statistics) {
        if (depth == 0) {
            EsRelation relation = relation("leaf", attribute("key"));
            statistics.put(relation, stats(1_000_000, 100_000_000, 100));
            return relation;
        }
        LogicalPlan left = bushyJoins(depth - 1, statistics);
        LogicalPlan right = bushyJoins(depth - 1, statistics);
        SubqueryLookupJoin join = subqueryJoin(left, right, left.output().get(0), right.output().get(0));
        statistics.put(join, stats(1_000_000, 100_000_000, 100));
        return join;
    }

    /** Builds a random tree of sources, lookup joins, subquery joins, and grouped aggregates with random estimates. */
    private static LogicalPlan randomPlan(int depth, Map<LogicalPlan, PlanStats> statistics) {
        if (depth == 0) {
            Attribute key = attribute("key");
            EsRelation relation = relation("index", key);
            statistics.put(relation, randomStats(key));
            return relation;
        }
        LogicalPlan child = randomPlan(depth - 1, statistics);
        Attribute key = child.output().get(0);
        LogicalPlan plan = switch (randomIntBetween(0, 2)) {
            case 0 -> {
                Attribute lookupKey = attribute("key");
                EsRelation lookup = relation("lookup", IndexMode.LOOKUP, lookupKey);
                statistics.put(lookup, randomStats(lookupKey));
                yield new LookupJoin(
                    Source.EMPTY,
                    child,
                    lookup,
                    JoinTypes.LEFT,
                    List.of(key),
                    List.of(lookupKey),
                    null,
                    ExecuteLocation.ANY
                );
            }
            case 1 -> {
                LogicalPlan right = randomPlan(randomIntBetween(0, depth - 1), statistics);
                yield subqueryJoin(child, right, key, right.output().get(0));
            }
            case 2 -> new Aggregate(Source.EMPTY, child, List.of(key), List.of(key));
            default -> throw new AssertionError("unexpected plan shape");
        };
        statistics.put(plan, randomStats(plan.output().get(0)));
        return plan;
    }

    private static PlanStats randomStats(Attribute key) {
        double rows = Math.pow(10, randomIntBetween(2, 9));
        double rowBytes = randomIntBetween(16, 200);
        Estimate ndv = randomBoolean() ? Estimate.UNKNOWN : Estimate.exact(Math.max(1, rows / Math.pow(10, randomIntBetween(0, 6))));
        if (randomIntBetween(0, 4) == 0) {
            return stats(Estimate.UNKNOWN, Estimate.UNKNOWN, Estimate.UNKNOWN);
        }
        return stats(rows, rows * rowBytes, rowBytes, key, ndv);
    }

    private static PlanDecisions optimize(LogicalPlan plan, Map<LogicalPlan, PlanStats> statistics, Settings pragmas, ExplainLevel level) {
        Configuration configuration = EsqlTestUtils.configuration(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.CBO.getKey(), true)
                    .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
                    .put(pragmas)
                    .build()
            )
        );
        CboContext context = new CboContext(configuration, TransportVersion.current(), 4, MATERIALIZE_LIMIT_BYTES);
        return new CostBasedOptimizer().optimizeWithStatistics(plan, context, statistics, level, RunInfo.INITIAL);
    }

    private static Settings pragmas(String key, Object value) {
        return Settings.builder().put(key, value.toString()).build();
    }

    private static List<String> failedChecks(AlternativeReport alternative) {
        return alternative.fitChecks().stream().filter(check -> check.passed() == false).map(check -> check.check()).toList();
    }

    private static String component(CostTerm term, Side side) {
        return side == null ? term.name() : term.name() + "/" + side.name();
    }

    private static List<String> terms(List<CostComponent> components) {
        return components.stream().map(c -> component(c.term(), c.side())).toList();
    }

    private static SubqueryLookupJoin subqueryJoin(LogicalPlan left, LogicalPlan right, Attribute leftKey, Attribute rightKey) {
        return new SubqueryLookupJoin(
            Source.EMPTY,
            left,
            right,
            JoinTypes.LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );
    }

    private static EsRelation relation(String name, Attribute key) {
        return relation(name, IndexMode.STANDARD, key);
    }

    private static EsRelation relation(String name, IndexMode mode, Attribute key) {
        return new EsRelation(Source.EMPTY, name, mode, Map.of(), Map.of(), Map.of(), List.of(key));
    }

    private static Attribute attribute(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.LONG);
    }

    private static PlanStats stats(double rows, double bytes, double avgRowBytes) {
        return stats(Estimate.exact(rows), Estimate.exact(bytes), Estimate.exact(avgRowBytes));
    }

    private static PlanStats stats(Estimate rows, Estimate bytes, Estimate avgRowBytes) {
        return new PlanStats(rows, avgRowBytes, Map.of(), bytes);
    }

    private static PlanStats stats(double rows, double bytes, double avgRowBytes, Attribute attribute, Estimate ndv) {
        return new PlanStats(
            Estimate.exact(rows),
            Estimate.exact(avgRowBytes),
            Map.of(
                attribute.id(),
                new ColumnStats(ndv, Estimate.UNKNOWN, Optional.empty(), Optional.empty(), Estimate.UNKNOWN, Estimate.UNKNOWN)
            ),
            Estimate.exact(bytes)
        );
    }
}
