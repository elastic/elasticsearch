/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.optimizer.cbo.DecisionKind.Observation;
import org.elasticsearch.xpack.esql.optimizer.cbo.DecisionLedger.Entry;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.ChosenReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionMode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OptimizerProfile;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Outcome;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OutcomeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Trigger;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PlanDecisionsHolderTests extends ESTestCase {
    private static final String NO_MATCH = PlanDecisionsHolder.NO_MATCH;
    private static final Observation RAN_PARTITIONED = new Observation("PARTITIONED", "join", "coordinator plan");
    private static final Observation RAN_LOOKUP = new Observation("LOOKUP", "join", "data node fragment");

    public void testOutcomeMergeRule() {
        assertOutcome(outcome(null, null), OutcomeStatus.NOT_OBSERVED, null, NO_MATCH);
        assertOutcome(
            outcome(new Entry("PARTITIONED", "LOOKUP", "no scan fragment"), null),
            OutcomeStatus.NOT_OBSERVED,
            "LOOKUP",
            NO_MATCH + "; planning used LOOKUP because no scan fragment"
        );
        assertOutcome(outcome(null, RAN_PARTITIONED), OutcomeStatus.APPLIED, null, null);
        assertOutcome(
            outcome(null, RAN_LOOKUP),
            OutcomeStatus.MISMATCH,
            "LOOKUP",
            "the plan runs LOOKUP and the planner recorded no override"
        );
        assertOutcome(outcome(new Entry("PARTITIONED", "PARTITIONED", null), RAN_PARTITIONED), OutcomeStatus.APPLIED, null, null);
        assertOutcome(
            outcome(new Entry("PARTITIONED", "LOOKUP", "no scan fragment"), RAN_LOOKUP),
            OutcomeStatus.OVERRIDDEN,
            "LOOKUP",
            "no scan fragment"
        );
        assertOutcome(
            outcome(new Entry("LOOKUP", "LOOKUP", null), RAN_LOOKUP),
            OutcomeStatus.MISMATCH,
            "LOOKUP",
            "the planner read LOOKUP instead of the published PARTITIONED"
        );
        assertOutcome(
            outcome(new Entry("PARTITIONED", "LOOKUP", "no scan fragment"), RAN_PARTITIONED),
            OutcomeStatus.MISMATCH,
            null,
            "the planner recorded LOOKUP but the plan runs PARTITIONED"
        );
        assertOutcome(
            outcome(new Entry("PARTITIONED", "LOOKUP", null), RAN_LOOKUP),
            OutcomeStatus.MISMATCH,
            "LOOKUP",
            "the planner used LOOKUP without recording a reason"
        );
    }

    public void testSnapshotAttachesOutcomes() {
        PlanDecisionsHolder holder = new PlanDecisionsHolder();
        JoinKey applied = randomKey();
        JoinKey overridden = randomKey();
        JoinKey mismatched = randomKey();
        JoinKey conflicting = randomKey();
        DecisionReport conflict = report("d4", "PARTITIONED", new Outcome(OutcomeStatus.CONFLICT, null, "conflict", null, null));
        holder.startRun(
            run(
                holder.nextRun(Trigger.INITIAL),
                List.of(applied, overridden, mismatched, conflicting),
                List.of(report("d1", "PARTITIONED", null), report("d2", "PARTITIONED", null), report("d3", "PARTITIONED", null), conflict)
            )
        );
        holder.record(JoinStrategyDecisionKind.INSTANCE, applied, "PARTITIONED", "PARTITIONED", null);
        holder.record(JoinStrategyDecisionKind.INSTANCE, overridden, "PARTITIONED", "LOOKUP", "no scan fragment");
        holder.observeExternal(JoinStrategyDecisionKind.INSTANCE, applied, RAN_PARTITIONED);
        holder.observeExternal(JoinStrategyDecisionKind.INSTANCE, overridden, RAN_LOOKUP);
        holder.observeExternal(JoinStrategyDecisionKind.INSTANCE, mismatched, RAN_LOOKUP);
        holder.observeExternal(JoinStrategyDecisionKind.INSTANCE, conflicting, RAN_LOOKUP);
        holder.observeExternal(AggregateDistributionDecisionKind.INSTANCE, applied, new Observation("COORDINATOR", "agg", "plan"));

        OptimizerProfile profile = holder.snapshot();
        assertEquals(1, profile.runs().size());
        List<DecisionReport> decisions = profile.runs().get(0).decisions();

        assertEquals(OutcomeStatus.APPLIED, decisions.get(0).outcome().status());
        assertEquals("join", decisions.get(0).outcome().physicalNode());
        assertEquals("coordinator plan", decisions.get(0).outcome().observedIn());
        assertEquals("PARTITIONED chosen by cost.", decisions.get(0).summary());

        assertEquals(OutcomeStatus.OVERRIDDEN, decisions.get(1).outcome().status());
        assertEquals("PARTITIONED chosen by cost. Overridden at planning: no scan fragment; ran LOOKUP.", decisions.get(1).summary());

        assertEquals(OutcomeStatus.MISMATCH, decisions.get(2).outcome().status());
        assertEquals("PARTITIONED chosen by cost. Mismatch: plan ran LOOKUP.", decisions.get(2).summary());

        assertEquals(conflict, decisions.get(3));
    }

    public void testRunsAcrossSubplans() {
        PlanDecisionsHolder holder = new PlanDecisionsHolder();
        assertNull(holder.snapshot());
        holder.startRun(PlanDecisions.EMPTY);
        assertNull(holder.snapshot());

        JoinKey key = randomKey();
        RunInfo first = holder.nextRun(Trigger.INITIAL);
        holder.startRun(run(first, List.of(key), List.of(report("d1", "PARTITIONED", null))));
        assertEquals(1, holder.startSubPlan());
        RunInfo second = holder.nextRun(Trigger.AFTER_SUBPLAN);
        assertEquals(new RunInfo(2, Trigger.AFTER_SUBPLAN, 1), second);
        holder.startRun(run(second, List.of(key), List.of(report("d1", "PARTITIONED", null))));
        holder.observeExternal(JoinStrategyDecisionKind.INSTANCE, key, RAN_PARTITIONED);

        List<RunReport> runs = holder.snapshot().runs();
        assertEquals(2, runs.size());
        Outcome superseded = runs.get(0).decisions().get(0).outcome();
        assertEquals(OutcomeStatus.NOT_OBSERVED, superseded.status());
        assertEquals(NO_MATCH + "; run 2 re-optimized the plan before the node ran", superseded.reason());
        assertEquals(OutcomeStatus.APPLIED, runs.get(1).decisions().get(0).outcome().status());

        int extra = between(1, 3);
        for (int i = 0; i < PlanDecisionsHolder.MAX_RUNS - 2 + extra; i++) {
            holder.startRun(run(holder.nextRun(Trigger.AFTER_SUBPLAN), List.of(), List.of()));
        }
        OptimizerProfile profile = holder.snapshot();
        assertEquals(PlanDecisionsHolder.MAX_RUNS, profile.runs().size());
        assertEquals(extra, profile.truncatedRuns());
    }

    private static Outcome outcome(@Nullable Entry entry, @Nullable Observation observation) {
        return PlanDecisionsHolder.outcome("PARTITIONED", entry, observation, NO_MATCH);
    }

    private static void assertOutcome(Outcome outcome, OutcomeStatus status, @Nullable String effective, @Nullable String reason) {
        assertEquals(status, outcome.status());
        assertEquals(effective, outcome.effective());
        assertEquals(reason, outcome.reason());
    }

    private static JoinKey randomKey() {
        return new JoinKey(List.of(new NameId()), List.of(new NameId()));
    }

    private static PlanDecisions run(RunInfo info, List<JoinKey> keys, List<DecisionReport> decisions) {
        List<PlanDecisions.DecisionPoint> points = new ArrayList<>();
        for (int i = 0; i < decisions.size(); i++) {
            DecisionReport decision = decisions.get(i);
            points.add(
                new PlanDecisions.DecisionPoint(decision.id(), JoinStrategyDecisionKind.INSTANCE, keys.get(i), decision.chosen().name())
            );
        }
        RunReport report = new RunReport(
            info.ordinal(),
            info.trigger(),
            info.subplan(),
            RunStatus.OK,
            null,
            null,
            null,
            decisions,
            0,
            null
        );
        return new PlanDecisions(Map.of(), Map.of(), report, points);
    }

    private static DecisionReport report(String id, String chosen, @Nullable Outcome outcome) {
        return new DecisionReport(
            id,
            JoinStrategyDecisionKind.NAME,
            "LookupJoin",
            null,
            chosen + " chosen by cost.",
            DecisionMode.COST_BASED,
            new ChosenReport(chosen, "source", List.of(), List.of()),
            outcome,
            "any",
            List.of(),
            Provenance.METADATA,
            List.of(),
            List.of(),
            List.of(),
            false,
            null
        );
    }
}
