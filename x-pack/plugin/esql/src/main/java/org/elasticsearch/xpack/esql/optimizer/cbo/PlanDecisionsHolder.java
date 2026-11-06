/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.optimizer.cbo.DecisionKind.Observation;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OptimizerProfile;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Outcome;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OutcomeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Trigger;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-query planning state for the cost-based optimizer. Each optimizer run publishes its decisions here, the mapper and the
 * physical optimizer read the latest run, consumers record how they used each decision, and the session observes the physical
 * plans the run produced. {@link #snapshot()} merges the records into the outcome of every reported decision.
 */
public final class PlanDecisionsHolder {
    /** Runs beyond this many are counted in {@link OptimizerProfile#truncatedRuns()} instead of reported. */
    public static final int MAX_RUNS = 16;

    static final String NO_MATCH = "no physical node matched the decision";

    private final List<Run> runs = new ArrayList<>();
    private volatile Run current = new Run(PlanDecisions.EMPTY);
    private int runCount;
    private int truncatedRuns;
    private int subplans;

    /** Reads the decisions of the latest run. */
    public PlanDecisions get() {
        return current.decisions;
    }

    /** Records how a consumer used a decision of the latest run. See {@link DecisionLedger#record}. */
    public void record(DecisionKind kind, Object key, String decided, String effective, @Nullable String overrideReason) {
        current.ledger.record(kind, key, decided, effective, overrideReason);
    }

    /** Identifies the next run, so its report carries the ordinal and the subplan that triggered it. */
    public synchronized RunInfo nextRun(Trigger trigger) {
        return new RunInfo(runCount, trigger, trigger == Trigger.AFTER_SUBPLAN ? subplans : null);
    }

    /** Publishes the decisions of a new run. Runs without a report are applied but not reported. */
    public synchronized void startRun(PlanDecisions decisions) {
        Run run = new Run(Objects.requireNonNull(decisions));
        runCount++;
        current = run;
        if (decisions.report() == null) {
            return;
        }
        if (runs.size() < MAX_RUNS) {
            runs.add(run);
        } else {
            truncatedRuns++;
        }
    }

    /** Counts a subplan about to execute and returns its 1-based ordinal. */
    public synchronized int startSubPlan() {
        return ++subplans;
    }

    /**
     * Looks for every decision of the latest run in {@code plan}. The first plan that contains a decision's node wins.
     *
     * @param where prefixed to the observation's location, for example {@code subplan 1}, or {@code null} for the main plan
     */
    public synchronized void observe(PhysicalPlan plan, @Nullable String where) {
        Run run = current;
        if (run.decisions.report() == null) {
            return;
        }
        for (PlanDecisions.DecisionPoint point : run.decisions.points()) {
            if (run.observations.containsKey(point.id())) {
                continue;
            }
            Optional<Observation> found = point.kind().observe(plan, point.key());
            found.ifPresent(o -> {
                String observedIn = where == null ? o.observedIn() : where + ", " + o.observedIn();
                run.observations.put(point.id(), new Observation(o.effective(), o.physicalNode(), observedIn));
            });
        }
    }

    /** Records an observation made outside a physical plan, for example a subquery join executed as a subplan. */
    public synchronized void observeExternal(DecisionKind kind, Object key, Observation observation) {
        Run run = current;
        for (PlanDecisions.DecisionPoint point : run.decisions.points()) {
            if (point.kind().name().equals(kind.name()) && point.key().equals(key)) {
                run.observations.putIfAbsent(point.id(), observation);
            }
        }
    }

    /** The reported runs with outcomes attached, or {@code null} if no run was reported. */
    @Nullable
    public synchronized OptimizerProfile snapshot() {
        if (runs.isEmpty() && truncatedRuns == 0) {
            return null;
        }
        int lastOrdinal = runCount - 1;
        List<RunReport> reports = new ArrayList<>(runs.size());
        for (Run run : runs) {
            RunReport report = run.decisions.report();
            String notObserved = report.run() < lastOrdinal
                ? NO_MATCH + "; run " + (report.run() + 1) + " re-optimized the plan before the node ran"
                : NO_MATCH;
            Map<String, PlanDecisions.DecisionPoint> points = new HashMap<>();
            for (PlanDecisions.DecisionPoint point : run.decisions.points()) {
                points.put(point.id(), point);
            }
            List<DecisionReport> decisions = new ArrayList<>(report.decisions().size());
            for (DecisionReport decision : report.decisions()) {
                PlanDecisions.DecisionPoint point = points.get(decision.id());
                if (point == null || decision.outcome() != null && decision.outcome().status() == OutcomeStatus.CONFLICT) {
                    decisions.add(decision);
                    continue;
                }
                Outcome outcome = outcome(
                    point.chosen(),
                    run.ledger.find(point.kind(), point.key()),
                    run.observations.get(point.id()),
                    notObserved
                );
                decisions.add(withOutcome(decision, outcome));
            }
            reports.add(report.withDecisions(decisions));
        }
        return new OptimizerProfile(OptimizerProfile.COST_MODEL, reports, truncatedRuns);
    }

    /**
     * Merges the consumer record and the observation of the physical plan into an outcome. The two are recorded independently,
     * so a bug in either shows up as {@link OutcomeStatus#MISMATCH}.
     */
    static Outcome outcome(
        String chosen,
        @Nullable DecisionLedger.Entry entry,
        @Nullable Observation observation,
        String notObservedReason
    ) {
        if (observation == null) {
            if (entry == null) {
                return new Outcome(OutcomeStatus.NOT_OBSERVED, null, notObservedReason, null, null);
            }
            String reason = notObservedReason + "; planning used " + entry.effective();
            if (entry.overrideReason() != null) {
                reason += " because " + entry.overrideReason();
            }
            return new Outcome(OutcomeStatus.NOT_OBSERVED, effective(chosen, entry.effective()), reason, null, null);
        }
        String ran = observation.effective();
        if (entry == null) {
            return chosen.equals(ran)
                ? observed(OutcomeStatus.APPLIED, chosen, ran, null, observation)
                : observed(
                    OutcomeStatus.MISMATCH,
                    chosen,
                    ran,
                    "the plan runs " + ran + " and the planner recorded no override",
                    observation
                );
        }
        if (entry.decided().equals(chosen) == false) {
            String reason = "the planner read " + entry.decided() + " instead of the published " + chosen;
            return observed(OutcomeStatus.MISMATCH, chosen, ran, reason, observation);
        }
        if (entry.effective().equals(ran) == false) {
            String reason = "the planner recorded " + entry.effective() + " but the plan runs " + ran;
            return observed(OutcomeStatus.MISMATCH, chosen, ran, reason, observation);
        }
        if (entry.overrideReason() != null) {
            return observed(OutcomeStatus.OVERRIDDEN, chosen, ran, entry.overrideReason(), observation);
        }
        return chosen.equals(ran)
            ? observed(OutcomeStatus.APPLIED, chosen, ran, null, observation)
            : observed(OutcomeStatus.MISMATCH, chosen, ran, "the planner used " + ran + " without recording a reason", observation);
    }

    /** Attaches the outcome and, for overrides and mismatches, appends a sentence to the summary. */
    static DecisionReport withOutcome(DecisionReport decision, Outcome outcome) {
        DecisionReport withOutcome = decision.withOutcome(outcome);
        String ran = outcome.effective() == null ? decision.chosen().name() : outcome.effective();
        return switch (outcome.status()) {
            case OVERRIDDEN -> withOutcome.withSummary(
                decision.summary() + " Overridden at planning: " + outcome.reason() + "; ran " + ran + "."
            );
            case MISMATCH -> withOutcome.withSummary(decision.summary() + " Mismatch: plan ran " + ran + ".");
            case APPLIED, NOT_OBSERVED, CONFLICT -> withOutcome;
        };
    }

    private static Outcome observed(OutcomeStatus status, String chosen, String ran, @Nullable String reason, Observation observation) {
        return new Outcome(status, effective(chosen, ran), reason, observation.physicalNode(), observation.observedIn());
    }

    @Nullable
    private static String effective(String chosen, String effective) {
        return chosen.equals(effective) ? null : effective;
    }

    private static final class Run {
        private final PlanDecisions decisions;
        private final DecisionLedger ledger = new DecisionLedger();
        private final Map<String, Observation> observations = new HashMap<>();

        private Run(PlanDecisions decisions) {
            this.decisions = decisions;
        }
    }
}
