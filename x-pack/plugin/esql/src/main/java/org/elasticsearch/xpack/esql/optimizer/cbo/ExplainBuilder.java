/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Alternative;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.AnyRequirement;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.ArbitraryPartitioning;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.BroadcastRequirement;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.CostConstants;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Decided;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Delivered;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Enforcer;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Enumeration;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.HashPartitioning;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.HashRequirement;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.OperatorKind;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Part;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Requirement;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Search;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.SearchAborted;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.SinglePartitioning;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.SingleRequirement;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.SourcePartitioning;
import org.elasticsearch.xpack.esql.optimizer.cbo.CostBasedOptimizer.Winner;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.AlternativeReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.AlternativeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.ChosenReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostComponent;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostTerm;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.CostVector;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionMode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionRef;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.DecisionReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Delta;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Exclusion;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.ExclusionCode;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.FitCheck;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.FitMethod;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.NamedEstimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Outcome;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.OutcomeStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunReport;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunStatus;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.SearchStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.Side;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.SourceAnchor;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.TieBreak;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.TieBreakRule;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Provenance;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Explains a solved search: which decision points the chosen plan has, what each one chose and why, and what each other
 * enumerated alternative would have cost. Only reads the search, so the published decisions never depend on the explain level.
 */
final class ExplainBuilder {
    private static final Logger logger = LogManager.getLogger(ExplainBuilder.class);

    /** Bounds the extra planning time of {@link ExplainLevel#FULL}; alternatives beyond it are reported as not costed. */
    static final int MAX_COUNTERFACTUAL_SEARCHES = 32;
    static final int MAX_DECISIONS = 64;
    static final int MAX_EXPLAIN_CHARS = 16 * 1024;
    static final String TRUNCATED_MARKER = "...[truncated]";
    private static final int MAX_SOURCE_TEXT_CHARS = 80;
    private static final String NOT_FINITE_WARNING = "a cost estimate is not finite and is reported as null";
    private static final String CONFLICT_REASON = "another decision point with the same key chose differently, so neither was published";

    private final Search search;
    private final Winner best;
    private final Decided decided;
    private final ExplainLevel level;
    private final RunInfo run;
    private final long startNanos;
    private final CostVector planCost;
    private final List<Point> points;
    private final Map<Winner, Point> pointsByWinner = new IdentityHashMap<>();
    private int counterfactualSearches;

    /**
     * A decision point of the chosen plan.
     *
     * @param parent the winner that consumes this point's output, or {@code null} at the root
     * @param childIndex the index of this point among {@code parent}'s children
     */
    private record Point(String id, DecisionKind kind, Winner winner, @Nullable Winner parent, int childIndex) {
        LogicalPlan plan() {
            return winner.group().plan;
        }

        String chosen() {
            return winner.alternative().label();
        }
    }

    /** The result of costing one alternative; {@code root} is {@code null} when no plan was produced. */
    private record Counterfactual(@Nullable Winner root, boolean costed) {
        static final Counterfactual NOT_COSTED = new Counterfactual(null, false);
        static final Counterfactual NO_PLAN = new Counterfactual(null, true);
    }

    ExplainBuilder(Search search, Winner best, Decided decided, ExplainLevel level, RunInfo run, long startNanos) {
        assert level != ExplainLevel.NONE : "no report is built at explain level NONE";
        this.search = search;
        this.best = best;
        this.decided = decided;
        this.level = level;
        this.run = run;
        this.startNanos = startNanos;
        this.planCost = best.cost().toVector();
        List<Point> collected = new ArrayList<>();
        collectPoints(best, null, -1, Collections.newSetFromMap(new IdentityHashMap<>()), collected);
        this.points = collected;
        for (Point point : collected) {
            pointsByWinner.put(point.winner(), point);
        }
    }

    /** Reports a run that did not search, for example because shuffle execution is unavailable. */
    static PlanDecisions ineligible(ExplainLevel level, RunInfo run, String reason) {
        if (level == ExplainLevel.NONE) {
            return PlanDecisions.EMPTY;
        }
        String explain = header(run, RunStatus.INELIGIBLE) + ": " + reason;
        RunReport report = new RunReport(
            run.ordinal(),
            run.trigger(),
            run.subplan(),
            RunStatus.INELIGIBLE,
            reason,
            null,
            null,
            List.of(),
            0,
            explain
        );
        return new PlanDecisions(Map.of(), Map.of(), report, List.of());
    }

    /** Reports a search that produced no plan; rule-based planning applies, so no decisions are published. */
    static PlanDecisions aborted(ExplainLevel level, RunInfo run, Search search, String reason, long startNanos) {
        if (level == ExplainLevel.NONE) {
            return PlanDecisions.EMPTY;
        }
        SearchStats stats = new SearchStats(
            search.groupCount(),
            search.evaluatedCandidates(),
            CostConstants.MAX_CANDIDATES,
            0,
            System.nanoTime() - startNanos
        );
        String explain = header(run, RunStatus.ABORTED) + " candidates=" + search.evaluatedCandidates() + ": " + reason;
        RunReport report = new RunReport(
            run.ordinal(),
            run.trigger(),
            run.subplan(),
            RunStatus.ABORTED,
            reason,
            stats,
            null,
            List.of(),
            0,
            explain
        );
        return new PlanDecisions(Map.of(), Map.of(), report, List.of());
    }

    PlanDecisions build() {
        int reported = Math.min(points.size(), MAX_DECISIONS);
        List<DecisionReport> decisions = new ArrayList<>(reported);
        List<PlanDecisions.DecisionPoint> decisionPoints = new ArrayList<>(reported);
        for (int i = 0; i < reported; i++) {
            Point point = points.get(i);
            decisions.add(report(point));
            decisionPoints.add(new PlanDecisions.DecisionPoint(point.id(), point.kind(), point.kind().key(point.plan()), point.chosen()));
        }
        SearchStats stats = new SearchStats(
            search.groupCount(),
            search.evaluatedCandidates(),
            CostConstants.MAX_CANDIDATES,
            counterfactualSearches,
            System.nanoTime() - startNanos
        );
        String explain = explainText(decisions);
        RunReport report = new RunReport(
            run.ordinal(),
            run.trigger(),
            run.subplan(),
            RunStatus.OK,
            null,
            stats,
            planCost,
            decisions,
            points.size() - reported,
            explain
        );
        return new PlanDecisions(decided.joins(), decided.aggregates(), report, decisionPoints);
    }

    private static boolean isPoint(Winner winner) {
        return winner.decisionApplicable() && CostBasedOptimizer.kindOf(winner.group().plan) != null;
    }

    private void collectPoints(Winner winner, @Nullable Winner parent, int childIndex, Set<Winner> visited, List<Point> out) {
        if (visited.add(winner) == false) {
            return;
        }
        if (isPoint(winner)) {
            out.add(new Point("d" + (out.size() + 1), CostBasedOptimizer.kindOf(winner.group().plan), winner, parent, childIndex));
        }
        for (int i = 0; i < winner.children().size(); i++) {
            collectPoints(winner.children().get(i), winner, i, visited, out);
        }
    }

    private DecisionReport report(Point point) {
        Winner winner = point.winner();
        LogicalPlan plan = point.plan();
        Enumeration enumeration = search.enumeration(winner.group());
        DecisionMode mode = winner.fitFallback() ? DecisionMode.FIT_FALLBACK : enumeration.mode();
        ChosenReport chosen = new ChosenReport(
            point.chosen(),
            render(winner.nativeDelivered(), plan),
            attributed(best, plan),
            search.fitChecks(winner.group(), winner.alternative())
        );

        List<AlternativeReport> alternatives = new ArrayList<>();
        boolean anomaly = false;
        Set<String> seen = new LinkedHashSet<>();
        seen.add(point.chosen());
        String reason = enumeration.reason();
        List<Exclusion> excluded = enumeration.excluded();
        if (mode == DecisionMode.GATED && winner.alternative().kind() == OperatorKind.AGGREGATE) {
            reason = search.gatedReason(winner);
            excluded = new ArrayList<>(enumeration.excluded());
            for (Alternative alternative : enumeration.alternatives()) {
                if (seen.add(alternative.label())) {
                    excluded.add(new Exclusion(alternative.label(), ExclusionCode.UNKNOWN_STATISTICS, reason));
                }
            }
        }
        for (Alternative alternative : enumeration.alternatives()) {
            if (seen.add(alternative.label()) == false) {
                continue;
            }
            AlternativeReport report = alternative(point, mode, alternative);
            if (report.planCost() != null
                && report.status() == AlternativeStatus.LOST_ON_COST
                && report.planCost().score() < planCost.score()) {
                anomaly = true;
                logger.debug("counterfactual plan for [{}={}] scored lower than the chosen plan", point.id(), alternative.label());
            }
            alternatives.add(report);
        }
        alternatives.sort(Comparator.comparingDouble(ExplainBuilder::sortScore));

        List<NamedEstimate> inputs = point.kind().inputs(plan, search::stats);
        List<String> anchor = new ArrayList<>();
        for (Attribute attribute : point.kind().anchor(plan)) {
            anchor.add(anchor(attribute));
        }
        Outcome outcome = conflicting(point) ? new Outcome(OutcomeStatus.CONFLICT, null, CONFLICT_REASON, null, null) : null;
        String summary = summary(point.chosen(), mode, reason, excluded, alternatives);
        return new DecisionReport(
            point.id(),
            point.kind().name(),
            plan.getClass().getSimpleName(),
            sourceAnchor(plan.source()),
            summary,
            mode,
            chosen,
            outcome,
            required(point),
            anchor,
            estimateBasis(inputs),
            inputs,
            alternatives,
            excluded,
            anomaly,
            finite(chosen, alternatives) ? null : NOT_FINITE_WARNING
        );
    }

    private static double sortScore(AlternativeReport alternative) {
        if (alternative.planCost() == null || Double.isFinite(alternative.planCost().score()) == false) {
            return Double.POSITIVE_INFINITY;
        }
        return alternative.planCost().score();
    }

    private boolean conflicting(Point point) {
        Object key = point.kind().key(point.plan());
        return decided.conflictingJoins().contains(key) || decided.conflictingAggregates().contains(key);
    }

    private AlternativeReport alternative(Point point, DecisionMode mode, Alternative alternative) {
        List<FitCheck> fitChecks = search.fitChecks(point.winner().group(), alternative);
        boolean fitFailed = (mode == DecisionMode.COST_BASED || mode == DecisionMode.FIT_FALLBACK)
            && fitChecks.stream().anyMatch(check -> check.passed() == false);
        Counterfactual counterfactual = level == ExplainLevel.FULL ? counterfactual(point, alternative.label()) : Counterfactual.NOT_COSTED;
        Winner root = counterfactual.root();
        if (root == null) {
            AlternativeStatus status = fitFailed ? AlternativeStatus.REJECTED_FIT
                : counterfactual.costed() ? AlternativeStatus.NO_PLAN
                : AlternativeStatus.NOT_COSTED;
            return new AlternativeReport(alternative.label(), status, null, null, null, List.of(), fitChecks, List.of(), null);
        }

        Winner atPoint = find(root, point.plan());
        CostVector cost = root.cost().toVector();
        double deltaScore = cost.score() - planCost.score();
        double percent = planCost.score() == 0 ? 0 : 100 * deltaScore / planCost.score();
        AlternativeStatus status;
        TieBreak tieBreak = null;
        if (fitFailed) {
            status = AlternativeStatus.REJECTED_FIT;
        } else if (Search.compareScores(root.cost(), best.cost()) != 0) {
            // A lower score here is a search anomaly; the caller flags it on the decision.
            status = AlternativeStatus.LOST_ON_COST;
        } else {
            status = AlternativeStatus.LOST_ON_TIE;
            tieBreak = new TieBreak(tieBreakRule(point, atPoint, root), CostConstants.TIE_EPSILON * 100);
        }
        return new AlternativeReport(
            alternative.label(),
            status,
            atPoint == null ? null : render(atPoint.nativeDelivered(), point.plan()),
            cost,
            new Delta(deltaScore, percent),
            attributed(root, point.plan()),
            fitChecks,
            otherDecisions(point, root),
            tieBreak
        );
    }

    private Counterfactual counterfactual(Point point, String label) {
        if (counterfactualSearches >= MAX_COUNTERFACTUAL_SEARCHES) {
            return Counterfactual.NOT_COSTED;
        }
        counterfactualSearches++;
        try {
            Winner root = search.constrained(point.winner().group(), label).solve();
            return root == null ? Counterfactual.NO_PLAN : new Counterfactual(root, true);
        } catch (SearchAborted e) {
            return Counterfactual.NOT_COSTED;
        }
    }

    /** Mirrors {@link Search}'s tie-break order: the tie preference summed over each plan first, then the plan-level rules. */
    private TieBreakRule tieBreakRule(Point point, @Nullable Winner atPoint, Winner root) {
        if (search.subtreePreference(best) != search.subtreePreference(root)) {
            boolean broadcast = atPoint != null
                && point.plan() instanceof SubqueryLookupJoin
                && (point.winner().alternative().joinStrategy() == JoinStrategySelector.JoinStrategy.BROADCAST
                    || atPoint.alternative().joinStrategy() == JoinStrategySelector.JoinStrategy.BROADCAST);
            return broadcast ? TieBreakRule.BROADCAST_PREFERRED : TieBreakRule.INCUMBENT;
        }
        if (best.cost().peakLaneBytes() != root.cost().peakLaneBytes()) {
            return TieBreakRule.PEAK_LANE_BYTES;
        }
        if (best.cost().peakNodeBytes() != root.cost().peakNodeBytes()) {
            return TieBreakRule.PEAK_NODE_BYTES;
        }
        if (best.shuffleEdges() != root.shuffleEdges()) {
            return TieBreakRule.SHUFFLE_EDGES;
        }
        return TieBreakRule.ENUMERATION_ORDER;
    }

    private List<DecisionRef> otherDecisions(Point point, Winner root) {
        List<DecisionRef> others = new ArrayList<>();
        for (Point other : points) {
            if (other == point) {
                continue;
            }
            Winner counterpart = find(root, other.plan());
            if (counterpart != null && counterpart.alternative().label().equals(other.chosen()) == false) {
                others.add(new DecisionRef(other.id(), counterpart.alternative().label()));
            }
        }
        return others;
    }

    /** Returns the first winner in pre-order that implements {@code plan}, or {@code null}. */
    private static Winner find(Winner winner, LogicalPlan plan) {
        if (winner.group().plan == plan) {
            return winner;
        }
        for (Winner child : winner.children()) {
            Winner found = find(child, plan);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Collects the cost components of the decision at {@code target} in the plan rooted at {@code root}. The operator's own parts
     * always belong to it. An exchange between a parent and a child belongs to the parent if the parent is a decision point, else
     * to the child if the child is one, else to neither.
     */
    private List<CostComponent> attributed(Winner root, LogicalPlan target) {
        List<CostComponent> components = new ArrayList<>();
        attribute(root, null, -1, target, Collections.newSetFromMap(new IdentityHashMap<>()), components);
        return components;
    }

    private void attribute(
        Winner winner,
        @Nullable Winner parent,
        int childIndex,
        LogicalPlan target,
        Set<Winner> visited,
        List<CostComponent> out
    ) {
        if (visited.add(winner) == false) {
            return;
        }
        boolean isTarget = winner.group().plan == target;
        if (isTarget) {
            for (Part part : winner.local().parts()) {
                out.add(new CostComponent(part.term(), part.side(), part.cost().toVector()));
            }
        } else if (isPointOrTarget(winner, target) == false) {
            for (Part part : winner.local().parts()) {
                if (part.edge() >= 0 && winner.children().get(part.edge()).group().plan == target) {
                    out.add(new CostComponent(part.term(), Side.OUTPUT, part.cost().toVector()));
                }
            }
        }
        if (winner.enforcer() != null) {
            if (parent != null && isPointOrTarget(parent, target)) {
                if (parent.group().plan == target) {
                    out.add(new CostComponent(term(winner.enforcer()), side(parent, childIndex), winner.enforcerCost().toVector()));
                }
            } else if (isTarget) {
                out.add(new CostComponent(term(winner.enforcer()), Side.OUTPUT, winner.enforcerCost().toVector()));
            }
        }
        for (int i : costedChildren(winner)) {
            attribute(winner.children().get(i), winner, i, target, visited, out);
        }
    }

    /** The target counts as a decision point even when a counterfactual plan runs it without a choice. */
    private static boolean isPointOrTarget(Winner winner, LogicalPlan target) {
        return winner.group().plan == target || isPoint(winner);
    }

    /** The children whose cost is part of the winner's cost; a lookup join does not scan its lookup index side. */
    private static List<Integer> costedChildren(Winner winner) {
        List<Integer> indices = new ArrayList<>(winner.children().size());
        boolean lookupIndexJoin = winner.alternative().kind() == OperatorKind.JOIN
            && winner.alternative().joinStrategy() == JoinStrategySelector.JoinStrategy.LOOKUP
            && Mapper.isIndexModeLookup(((Join) winner.group().plan).right());
        for (int i = 0; i < winner.children().size(); i++) {
            if (lookupIndexJoin && i == 1) {
                continue;
            }
            indices.add(i);
        }
        return indices;
    }

    private static CostTerm term(Enforcer enforcer) {
        return switch (enforcer) {
            case GATHER -> CostTerm.EXCHANGE_GATHER;
            case HASH -> CostTerm.EXCHANGE_HASH;
            case BROADCAST -> CostTerm.EXCHANGE_BROADCAST;
        };
    }

    private static Side side(Winner parent, int childIndex) {
        if (parent.group().plan instanceof Join) {
            return childIndex == 0 ? Side.LEFT : Side.RIGHT;
        }
        return Side.INPUT;
    }

    private String required(Point point) {
        if (point.parent() == null) {
            return render(SingleRequirement.INSTANCE, point.plan());
        }
        return render(point.parent().alternative().childRequirements().get(point.childIndex()), point.plan());
    }

    private static String render(Requirement requirement, LogicalPlan plan) {
        return switch (requirement) {
            case AnyRequirement any -> "any";
            case SingleRequirement single -> "single";
            case BroadcastRequirement broadcast -> "broadcast";
            case HashRequirement hash -> "hash(" + renderKeys(hash.keys(), plan) + ")";
        };
    }

    private static String render(Delivered delivered, LogicalPlan plan) {
        return switch (delivered) {
            case SourcePartitioning source -> "source";
            case SinglePartitioning single -> "single";
            case ArbitraryPartitioning arbitrary -> "arbitrary";
            case HashPartitioning hash -> "hash(" + renderKeys(hash.keys(), plan) + ")";
        };
    }

    /** Renders key ids as the attributes of {@code plan}'s output, as they appear in plan strings. */
    private static String renderKeys(List<NameId> keys, LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        for (NameId key : keys) {
            if (sb.isEmpty() == false) {
                sb.append(", ");
            }
            Attribute found = null;
            for (Attribute attribute : plan.output()) {
                if (attribute.id().equals(key)) {
                    found = attribute;
                    break;
                }
            }
            sb.append(found == null ? "#" + key : found.toString());
        }
        return sb.toString();
    }

    /**
     * Renders {@code attribute} as {@code name{type}}, the form it takes in every plan string. The id is dropped because each
     * node assigns fresh ids when it reads a plan, so an id only matches the coordinator's plan.
     */
    static String anchor(Attribute attribute) {
        String rendered = attribute.toString();
        String id = "#" + attribute.id();
        return rendered.endsWith(id) ? rendered.substring(0, rendered.length() - id.length()) : rendered;
    }

    private static SourceAnchor sourceAnchor(Source source) {
        if (source == null || source.text() == null || source.text().isEmpty()) {
            return null;
        }
        return new SourceAnchor(source.text(), source.lineNumber(), source.columnNumber());
    }

    private static Provenance estimateBasis(List<NamedEstimate> inputs) {
        if (inputs.isEmpty()) {
            return Provenance.UNKNOWN;
        }
        Provenance basis = Provenance.EXACT;
        for (NamedEstimate input : inputs) {
            basis = Search.weaker(basis, input.estimate().provenance());
        }
        return basis;
    }

    private boolean finite(ChosenReport chosen, List<AlternativeReport> alternatives) {
        if (planCost.isFinite() == false || finite(chosen.components()) == false) {
            return false;
        }
        for (AlternativeReport alternative : alternatives) {
            if (alternative.planCost() != null && alternative.planCost().isFinite() == false) {
                return false;
            }
            if (finite(alternative.components()) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean finite(List<CostComponent> components) {
        for (CostComponent component : components) {
            if (component.cost().isFinite() == false) {
                return false;
            }
        }
        return true;
    }

    private static String summary(
        String chosen,
        DecisionMode mode,
        @Nullable String reason,
        List<Exclusion> excluded,
        List<AlternativeReport> alternatives
    ) {
        return switch (mode) {
            case COST_BASED -> costBasedSummary(chosen, alternatives);
            case FORCED -> chosen + " forced by " + reason + ".";
            case GATED -> {
                List<String> names = new ArrayList<>();
                for (Exclusion exclusion : excluded) {
                    names.add(exclusion.name());
                }
                yield chosen
                    + " kept: "
                    + reason
                    + (names.isEmpty() ? "." : ", so " + joinNames(names) + (names.size() == 1 ? " was" : " were") + " not costed.");
            }
            case INELIGIBLE -> reason == null
                ? chosen + " is the only legal strategy."
                : chosen + " is the only legal strategy: " + reason + ".";
            case FIT_FALLBACK -> "All alternatives failed fit checks; " + chosen + " used as the safe fallback.";
        };
    }

    private static String costBasedSummary(String chosen, List<AlternativeReport> alternatives) {
        StringBuilder sb = new StringBuilder(chosen).append(" chosen by cost.");
        List<String> scored = new ArrayList<>();
        for (AlternativeReport alternative : alternatives) {
            if (alternative.status() == AlternativeStatus.LOST_ON_COST || alternative.status() == AlternativeStatus.LOST_ON_TIE) {
                String text = alternative.name() + " scores " + scoreAndDelta(alternative);
                if (alternative.tieBreak() != null) {
                    text += ", tie broken by " + alternative.tieBreak().rule().name().toLowerCase(Locale.ROOT);
                }
                scored.add(text);
            }
        }
        if (scored.isEmpty() == false) {
            sb.append(' ').append(String.join("; ", scored)).append('.');
        }
        for (AlternativeReport alternative : alternatives) {
            switch (alternative.status()) {
                case REJECTED_FIT -> sb.append(' ')
                    .append(alternative.name())
                    .append(" rejected: ")
                    .append(failedChecks(alternative))
                    .append('.');
                case NO_PLAN -> sb.append(' ').append(alternative.name()).append(" has no valid plan.");
                case NOT_COSTED -> sb.append(' ').append(alternative.name()).append(" was not costed.");
                case LOST_ON_COST, LOST_ON_TIE -> {
                }
            }
        }
        return sb.toString();
    }

    private static String scoreAndDelta(AlternativeReport alternative) {
        return String.format(Locale.ROOT, "%.3f (%+.1f%%)", alternative.planCost().score(), alternative.delta().percent());
    }

    private static String failedChecks(AlternativeReport alternative) {
        List<String> failed = new ArrayList<>();
        for (FitCheck check : alternative.fitChecks()) {
            if (check.passed() == false) {
                failed.add(check.check() + " failed " + checkDetail(check));
            }
        }
        return String.join(", ", failed);
    }

    private static String checkDetail(FitCheck check) {
        if (check.method() == FitMethod.PROBABILITY) {
            return String.format(Locale.ROOT, "(p=%.2f < %.2f)", check.probability(), check.threshold());
        }
        return "(demand " + bytes(check.demand().upperBound()) + " > budget " + bytes(check.budgetInBytes()) + ")";
    }

    private static String joinNames(List<String> names) {
        if (names.size() <= 2) {
            return String.join(" and ", names);
        }
        return String.join(", ", names.subList(0, names.size() - 1)) + ", and " + names.get(names.size() - 1);
    }

    // Explain text

    private static String header(RunInfo run, RunStatus status) {
        String trigger = run.trigger().name().toLowerCase(Locale.ROOT) + (run.subplan() == null ? "" : " " + run.subplan());
        return "run " + run.ordinal() + " (" + trigger + ") status=" + status.name().toLowerCase(Locale.ROOT);
    }

    private String explainText(List<DecisionReport> decisions) {
        StringBuilder sb = new StringBuilder();
        sb.append(header(run, RunStatus.OK))
            .append(" decisions=")
            .append(points.size())
            .append(" candidates=")
            .append(search.evaluatedCandidates())
            .append(" counterfactuals=")
            .append(counterfactualSearches)
            .append('\n');
        sb.append("plan ").append(costText(planCost)).append('\n');
        Map<Winner, DecisionReport> reports = new IdentityHashMap<>();
        for (int i = 0; i < decisions.size(); i++) {
            reports.put(points.get(i).winner(), decisions.get(i));
        }
        TreeLine root = withEnforcer(best, subtree(best, 0, reports), 0);
        root.render(sb, "", "");
        if (sb.length() > MAX_EXPLAIN_CHARS) {
            sb.setLength(MAX_EXPLAIN_CHARS - TRUNCATED_MARKER.length());
            sb.append(TRUNCATED_MARKER);
        } else if (sb.isEmpty() == false && sb.charAt(sb.length() - 1) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /** One line of the decision tree with its notes and children; renders with the same glyphs as plan trees. */
    private static final class TreeLine {
        private final String text;
        private final List<String> notes = new ArrayList<>();
        private final List<TreeLine> children = new ArrayList<>();

        TreeLine(String text) {
            this.text = text;
        }

        void render(StringBuilder sb, String linePrefix, String childPrefix) {
            sb.append(linePrefix).append(text).append('\n');
            for (String note : notes) {
                sb.append(childPrefix).append(children.isEmpty() ? "   " : "|  ").append(note).append('\n');
            }
            for (int i = 0; i < children.size(); i++) {
                boolean last = i == children.size() - 1;
                children.get(i).render(sb, childPrefix + (last ? "\\_" : "|_"), childPrefix + (last ? "  " : "| "));
            }
        }
    }

    /**
     * Builds the tree for {@code winner} without the exchange on top of it. Decision points, leaves, and nodes with several
     * children are kept; other nodes are hidden and counted in the {@code [+N ops]} suffix of the next kept line below them.
     */
    private TreeLine subtree(Winner winner, int hidden, Map<Winner, DecisionReport> reports) {
        DecisionReport report = reports.get(winner);
        Point point = pointsByWinner.get(winner);
        int children = winner.children().size();
        if (point == null && children == 1) {
            return edge(winner, 0, hidden + 1, reports);
        }
        TreeLine line;
        if (report != null) {
            line = new TreeLine(decisionLine(report) + ops(hidden));
            addNotes(line, report);
        } else if (point != null) {
            line = new TreeLine(
                point.plan().nodeName() + " " + point.id() + "=" + point.chosen() + " [not reported, decision cap reached]" + ops(hidden)
            );
        } else if (children == 0) {
            line = new TreeLine(leafLine(winner.group().plan) + ops(hidden));
        } else {
            line = new TreeLine(winner.group().plan.nodeName() + "[" + winner.alternative().label() + "]" + ops(hidden));
        }
        for (int i = 0; i < children; i++) {
            line.children.add(edge(winner, i, 0, reports));
        }
        return line;
    }

    /** Builds the edge from {@code parent} to its child at {@code childIndex}, including the exchanges on it. */
    private TreeLine edge(Winner parent, int childIndex, int hidden, Map<Winner, DecisionReport> reports) {
        Winner child = parent.children().get(childIndex);
        List<String> exchanges = new ArrayList<>();
        for (Part part : parent.local().parts()) {
            if (part.edge() == childIndex && part.term() == CostTerm.EXCHANGE_GATHER) {
                exchanges.add("Exchange[GATHER]");
            }
        }
        if (child.enforcer() != null) {
            exchanges.add(enforcerLine(child));
        }
        if (exchanges.isEmpty()) {
            return subtree(child, hidden, reports);
        }
        TreeLine below = subtree(child, 0, reports);
        for (int i = exchanges.size() - 1; i >= 0; i--) {
            TreeLine exchange = new TreeLine(exchanges.get(i) + (i == 0 ? ops(hidden) : ""));
            exchange.children.add(below);
            below = exchange;
        }
        return below;
    }

    private TreeLine withEnforcer(Winner winner, TreeLine line, int hidden) {
        if (winner.enforcer() == null) {
            return line;
        }
        TreeLine exchange = new TreeLine(enforcerLine(winner) + ops(hidden));
        exchange.children.add(line);
        return exchange;
    }

    private static String enforcerLine(Winner winner) {
        return switch (winner.enforcer()) {
            case GATHER -> "Exchange[GATHER]";
            case BROADCAST -> "Exchange[BROADCAST]";
            case HASH -> "Exchange[HASH "
                + (winner.delivered() instanceof HashPartitioning hash ? renderKeys(hash.keys(), winner.group().plan) : "")
                + "]";
        };
    }

    private static String ops(int hidden) {
        return hidden == 0 ? "" : " [+" + hidden + " ops]";
    }

    private static String decisionLine(DecisionReport report) {
        StringBuilder sb = new StringBuilder(report.node());
        if (report.source() != null) {
            sb.append('[')
                .append(shorten(report.source().text()))
                .append(" @")
                .append(report.source().line())
                .append(':')
                .append(report.source().column())
                .append(']');
        }
        return sb.append(' ')
            .append(report.id())
            .append('=')
            .append(report.chosen().name())
            .append(' ')
            .append(report.mode().name().toLowerCase(Locale.ROOT))
            .append(" basis=")
            .append(report.estimateBasis().name().toLowerCase(Locale.ROOT))
            .toString();
    }

    private static void addNotes(TreeLine line, DecisionReport report) {
        for (AlternativeReport alternative : report.alternatives()) {
            StringBuilder note = new StringBuilder("vs ").append(alternative.name());
            if (alternative.planCost() != null) {
                note.append(" score=").append(scoreAndDelta(alternative));
            }
            switch (alternative.status()) {
                case LOST_ON_COST -> {
                }
                case LOST_ON_TIE -> note.append(" tie broken by ").append(alternative.tieBreak().rule().name().toLowerCase(Locale.ROOT));
                case REJECTED_FIT -> note.append(" rejected_fit: ").append(failedChecksShort(alternative));
                case NO_PLAN -> note.append(" no_plan");
                case NOT_COSTED -> note.append(" not_costed");
            }
            if (alternative.otherDecisions().isEmpty() == false) {
                List<String> others = new ArrayList<>();
                for (DecisionRef other : alternative.otherDecisions()) {
                    others.add(other.id() + "=" + other.chosen());
                }
                note.append(" with ").append(String.join(", ", others));
            }
            line.notes.add(note.toString());
        }
        for (Exclusion exclusion : report.excluded()) {
            line.notes.add(
                "- "
                    + exclusion.name()
                    + " excluded "
                    + exclusion.code().name().toLowerCase(Locale.ROOT)
                    + (exclusion.reason() == null ? "" : ": " + exclusion.reason())
            );
        }
        if (report.searchAnomaly()) {
            line.notes.add("! search_anomaly: an alternative scored lower than the chosen plan");
        }
    }

    private static String failedChecksShort(AlternativeReport alternative) {
        List<String> failed = new ArrayList<>();
        for (FitCheck check : alternative.fitChecks()) {
            if (check.passed() == false) {
                if (check.method() == FitMethod.PROBABILITY) {
                    failed.add(String.format(Locale.ROOT, "%s p=%.2f<%.2f", check.check(), check.probability(), check.threshold()));
                } else {
                    failed.add(check.check() + " demand=" + bytes(check.demand().upperBound()) + ">" + bytes(check.budgetInBytes()));
                }
            }
        }
        return String.join(", ", failed);
    }

    private String leafLine(LogicalPlan plan) {
        String name = plan instanceof EsRelation relation ? "EsRelation[" + relation.indexPattern() + "]" : plan.nodeName();
        PlanStats stats = search.stats(plan);
        Estimate rows = stats == null ? Estimate.UNKNOWN : stats.rows();
        Estimate bytes = stats == null ? Estimate.UNKNOWN : stats.bytes();
        Provenance provenance = Search.weaker(rows.provenance(), bytes.provenance());
        return name
            + " rows="
            + (rows.isUnknown() ? "?" : count(rows.value()))
            + " bytes="
            + (bytes.isUnknown() ? "?" : bytes(bytes.value()))
            + " "
            + provenance.name().toLowerCase(Locale.ROOT);
    }

    private static String costText(CostVector cost) {
        return String.format(Locale.ROOT, "score=%.3f", cost.score())
            + " latency="
            + duration(cost.latencyNanos())
            + " work="
            + count(cost.workUnits())
            + " network="
            + bytes(cost.networkInBytes())
            + " coordinator="
            + bytes(cost.coordinatorInBytes())
            + " peak_node="
            + bytes(cost.peakNodeInBytes())
            + " peak_lane="
            + bytes(cost.peakLaneInBytes());
    }

    private static String shorten(String text) {
        String single = text.replaceAll("\\s+", " ").trim();
        return single.length() <= MAX_SOURCE_TEXT_CHARS ? single : single.substring(0, MAX_SOURCE_TEXT_CHARS - 3) + "...";
    }

    private static String duration(double nanos) {
        return Double.isFinite(nanos) ? TimeValue.timeValueNanos(Math.round(nanos)).toString() : "?";
    }

    private static String bytes(double bytes) {
        return Double.isFinite(bytes) ? ByteSizeValue.ofBytes(Math.round(bytes)).toString() : "?";
    }

    private static String count(double value) {
        if (Double.isFinite(value) == false) {
            return "?";
        }
        double abs = Math.abs(value);
        if (abs < 1_000) {
            return Long.toString(Math.round(value));
        }
        if (abs < 1_000_000) {
            return String.format(Locale.ROOT, "%.1fK", value / 1_000);
        }
        if (abs < 1_000_000_000) {
            return String.format(Locale.ROOT, "%.1fM", value / 1_000_000);
        }
        if (abs < 1_000_000_000_000d) {
            return String.format(Locale.ROOT, "%.1fB", value / 1_000_000_000);
        }
        return String.format(Locale.ROOT, "%.1fT", value / 1_000_000_000_000d);
    }
}
