/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * One resolved {@code FROM} expanded to its producers: datasets, indices, and matched
 * cross-project namesakes. The coordinator merges those producers the same way it merges any
 * other {@link UnionAll}.
 * <p>
 * {@link Fork} allows this node inside a branch. A subquery the user wrote stays a plain
 * {@link UnionAll} and is still rejected under {@code FORK}.
 */
public final class SourceFanInUnionAll extends UnionAll {

    /**
     * Producers one {@code FROM} may expand to. Tied to {@link MergePlan#MAX_BRANCHES} so the
     * per-command caps cannot drift.
     */
    public static final int MAX_PRODUCERS = MergePlan.MAX_BRANCHES;

    /** True when {@code count} is more producers than one {@code FROM} may expand to. */
    public static boolean exceedsMaxProducers(int count) {
        return count > MAX_PRODUCERS;
    }

    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, flattenDirect(children), output);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output());
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new SourceFanInUnionAll(source(), newChildren, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new SourceFanInUnionAll(source(), subPlans, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new SourceFanInUnionAll(source(), subPlans, output);
    }

    @Override
    public SourceFanInUnionAll refreshOutput() {
        return new SourceFanInUnionAll(source(), children(), refreshedOutput());
    }

    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new SourceFanInUnionAll(source(), kept, output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(SourceFanInUnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceFanInUnionAll other = (SourceFanInUnionAll) o;
        return Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return SourceFanInUnionAll::checkSourceFanIn;
    }

    /**
     * Producers under {@code plan}. A nested fan-in and a unary pipeline wrapped around one
     * ({@code WHERE}, {@code EVAL}, {@code STATS}, {@code SORT}, {@code LIMIT}, or a projection)
     * contribute the producers inside them. Any other node is one producer.
     */
    public static int producerCount(LogicalPlan plan) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            int count = 0;
            for (LogicalPlan child : fanIn.children()) {
                count += producerCount(child);
            }
            return count;
        }
        if (isSourcePipelineUnary(plan)) {
            return producerCount(((UnaryPlan) plan).child());
        }
        return 1;
    }

    /**
     * A unary command that stays wrapped around a source fan-in: a filter, projection, eval, limit,
     * sort, or aggregate. Any other node is one producer, so a command such as {@code FORK} or a
     * subquery is not walked through.
     */
    public static boolean isSourcePipelineUnary(LogicalPlan plan) {
        return plan instanceof Filter
            || plan instanceof Project
            || plan instanceof Eval
            || plan instanceof Limit
            || plan instanceof OrderBy
            || plan instanceof Aggregate;
    }

    private static void checkSourceFanIn(LogicalPlan plan, Failures failures) {
        if (plan instanceof SourceFanInUnionAll fanIn) {
            if (fanIn.children().isEmpty()) {
                failures.add(Failure.fail(plan, "{} requires at least one branch", plan.getClass().getSimpleName()));
            }
            int producers = producerCount(fanIn);
            if (exceedsMaxProducers(producers)) {
                failures.add(
                    Failure.fail(
                        fanIn,
                        "FROM [{}] resolved to {} sources, exceeding the current limit of {} per FROM. "
                            + "Narrow the pattern, exclude some datasets, or split into multiple queries.",
                        fanIn.sourceText(),
                        producers,
                        MAX_PRODUCERS
                    )
                );
            }
        }
        UnionAll.checkOutputTypes(plan, failures);
    }

    /** Flattens a fan-in that is itself a direct child. A pipeline wrapped around an inner fan-in stays put. */
    private static List<LogicalPlan> flattenDirect(List<LogicalPlan> children) {
        boolean nested = false;
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll) {
                nested = true;
                break;
            }
        }
        if (nested == false) {
            return children;
        }
        List<LogicalPlan> flat = new ArrayList<>(children.size());
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll fanIn) {
                flat.addAll(fanIn.children());
            } else {
                flat.add(child);
            }
        }
        return flat;
    }
}
