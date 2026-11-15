/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult.Kind;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates the PromQL set operators over a label container. {@code and} and {@code unless} translate like the other binary
 * operators; {@code or} adds series, so its branches translate independently and a union keeps, for each step, the series of
 * the first branch whose labels, the metric name left out, no earlier branch has.
 */
public final class VectorBinarySetV2 {

    /** The labels two {@code or} branches compare: the series' labels without the metric name. */
    private static final String OR_MATCH_KEY = "$_or_match";

    private VectorBinarySetV2() {}

    public static IntermediateResult translate(VectorBinarySet set, TranslationContextV2 context) {
        if (set.op() != VectorBinarySet.SetOp.UNION) {
            return VectorBinaryOperatorV2.translate(set, context);
        }
        List<LogicalPlan> branches = flattenUnion(set);
        // One limb: this or keeps every left series of a signature, so it unions a with b unless a, deduplicating nothing.
        boolean keepsLeftTies = branches.size() == 1;
        if (keepsLeftTies) {
            branches = List.of(set.left(), rightUnlessLeft(set));
        }
        List<IntermediateResult> intermediateResults = new ArrayList<>(branches.size());
        for (LogicalPlan branch : branches) {
            intermediateResults.add(context.translateIntermediate(branch, new NameId(), new NameId()));
        }
        UnionAll union = unionBranches(context, intermediateResults);
        return wrapUnion(set, context, keepsLeftTies ? union : firstPerSeries(context, union));
    }

    /**
     * The right operand without the series the left one matches: {@code b unless<M> a} for {@code a or<M> b}. Prometheus
     * keeps every series of {@code a} and those of {@code b} whose match signature no series of {@code a} has, so the union of
     * {@code a} and this needs no deduplication.
     */
    private static VectorBinarySet rightUnlessLeft(VectorBinarySet set) {
        return new VectorBinarySet(set.source(), set.right(), set.left(), set.match(), VectorBinarySet.SetOp.SUBTRACT);
    }

    /**
     * Always rename value to kill UnionAll ghost channels (#158164).
     */
    private static IntermediateResult wrapUnion(VectorBinarySet set, TranslationContextV2 context, LogicalPlan union) {
        PromqlCommand cmd = context.cmd();
        Attribute value = null;
        Attribute step = null;
        Attribute labels = null;
        for (Attribute attr : union.output()) {
            if (attr.name().equals(cmd.valueColumnName())) {
                value = attr;
            } else if (attr.name().equals(cmd.stepColumnName())) {
                step = attr;
            } else if (attr.name().equals(MetadataAttribute.TIMESERIES)) {
                labels = attr;
            }
        }
        assert value != null && step != null : "union result must expose value and step";
        // Drop the branch tag first, then pin with a rename-Eval so the Project cannot be optimized away while ghosts
        // remain in the page (same materialization {@link VectorBinaryOperator} applies to join operands).
        List<NamedExpression> withoutBranch = new ArrayList<>();
        withoutBranch.add(value);
        withoutBranch.add(step);
        withoutBranch.add(labels);
        LogicalPlan plan = new Project(set.source(), union, withoutBranch);
        String pinName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        Alias pinned = new Alias(value.source(), pinName, value, new NameId());
        // Expose the step under the enclosing frame's step identity, as a join result does, so an operand sharing the
        // frame (a scalar in 2 + (a or b)) reads the union's step.
        Alias frameStep = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        plan = new Eval(set.source(), plan, List.of(pinned, frameStep));
        Attribute stepAttr = TranslationContextV2.pinnedAttribute(frameStep);
        List<NamedExpression> pinnedOut = new ArrayList<>();
        pinnedOut.add(pinned.toAttribute());
        pinnedOut.add(stepAttr);
        pinnedOut.add(labels);
        plan = new Project(set.source(), plan, pinnedOut);
        return new IntermediateResult(plan, pinned.toAttribute(), labels, stepAttr, null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The union of {@code intermediateResults}: each series of a branch whose labels, the metric name left out, no earlier
     * branch has at the same step - Prometheus' {@code or}.
     */
    public static LogicalPlan translateUnion(TranslationContextV2 context, List<IntermediateResult> intermediateResults) {
        return firstPerSeries(context, unionBranches(context, intermediateResults));
    }

    /** The branches side by side, each tagged with its position and projected onto the union's columns. */
    private static UnionAll unionBranches(TranslationContextV2 context, List<IntermediateResult> intermediateResults) {
        PromqlCommand cmd = context.cmd();
        // Already validated against max_branch_count_per_merge by PromqlCommand.verify.
        int maxBranches = context.analyzer().maxBranchCountPerMerge();
        assert intermediateResults.size() <= maxBranches
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be <= the max_branch_count_per_merge limit ["
                + maxBranches
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Absent left must not shadow a present right (rate or irate fallback).
            var ir = intermediateResults.get(i);
            LogicalPlan branchPlan = TranslationContextV2.emitNullsFilter(source, ir.plan(), ir.valueColumn());
            var branch = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            var labels = new Alias(
                source,
                MetadataAttribute.TIMESERIES,
                ir.labels() != null ? ir.labels() : TranslationContextV2.emptyLabels(source)
            );
            var match = new Alias(source, OR_MATCH_KEY, PromqlLabels.withoutMetricName(labels.toAttribute()));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branch, labels, match));
            // Pin layout across the exchange (#158164): Eval name-shadow leaves ghost channels otherwise.
            branchPlans.add(
                new Project(
                    source,
                    tagged,
                    List.of(ir.valueColumn(), ir.step(), labels.toAttribute(), match.toAttribute(), branch.toAttribute())
                )
            );
        }

        // Every branch projects the same columns; the union outputs the first branch's.
        return new UnionAll(source, branchPlans, branchPlans.getFirst().output());
    }

    /** Each series of {@code union} at a step whose labels, the metric name left out, no earlier branch has. */
    private static LogicalPlan firstPerSeries(TranslationContextV2 context, UnionAll union) {
        PromqlCommand cmd = context.cmd();
        Source source = cmd.source();
        List<Attribute> unionOutput = union.output();
        Attribute step = attribute(unionOutput, cmd.stepColumnName());
        Attribute match = attribute(unionOutput, OR_MATCH_KEY);
        var order = new Order(source, attribute(unionOutput, cmd.branchColumnName()), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        PackDims pack = TranslationContextV2.packLabels(source, union, match);
        return new TopNBy(source, pack, List.of(order), new Literal(source, 1, DataType.INTEGER), List.of(step, pack.packed()));
    }

    private static Attribute attribute(List<Attribute> output, String name) {
        for (Attribute attribute : output) {
            if (attribute.name().equals(name)) {
                return attribute;
            }
        }
        throw new IllegalStateException("union result must expose [" + name + "]");
    }

    /** Left-to-right limbs of a default-matching or chain. */
    public static List<LogicalPlan> flattenUnion(LogicalPlan node) {
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(node, branches);
        return branches;
    }

    /** Keep on/ignoring unions intact so their match semantics are not lost. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION && setOp.match().equals(VectorMatch.NONE)) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

}
