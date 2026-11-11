/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlLabelAttribute;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public final class VectorBinarySet extends VectorBinaryOperator {

    /** Dedup key that unsets __name__; grouping on a natural label container would keep both raw and rate branches. */
    public static final String OR_MATCH_KEY = "$_or_match";

    public enum SetOp implements BinaryOp {
        INTERSECT("and"),
        SUBTRACT("unless"),
        UNION("or");

        private final String keyword;

        SetOp(String keyword) {
            this.keyword = keyword;
        }

        public String keyword() {
            return keyword;
        }

        @Override
        public ScalarFunctionFactory asFunction() {
            return switch (this) {
                case INTERSECT -> (source, left, right, config) -> new Case(source, new IsNotNull(source, right), List.of(left));
                case SUBTRACT -> (source, left, right, config) -> new Case(source, new IsNull(source, right), List.of(left));
                case UNION -> throw new AssertionError("UNION translates via UnionAll, not asFunction");
            };
        }
    }

    /** Unmodified unless can fold (right IS NULL); cross-metric empty-right needs LEFT hash join instead. */
    @Override
    protected boolean requiresIndependentPipelines() {
        if (op == SetOp.SUBTRACT && match().filter() == VectorMatch.Filter.NONE && match().grouping() == Joining.NONE) {
            return false;
        }
        return super.requiresIndependentPipelines();
    }

    /**
     * Fold would empty the left when operands never share groups; verifier rejects until join covers it.
     */
    public boolean subtractOperandsNeverShareGroups() {
        return op == SetOp.SUBTRACT
            && match() == VectorMatch.NONE
            && (hasMismatchedLabelSets() || aggregatesAcrossSeries(left()) != aggregatesAcrossSeries(right()));
    }

    private final SetOp op;

    public VectorBinarySet(Source source, LogicalPlan left, LogicalPlan right, VectorMatch match, SetOp op) {
        // Set operators preserve the metric name (__name__); unlike arithmetic/comparison operators they do not drop it.
        super(source, left, right, match, false, op);
        this.op = op;
    }

    public SetOp op() {
        return op;
    }

    /** or unions label sets; and/unless keep left identity only. */
    @Override
    public List<Attribute> output() {
        if (op == SetOp.UNION) {
            return unionOutputByName(List.of(left(), right()));
        }
        return left().output();
    }

    /** The labels of the union, by name: every branch's, and one tags label when some branch has one. */
    public static List<Attribute> unionOutputByName(List<? extends LogicalPlan> plans) {
        List<Attribute> result = new ArrayList<>();
        Set<String> names = new HashSet<>();
        Attribute tags = null;
        for (LogicalPlan plan : plans) {
            for (Attribute attr : plan.output()) {
                if (PromqlLabels.isTagsLabel(attr)) {
                    tags = tags == null ? UnresolvedPromqlLabelAttribute.newTagsLabel(attr.source()) : tags;
                } else if (names.add(attr.name())) {
                    result.add(attr);
                }
            }
        }
        if (tags != null) {
            result.add(tags);
        }
        return result;
    }

    /** An or leaves a label out only where every branch with a tags label does; and and unless keep the left series. */
    @Override
    public Set<String> excludedLabels() {
        if (op != SetOp.UNION) {
            return PromqlLabels.excludedWith(left(), Set.of());
        }
        Set<String> excluded = null;
        for (LogicalPlan branch : List.of(left(), right())) {
            Set<String> branchExcluded = PromqlLabels.excluded(branch);
            if (branchExcluded != null) {
                if (excluded == null) {
                    excluded = new LinkedHashSet<>(branchExcluded);
                } else {
                    excluded.retainAll(branchExcluded);
                }
            }
        }
        return excluded == null ? Set.of() : excluded;
    }

    @Override
    public VectorBinarySet replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinarySet(source(), newLeft, newRight, match(), op());
    }

    @Override
    protected NodeInfo<VectorBinarySet> info() {
        return NodeInfo.create(this, VectorBinarySet::new, left(), right(), match(), op());
    }

    /** Finished UnionAll+TopNBy so parents can consume nested or. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (op != SetOp.UNION) {
            return super.translate(context);
        }
        List<LogicalPlan> branches = flattenUnion(this);
        List<IntermediateResult> intermediateResults = new ArrayList<>(branches.size());
        for (LogicalPlan branch : branches) {
            intermediateResults.add(context.translateIntermediate(branch, new NameId(), new NameId()));
        }
        return wrapUnion(context, translateUnion(context, intermediateResults));
    }

    /**
     * Always rename value to kill UnionAll ghost channels (#158164).
     */
    private IntermediateResult wrapUnion(TranslationContext context, LogicalPlan union) {
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
        LogicalPlan plan = new Project(source(), union, withoutBranch);
        String pinName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        Alias pinned = new Alias(value.source(), pinName, value, new NameId());
        plan = new Eval(source(), plan, List.of(pinned));
        List<NamedExpression> pinnedOut = new ArrayList<>();
        pinnedOut.add(pinned.toAttribute());
        pinnedOut.add(step);
        pinnedOut.add(labels);
        plan = new Project(source(), plan, pinnedOut);
        return new IntermediateResult(plan, pinned.toAttribute(), labels, step, null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /** Combines independent union branches and keeps the leftmost value for each series and step. */
    /**
     * The union of {@code intermediateResults}: each series of a branch whose labels, the metric name left out, no earlier
     * branch has at the same step - Prometheus' {@code or}.
     */
    public static LogicalPlan translateUnion(TranslationContext context, List<IntermediateResult> intermediateResults) {
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
            LogicalPlan branchPlan = PromqlCommand.emitNullsFilter(source, ir.plan(), ir.valueColumn());
            var branch = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            var labels = new Alias(source, MetadataAttribute.TIMESERIES, ir.labels() != null ? ir.labels() : emptyLabels(source));
            var match = new Alias(
                source,
                OR_MATCH_KEY,
                new TimeSeriesMetadataFilter(source, labels.toAttribute(), List.of(Literal.keyword(source, LabelMatcher.NAME)), true)
            );
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
        List<Attribute> unionOutput = branchPlans.getFirst().output();
        var union = new UnionAll(source, branchPlans, unionOutput);
        Attribute step = attribute(unionOutput, cmd.stepColumnName());
        Attribute match = attribute(unionOutput, OR_MATCH_KEY);
        var order = new Order(source, attribute(unionOutput, cmd.branchColumnName()), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), List.of(step, match));
    }

    /** The labels of a vector without labels. */
    public static Expression emptyLabels(Source source) {
        return new ToTimeSeriesMetadata(source, Literal.keyword(source, "{}"));
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
        if (node instanceof VectorBinarySet setOp && setOp.op() == SetOp.UNION && setOp.match().equals(VectorMatch.NONE)) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

}
