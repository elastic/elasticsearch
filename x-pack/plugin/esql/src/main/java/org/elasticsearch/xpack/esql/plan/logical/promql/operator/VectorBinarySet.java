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
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlAttribute;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public final class VectorBinarySet extends VectorBinaryOperator {

    /** Dedup key that unsets __name__; grouping on natural metadata would keep both raw and rate branches. */
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

    /**
     * One time-series metadata column for the union: intersecting unset avoids dual blobs that poison fingerprints.
     */
    public static List<Attribute> unionOutputByName(List<? extends LogicalPlan> plans) {
        List<Attribute> result = new ArrayList<>();
        Set<String> names = new HashSet<>();
        List<UnresolvedPromqlAttribute> metadatas = new ArrayList<>();
        Source metadataSource = null;
        for (LogicalPlan plan : plans) {
            for (Attribute attr : plan.output()) {
                if (PromqlLabels.isMetadataLabel(attr)) {
                    metadatas.add((UnresolvedPromqlAttribute) attr);
                    if (metadataSource == null) {
                        metadataSource = attr.source();
                    }
                } else if (names.add(attr.name())) {
                    result.add(attr);
                }
            }
        }
        if (metadatas.isEmpty() == false) {
            Set<String> unset = null;
            for (UnresolvedPromqlAttribute timeSeriesMetadata : metadatas) {
                if (unset == null) {
                    unset = new LinkedHashSet<>(timeSeriesMetadata.unset());
                } else {
                    unset.retainAll(timeSeriesMetadata.unset());
                }
            }
            result.add(UnresolvedPromqlAttribute.metadata(metadataSource, unset));
        }
        return result;
    }

    @Override
    public VectorBinarySet replaceChildren(LogicalPlan newLeft, LogicalPlan newRight) {
        return new VectorBinarySet(source(), newLeft, newRight, match(), op());
    }

    @Override
    protected NodeInfo<VectorBinarySet> info() {
        return NodeInfo.create(this, VectorBinarySet::new, left(), right(), match(), op());
    }

    /** Finished UnionAll+TopNBy so parents can consume nested or; empty on () dedups by step only. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (op != SetOp.UNION) {
            return super.translate(context);
        }
        if (isLabellessOrOnEmpty(this)) {
            List<LogicalPlan> branches = List.of(left(), right());
            List<IntermediateResult> intermediateResults = List.of(
                context.translateIntermediate(left(), new NameId(), new NameId()),
                context.translateIntermediate(right(), new NameId(), new NameId())
            );
            return wrapUnion(context, translateUnion(context, branches, intermediateResults, List.of()), true);
        }
        List<LogicalPlan> branches = flattenUnion(this);
        List<IntermediateResult> intermediateResults = new ArrayList<>(branches.size());
        for (LogicalPlan branch : branches) {
            intermediateResults.add(context.translateIntermediate(branch, new NameId(), new NameId()));
        }
        return wrapUnion(context, translateUnion(context, branches, intermediateResults, output()), false);
    }

    /** Labelless or on (): empty-guard idiom, step-only dedup. */
    public static boolean isLabellessOrOnEmpty(VectorBinarySet setOp) {
        VectorMatch match = setOp.match();
        return setOp.op() == SetOp.UNION
            && match.filter() == VectorMatch.Filter.ON
            && match.filterLabels().isEmpty()
            && match.grouping() == Joining.NONE
            && setOp.left().output().isEmpty()
            && setOp.right().output().isEmpty();
    }

    /**
     * Always rename value to kill UnionAll ghost channels (#158164). stickyValue also drops labels (empty on ()).
     */
    private IntermediateResult wrapUnion(TranslationContext context, LogicalPlan union, boolean stickyValue) {
        PromqlCommand cmd = context.cmd();
        Attribute value = null;
        Attribute step = null;
        List<Attribute> labels = new ArrayList<>();
        for (Attribute attr : union.output()) {
            if (attr.name().equals(cmd.branchColumnName()) || attr.name().equals(OR_MATCH_KEY)) {
                continue;
            }
            if (attr.name().equals(cmd.valueColumnName())) {
                value = attr;
            } else if (attr.name().equals(cmd.stepColumnName())) {
                step = attr;
            } else {
                labels.add(attr);
            }
        }
        assert value != null && step != null : "union result must expose value and step";
        // Drop the branch tag first, then pin with a rename-Eval so the Project cannot be optimized away while ghosts
        // remain in the page (same materialization {@link VectorBinaryOperator} applies to join operands).
        List<NamedExpression> withoutBranch = new ArrayList<>();
        withoutBranch.add(value);
        withoutBranch.add(step);
        withoutBranch.addAll(labels);
        LogicalPlan plan = new Project(source(), union, withoutBranch);
        String pinName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        Alias pinned = new Alias(value.source(), pinName, value, new NameId());
        plan = new Eval(source(), plan, List.of(pinned));
        List<NamedExpression> pinnedOut = new ArrayList<>();
        pinnedOut.add(pinned.toAttribute());
        pinnedOut.add(step);
        if (stickyValue == false) {
            pinnedOut.addAll(labels);
        }
        plan = new Project(source(), plan, pinnedOut);
        return new IntermediateResult(plan, pinned.toAttribute(), step, null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /** Combines independent union branches and keeps the leftmost value for each series and step. */
    public static LogicalPlan translateUnion(
        TranslationContext context,
        List<LogicalPlan> branches,
        List<IntermediateResult> intermediateResults
    ) {
        return translateUnion(context, branches, intermediateResults, unionOutputByName(branches));
    }

    /** @param tsIdentity series identity (metadata + promoted labels) for left-preferring dedup. */
    public static LogicalPlan translateUnion(
        TranslationContext context,
        List<LogicalPlan> branches,
        List<IntermediateResult> intermediateResults,
        List<Attribute> tsIdentity
    ) {
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
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            var nullLabelEvals = new ArrayList<Alias>();
            // Pin layout across the exchange (#158164): Eval name-shadow leaves ghost channels otherwise.
            var projections = new ArrayList<NamedExpression>(List.of(ir.valueColumn(), ir.step()));
            if (ir.kind().constant == false) {
                // Typed: separate match key unsets __name__ (Prometheus set-op identity). Keyword cannot unset
                // __name__ after collapse, so it dedups on metadata instead.
                UnresolvedPromqlAttribute branchTimeSeriesMetadata = PromqlLabels.metadataLabel(branches.get(i).output());
                boolean typedMetadata = PromqlCommand.usesTimeSeriesMetadata(context.analyzer());
                for (Attribute label : tsIdentity) {
                    if (PromqlLabels.isMetadataLabel(label)) {
                        UnresolvedPromqlAttribute pack = branchTimeSeriesMetadata != null
                            ? UnresolvedPromqlAttribute.metadata(source, branchTimeSeriesMetadata.unset())
                            : UnresolvedPromqlAttribute.of(source, label);
                        projections.add(new Alias(source, MetadataAttribute.TIMESERIES, pack, label.id()));
                        if (typedMetadata) {
                            Set<String> matchUnset = new LinkedHashSet<>();
                            if (branchTimeSeriesMetadata != null) {
                                matchUnset.addAll(branchTimeSeriesMetadata.unset());
                            }
                            matchUnset.add(LabelMatcher.NAME);
                            projections.add(new Alias(source, OR_MATCH_KEY, UnresolvedPromqlAttribute.metadata(source, matchUnset)));
                        }
                    } else {
                        // Null-fill undeclared labels: resolving them would invent by-keys (empty max by (cluster) or vector(0)).
                        String labelName = PromqlLabels.labelName(label);
                        boolean declared = PromqlLabels.getLabel(
                            branches.get(i).output().stream().filter(a -> PromqlLabels.isMetadataLabel(a) == false).toList(),
                            labelName
                        ) != null;
                        if (declared) {
                            projections.add(UnresolvedPromqlAttribute.of(source, label));
                        } else {
                            Alias nullLabel = new Alias(source, label.name(), new Literal(source, null, label.dataType()), new NameId());
                            nullLabelEvals.add(nullLabel);
                            projections.add(nullLabel.toAttribute());
                        }
                    }
                }
            }
            projections.add(branchTagExpression.toAttribute());
            var evals = new ArrayList<Alias>();
            evals.add(branchTagExpression);
            evals.addAll(nullLabelEvals);
            LogicalPlan tagged = new Eval(source, branchPlan, evals);
            branchPlans.add(new Project(source, tagged, projections));
        }

        // UnionAll recomputes output by name; ids from the first branch stay valid for TopNBy groupings.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        Attribute orMatchKey = null;
        Attribute timeSeriesMetadataAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.stepColumnName())) {
                groupings.add(attr);
            } else if (attr.name().equals(OR_MATCH_KEY)) {
                orMatchKey = attr;
            } else if (PromqlLabels.isMetadataLabel(attr)
                || MetadataAttribute.TIMESERIES.equals(attr.name())
                || attr.name().startsWith(MetadataAttribute.TIMESERIES + "$")) {
                    // Alias.toAttribute of packed metadata is UnresolvedAttribute named _timeseries — still tsIdentity.
                    timeSeriesMetadataAttr = attr;
                }
        }
        // Prefer $_or_match when present; else metadata (full blob, no __name__ drop).
        if (orMatchKey != null) {
            groupings.add(orMatchKey);
        } else if (timeSeriesMetadataAttr != null) {
            groupings.add(timeSeriesMetadataAttr);
        }
        for (Attribute label : tsIdentity) {
            if (PromqlLabels.isMetadataLabel(label) == false) {
                groupings.add(UnresolvedPromqlAttribute.of(source, label));
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
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
