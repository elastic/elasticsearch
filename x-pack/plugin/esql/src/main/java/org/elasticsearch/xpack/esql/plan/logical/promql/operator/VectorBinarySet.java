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
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlLabelRef;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
        List<UnresolvedPromqlLabelRef> metadatas = new ArrayList<>();
        Source metadataSource = null;
        for (LogicalPlan plan : plans) {
            for (Attribute attr : plan.output()) {
                if (PromqlLabels.isMetadataLabel(attr)) {
                    metadatas.add((UnresolvedPromqlLabelRef) attr);
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
            for (UnresolvedPromqlLabelRef timeSeriesMetadata : metadatas) {
                if (unset == null) {
                    unset = new LinkedHashSet<>(timeSeriesMetadata.unset());
                } else {
                    unset.retainAll(timeSeriesMetadata.unset());
                }
            }
            result.add(UnresolvedPromqlLabelRef.metadata(metadataSource, unset));
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

    /** Finished UnionAll+TopNBy so parents can consume nested or. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (op != SetOp.UNION) {
            return super.translate(context);
        }
        List<LogicalPlan> branches = flattenUnion(this);
        // One limb: this or keeps every left series of a signature, so it unions a with b unless a, deduplicating nothing.
        boolean keepsLeftTies = branches.size() == 1;
        if (keepsLeftTies) {
            branches = List.of(left(), rightUnlessLeft());
        }
        List<IntermediateResult> intermediateResults = new ArrayList<>(branches.size());
        for (LogicalPlan branch : branches) {
            intermediateResults.add(context.translateIntermediate(branch, new NameId(), new NameId()));
        }
        if (keepsLeftTies) {
            return wrapUnion(context, unionBranches(context, branches, intermediateResults, output()));
        }
        return wrapUnion(context, translateUnion(context, branches, intermediateResults, output()));
    }

    /**
     * The right operand without the series the left one matches: {@code b unless<M> a} for {@code a or<M> b}. Prometheus
     * keeps every series of {@code a} and those of {@code b} whose match signature no series of {@code a} has, so the union of
     * {@code a} and this needs no deduplication.
     */
    private VectorBinarySet rightUnlessLeft() {
        return new VectorBinarySet(source(), right(), left(), match(), SetOp.SUBTRACT);
    }

    /**
     * Always rename value to kill UnionAll ghost channels (#158164).
     */
    private IntermediateResult wrapUnion(TranslationContext context, LogicalPlan union) {
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
        // Expose the step under the enclosing frame's step identity, as a join result does, so an operand sharing the
        // frame (a scalar in 2 + (a or b)) reads the union's step.
        Alias frameStep = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        plan = new Eval(source(), plan, List.of(pinned, frameStep));
        List<NamedExpression> pinnedOut = new ArrayList<>();
        pinnedOut.add(pinned.toAttribute());
        pinnedOut.add(TranslationContext.pinnedAttribute(frameStep));
        pinnedOut.addAll(labels);
        plan = new Project(source(), plan, pinnedOut);
        return new IntermediateResult(
            plan,
            pinned.toAttribute(),
            TranslationContext.pinnedAttribute(frameStep),
            null,
            Kind.AFTER_INITIAL_AGGREGATE
        );
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
        var source = cmd.source();
        UnionAll union = unionBranches(context, branches, intermediateResults, tsIdentity);
        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        Attribute orMatchKey = null;
        Attribute timeSeriesMetadataAttr = null;
        for (Attribute attr : union.output()) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.stepColumnName())) {
                groupings.add(attr);
            } else if (attr.name().equals(OR_MATCH_KEY)) {
                orMatchKey = attr;
            } else if (PromqlLabels.isMetadataLabel(attr)
                || MetadataAttribute.TIMESERIES.equals(attr.name())
                || attr.name().startsWith(MetadataAttribute.TIMESERIES + "$")) {
                    // Alias.toAttribute of packed metadata is UnresolvedAttribute named _timeseries - still tsIdentity.
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
                groupings.add(UnresolvedPromqlLabelRef.of(source, label));
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /** The branches side by side, each tagged with its position and projected onto the union's labels. */
    private static UnionAll unionBranches(
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
            // Every branch, a constant one too, lays out the union's columns: UnionAll lines them up by name.
            // A separate match key unsets __name__ (Prometheus set-op identity).
            UnresolvedPromqlLabelRef branchTimeSeriesMetadata = PromqlLabels.metadataLabel(branches.get(i).output());
            // The labels the union holds in columns compare there, so the match key leaves them out of the metadata.
            List<String> columnLabels = PromqlLabels.labelNames(
                tsIdentity.stream().filter(label -> PromqlLabels.isMetadataLabel(label) == false).toList()
            );
            for (Attribute label : tsIdentity) {
                if (PromqlLabels.isMetadataLabel(label)) {
                    if (branchTimeSeriesMetadata != null) {
                        var pack = UnresolvedPromqlLabelRef.metadata(source, branchTimeSeriesMetadata.unset());
                        projections.add(new Alias(source, MetadataAttribute.TIMESERIES, pack, label.id()));
                        Set<String> matchUnset = new LinkedHashSet<>(branchTimeSeriesMetadata.unset());
                        matchUnset.add(LabelMatcher.NAME);
                        matchUnset.addAll(columnLabels);
                        projections.add(new Alias(source, OR_MATCH_KEY, UnresolvedPromqlLabelRef.metadata(source, matchUnset)));
                    } else {
                        // A branch without metadata holds all its labels in columns: its metadata is the empty label set.
                        var empty = UnresolvedPromqlLabelRef.label(source, VectorBinaryOperator.EMPTY_SIGNATURE_KEY);
                        projections.add(new Alias(source, MetadataAttribute.TIMESERIES, empty, label.id()));
                        projections.add(new Alias(source, OR_MATCH_KEY, empty));
                    }
                } else {
                    // Null-fill a label the branch lacks: resolving it would invent by-keys (empty max by (cluster) or
                    // vector(0)). Metadata that keeps the label holds it, as the match key leaves it out there.
                    String labelName = PromqlLabels.labelName(label);
                    boolean declared = PromqlLabels.getLabel(
                        branches.get(i).output().stream().filter(a -> PromqlLabels.isMetadataLabel(a) == false).toList(),
                        labelName
                    ) != null;
                    boolean inMetadata = branchTimeSeriesMetadata != null && branchTimeSeriesMetadata.unset().contains(labelName) == false;
                    if (declared || inMetadata) {
                        // Under the union's column name: a branch may name the label after its source field (labels.pod)
                        // or bare (pod), and UnionAll lines branches up by name.
                        projections.add(new Alias(source, label.name(), UnresolvedPromqlLabelRef.of(source, label)));
                    } else {
                        Alias nullLabel = new Alias(source, label.name(), new Literal(source, null, label.dataType()), new NameId());
                        nullLabelEvals.add(nullLabel);
                        projections.add(nullLabel.toAttribute());
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
        return new UnionAll(source, branchPlans, VectorBinarySet.unionOutputByName(branchPlans));
    }

    /**
     * Left-to-right limbs of a default-matching or chain, which translation unions keeping one series per step and
     * signature: series that agree on every label but {@code __name__} agree on it. An on/ignoring or is one limb.
     */
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
