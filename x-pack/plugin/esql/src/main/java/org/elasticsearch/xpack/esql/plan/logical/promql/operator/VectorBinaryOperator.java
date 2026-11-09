/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlLabelAttribute;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

    /**
     * One side of a join: its plan, the fields it joins on (step and signature), and its value and label columns. Unlike a
     * {@link Frame}, which reads an operand's per-series aggregate to compute both operands in it without a join, a join side
     * is an operand finished as its own pipeline.
     */
    private record JoinSide(LogicalPlan plan, List<Attribute> fields, Attribute step, Attribute valueColumn, @Nullable Attribute labels) {}

    /** The join's signature column on either side. */
    private static final String SIGNATURE = "$prom_sig";
    /** The one side's labels carried across a group_x join, named apart from the many side's. */
    private static final String ONE_LABELS = "$prom_one";

    /**
     * Underlying binary operation (e.g. +, -, *, /, etc.) being performed
     * on the actual values of the vectors.
     */
    public interface BinaryOp {
        String name();

        ScalarFunctionFactory asFunction();
    }

    public interface ScalarFunctionFactory {
        Function create(Source source, Expression left, Expression right, Configuration configuration);
    }

    protected VectorBinaryOperator(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        VectorMatch match,
        boolean dropMetricName,
        BinaryOp binaryOp
    ) {
        super(source, left, right);
        this.match = Objects.requireNonNull(match, "match must be VectorMatch.NONE rather than null");
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    /** The declared vector matching; {@link VectorMatch#NONE} - never null - when the operator declares none. */
    public VectorMatch match() {
        return match;
    }

    /** Whether this operator drops {@code __name__} from the result identity (arithmetic and {@code bool} comparisons). */
    public boolean dropMetricName() {
        return dropMetricName;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = computeOutputAttributes();
        }
        return output;
    }

    /**
     * Result identity for vector x vector / vector x scalar matching. Scalar x vector keeps the vector's labels (with
     * {@code __name__} dropped when {@link #dropMetricName}); group_left/right and ignoring reshape the label container; default
     * match keeps the left identity when label sets agree.
     */
    private List<Attribute> computeOutputAttributes() {
        // Instant vector x scalar: apply the operator to every sample; identity is the vector's (scalars have none).
        if (PromqlPlan.returnsScalar(left())) {
            return withoutMetricName(right().output());
        }
        if (PromqlPlan.returnsScalar(right())) {
            return withoutMetricName(left().output());
        }

        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = labelSet(leftAttrs);
        Set<String> rightLabels = labelSet(rightAttrs);

        if (match.grouping() != Joining.NONE) {
            boolean manyLeft = match.grouping() == Joining.LEFT;
            if (PromqlLabels.getTagsLabel(manyLeft ? leftAttrs : rightAttrs) != null) {
                return groupedOverOpaqueMany(manyLeft ? leftAttrs : rightAttrs, manyLeft ? rightAttrs : leftAttrs);
            }
        }

        // Labels translation materializes even when neither operand declares them as attributes (opaque identity
        // packed into `_timeseries`): on(...) keys and group_x includes. Missing ones become synthesized refs.
        Set<String> guaranteed = new HashSet<>();
        Set<String> outputLabels;
        if (match.grouping() == Joining.LEFT) {
            // group_left keeps the left ("many") label set and copies only explicitly included labels from the right.
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.grouping() == Joining.RIGHT) {
            // group_right keeps the right ("many") label set and copies only explicitly included labels from the left.
            outputLabels = new HashSet<>(rightLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.filter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
            guaranteed.addAll(match.filterLabels());
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            return ignoringOutput(leftAttrs);
        } else if (leftLabels.equals(rightLabels)) {
            // Same label set: left identity; dropMetricName still applies (Prometheus implicit ignoring(__name__)).
            return withoutMetricName(leftAttrs);
        } else {
            // Differing concrete sets: only absent-on-both pairs match; result labels are left's.
            outputLabels = new HashSet<>(leftLabels);
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
        }
        List<Attribute> result = new ArrayList<>();
        for (String label : outputLabels) {
            Attribute attr = PromqlLabels.getLabel(leftAttrs, label);
            if (attr == null) {
                attr = PromqlLabels.getLabel(rightAttrs, label);
            }
            if (attr != null) {
                result.add(attr);
            } else if (guaranteed.contains(label)) {
                result.add(new ReferenceAttribute(source(), label, DataType.KEYWORD));
            }
        }
        return result;
    }

    /** Ignoring is WITHOUT-shaped: it drops the ignored labels (and {@code __name__} when {@link #dropMetricName}). */
    private List<Attribute> ignoringOutput(List<Attribute> leftAttrs) {
        var ignored = ignored();
        var result = new ArrayList<Attribute>();
        if (PromqlLabels.getTagsLabel(leftAttrs) != null) {
            result.add(UnresolvedPromqlLabelAttribute.newTagsLabel(source()));
        }
        for (Attribute attr : leftAttrs) {
            if (PromqlLabels.isTagsLabel(attr) == false && ignored.contains(PromqlLabels.labelName(attr)) == false) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Opaque many-side under group_left/right: includes come from the one side (null if absent); prior promoted
     * labels on many stay - already promoted beside the label container and must not be dropped.
     */
    private List<Attribute> groupedOverOpaqueMany(List<Attribute> many, List<Attribute> one) {
        var result = new ArrayList<Attribute>();
        result.add(UnresolvedPromqlLabelAttribute.newTagsLabel(source()));
        for (Attribute attr : many) {
            if (PromqlLabels.isTagsLabel(attr)) {
                continue;
            }
            String name = PromqlLabels.labelName(attr);
            if (match.groupingLabels().contains(name) || (dropMetricName && LabelMatcher.NAME.equals(name))) {
                continue;
            }
            result.add(attr);
        }
        for (String label : match.groupingLabels()) {
            Attribute attr = PromqlLabels.getLabel(one, label);
            result.add(attr != null ? attr : new ReferenceAttribute(source(), label, DataType.KEYWORD));
        }
        return result;
    }

    /** The labels an ignoring match drops: the ignored ones, and {@code __name__} when {@link #dropMetricName}. */
    private Set<String> ignored() {
        var ignored = new LinkedHashSet<>(match.filterLabels());
        if (dropMetricName) {
            ignored.add(LabelMatcher.NAME);
        }
        return ignored;
    }

    /**
     * What the result's tags label leaves out, as {@link #output()} declares it: the vector operand's, the many side's under
     * group_left/right with the labels it copies, and the left operand's otherwise, each with the dropped metric name.
     */
    @Override
    public Set<String> excludedLabels() {
        Set<String> name = dropMetricName ? Set.of(LabelMatcher.NAME) : Set.of();
        if (PromqlPlan.returnsScalar(left())) {
            return PromqlLabels.excludedWith(right(), name);
        }
        if (PromqlPlan.returnsScalar(right())) {
            return PromqlLabels.excludedWith(left(), name);
        }
        if (match.grouping() != Joining.NONE) {
            LogicalPlan many = match.grouping() == Joining.LEFT ? left() : right();
            var removed = new LinkedHashSet<>(match.groupingLabels());
            removed.addAll(name);
            return PromqlLabels.excludedWith(many, removed);
        }
        if (match.filter() == VectorMatch.Filter.IGNORING) {
            return PromqlLabels.excludedWith(left(), ignored());
        }
        return PromqlLabels.excludedWith(left(), name);
    }

    /**
     * Whether both operands declare concrete label sets that differ by name. Prometheus default matching compares each
     * pair's actual label sets, so such operands cannot fold into one aggregate over shared grouping keys; they
     * translate as a join, which yields the empty vector unless the differing labels are null on both sides.
     */
    public boolean hasMismatchedLabelSets() {
        if (match != VectorMatch.NONE || PromqlPlan.returnsScalar(left()) || PromqlPlan.returnsScalar(right())) {
            return false;
        }
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        if (PromqlLabels.getTagsLabel(leftAttrs) != null || PromqlLabels.getTagsLabel(rightAttrs) != null) {
            return false;
        }
        return labelSet(leftAttrs).equals(labelSet(rightAttrs)) == false;
    }

    /** The canonical names of an operand's labels. */
    private static Set<String> labelSet(List<Attribute> attrs) {
        return attrs.stream().map(PromqlLabels::labelName).collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    public abstract VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight);

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o)) {
            VectorBinaryOperator that = (VectorBinaryOperator) o;
            return dropMetricName == that.dropMetricName && Objects.equals(match, that.match) && Objects.equals(binaryOp, that.binaryOp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), match, dropMetricName, binaryOp);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public PromqlDataType returnType() {
        PromqlDataType leftType = PromqlPlan.getType(left());
        PromqlDataType rightType = PromqlPlan.getType(right());
        // scalar op scalar → scalar; otherwise → vector
        if (leftType == SCALAR && rightType == SCALAR) {
            return SCALAR;
        }
        return PromqlDataType.INSTANT_VECTOR;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Matches and merges two operands' series identities: a relabel below either operand feeds this boundary.
        return false;
    }

    /**
     * Join is the general case; the fold is an optimisation. {@link #requiresIndependentPipelines} decides up front only
     * what fixes the operands' translation context; everything else is decided once, from the translated operands, in
     * {@link #translateOverSharedFrame}. Scalar step-broadcast is a join shape decided inside the join translator - not
     * a third mode.
     */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (requiresIndependentPipelines()) {
            return translateAsLeftJoin(context);
        }
        return translateOverSharedFrame(context);
    }

    /**
     * Whether the operands must translate as independent pipelines before anything is known about their plans - the
     * properties that pick the translation context rather than describe the result: explicit vector matching, a
     * source-backed scalar (which broadcasts on {@code step}), and selectors disagreeing on {@code offset} (one shared
     * TimeSeriesAggregate carries a single evaluation timestamp; each independent pipeline shifts {@code @timestamp}
     * for its own selectors). Pure scalars may share the other side's frame. Set operators override for force-fold.
     */
    protected boolean requiresIndependentPipelines() {
        if (match().filter() != VectorMatch.Filter.NONE || match().grouping() != Joining.NONE) {
            return true;
        }
        boolean leftScalar = PromqlPlan.returnsScalar(left());
        boolean rightScalar = PromqlPlan.returnsScalar(right());
        if (leftScalar || rightScalar) {
            boolean shared = isPureScalar(left()) && isPureScalar(right())
                || isPureScalar(left()) && rightScalar == false
                || isPureScalar(right()) && leftScalar == false;
            return shared == false;
        }
        return hasHeterogeneousOffsets();
    }

    /**
     * Scalar with no source-backed selector: a literal, {@code time()}, {@code pi()}, or arithmetic over those.
     * Contrasts with {@code scalar(sum(...))}, which collapses source data and must broadcast independently.
     */
    private static boolean isPureScalar(LogicalPlan plan) {
        return PromqlPlan.returnsScalar(plan) && hasSourceBackedExpression(plan) == false;
    }

    private static boolean hasSourceBackedExpression(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Selector && (p instanceof LiteralSelector) == false);
    }

    protected static boolean aggregatesAcrossSeries(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof AcrossSeriesAggregate || p instanceof AcrossSeriesReduction || p instanceof ScalarConversionFunction
        );
    }

    /**
     * Whether the two operands' source-backed selectors disagree on {@code offset}. Literal selectors contribute none.
     * Used to refuse fold (one shared timestamp) while still admitting the shape via join.
     */
    public boolean hasHeterogeneousOffsets() {
        return PromqlCommand.distinctOffsets(left()).equals(PromqlCommand.distinctOffsets(right())) == false;
    }

    /**
     * Whether the operands read disjoint series, in which case no shared frame can pair them: opaque operands for
     * different metrics on a mapping that stores {@code __name__} as a dimension live in separate series, so a fold
     * yields no rows. This is a property of the data layout, not of the translated plans, so it is the one semantic
     * precondition the structural fold check cannot derive. They join on the label container excluding {@code __name__}.
     *
     * @param source the plan the command reads, whose dimensions say whether {@code __name__} is stored
     */
    public boolean readsDisjointSeries(LogicalPlan source) {
        if (PromqlLabels.getTagsLabel(left().output()) == null && PromqlLabels.getTagsLabel(right().output()) == null) {
            return false;
        }
        Set<String> leftNames = metricNames(left());
        Set<String> rightNames = metricNames(right());
        if (leftNames.isEmpty() || rightNames.isEmpty() || leftNames.equals(rightNames)) {
            return false;
        }
        return source.output()
            .stream()
            .anyMatch(a -> a instanceof FieldAttribute f && f.isDimension() && LabelMatcher.NAME.equals(PromqlLabels.labelName(f)));
    }

    private static Set<String> metricNames(LogicalPlan plan) {
        Set<String> names = new HashSet<>();
        for (Selector selector : plan.collect(Selector.class)) {
            if (selector instanceof LiteralSelector == false && selector.series() instanceof Attribute series) {
                names.add(series.name());
            }
        }
        return names;
    }

    /**
     * Translates both operands under this frame, then decides once: the operator folds when both operands turn out to
     * share the frame - both still raw over the shared source, or both one {@link TimeSeriesAggregate} over it with the
     * same grain (see {@link #frame}) - and otherwise joins the independent pipelines. A pure scalar shares whichever
     * frame the other operand has. Filter-mode {@code v > lit} is specialized here so parents that call translate
     * (label_replace, avg, topk, ...) see an already-filtered vector.
     */
    private IntermediateResult translateOverSharedFrame(TranslationContext context) {
        IntermediateResult left = context.translate(left());
        if (this instanceof VectorBinaryComparison comp && comp.filterMode() && right() instanceof LiteralSelector lit) {
            // Filter-mode comparison (v > lit): keep the left value, drop non-matching rows.
            IntermediateResult table = left.kind().afterInitialAggregation ? left : context.emitInitialAggregate(left, left.value());
            Expression value = new ToDouble(table.value().source(), table.value());
            Expression threshold = new ToDouble(lit.source(), lit.literal());
            Expression condition = comp.op().asFunction().create(source(), value, threshold, context.configuration());
            return table.with(new Filter(source(), table.plan(), condition), value);
        }

        IntermediateResult right = context.translate(right());
        // The operands as translated: the join fallback finishes these as independent pipelines rather than translating again.
        IntermediateResult leftTranslated = left;
        IntermediateResult rightTranslated = right;
        boolean scalarOperand = PromqlPlan.returnsScalar(left()) || PromqlPlan.returnsScalar(right());
        // Filter-mode vector comparison must finish the initial per-series aggregate before filtering: a pending
        // comparison would be pushed above the source (outside STATS).
        boolean filterModeVector = this instanceof VectorBinaryComparison comp
            && comp.filterMode()
            && right() instanceof LiteralSelector == false;
        // A collapsed vector beside a raw one: collapse the raw one too, so the two frames can be compared (folding as is
        // would embed the raw side's pending aggregate in an Eval outside STATS). A pure scalar needs no frame of its own.
        boolean mixed = left.kind().afterInitialAggregation != right.kind().afterInitialAggregation;
        if (filterModeVector || (mixed && scalarOperand == false)) {
            if (left.kind().afterInitialAggregation == false) {
                left = context.emitInitialAggregate(left, left.value());
            }
            if (right.kind().afterInitialAggregation == false) {
                right = context.emitInitialAggregate(right, right.value());
            }
        }
        boolean collapsed = left.kind().afterInitialAggregation && right.kind().afterInitialAggregation;
        if (collapsed && scalarOperand) {
            // Scalar operand over collapsed tables: broadcast on step alone.
            return translateAsBroadcastJoin(context, left, right);
        }

        LogicalPlan plan;
        Expression filter;
        if (collapsed) {
            Frame leftFrame = frame(context, left);
            Frame rightFrame = frame(context, right);
            if (leftFrame == null
                || rightFrame == null
                || leftFrame.sharesGrainWith(rightFrame) == false
                || readsDisjointSeries(context.cmd().child())) {
                return translateAsLeftJoin(context, leftTranslated, rightTranslated);
            }
            plan = emitBinaryOperatorAggregateExpression(left, leftFrame, right, rightFrame);
            if (rightFrame.filter() != null) {
                if (this instanceof VectorBinarySet) {
                    // Set ops read right presence via IS NULL / IS NOT NULL. Null the right value where its filter
                    // fails so absence stays visible; a Filter would drop the row and can strand the left value
                    // under later opts ("missing references").
                    Alias guarded = new Alias(
                        source(),
                        context.cmd().valueColumnName(),
                        new Case(source(), rightFrame.filter(), List.of(right.value()))
                    );
                    plan = new Eval(context.cmd().source(), plan, List.of(guarded));
                    right = right.with(plan, guarded.toAttribute());
                } else {
                    plan = new Filter(source(), plan, rightFrame.filter());
                }
            }
            filter = null;
        } else if (left.plan() == right.plan() && readsDisjointSeries(context.cmd().child()) == false) {
            // Both raw over the shared source (a pure scalar reads it too): the expressions compose; the frame is still
            // pending and the selector predicates stay deferred with it.
            plan = left.plan();
            // unless: never AND the right selector onto the scan - an empty right matcher would wipe left
            // (promfuzz-vm 981). Null the right value where its filter fails so Case(IS NULL, left) keeps left.
            if (this instanceof VectorBinarySet set && set.op() == VectorBinarySet.SetOp.SUBTRACT) {
                filter = left.pendingFilter();
                if (right.pendingFilter() != null) {
                    Alias guarded = new Alias(
                        source(),
                        context.cmd().valueColumnName(),
                        new Case(source(), right.pendingFilter(), List.of(right.value()))
                    );
                    plan = new Eval(context.cmd().source(), plan, List.of(guarded));
                    right = right.with(plan, guarded.toAttribute());
                }
            } else {
                filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
            }
        } else if (mixed && scalarOperand) {
            // A pure scalar over a collapsed vector: the expression composes over the vector's table.
            plan = left.kind().afterInitialAggregation ? left.plan() : right.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        } else {
            return translateAsLeftJoin(context, leftTranslated, rightTranslated);
        }

        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        Attribute step = left.step() != null ? left.step() : right.step();
        // Scalar x UnionAll (e.g. {@code (a or b) * 2}) composes an Eval over the union. Eval name-shadowing can leave a
        // channel absent from output(); later Projects then throw {@code Cannot project block} (#158164). Pin the
        // vector side before building the binary - the same materialization joins already apply. A shared-frame fold
        // of two aggregates has no UnionAll here ({@link #frame} refuses it), so this does not collapse two values.
        if (kind.afterInitialAggregation && plan.anyMatch(UnionAll.class::isInstance)) {
            IntermediateResult vector = left.kind().afterInitialAggregation ? left : right;
            IntermediateResult pinned = pinOperandLayout(
                context,
                new IntermediateResult(plan, vector.value(), vector.labels(), step, filter, kind)
            );
            plan = pinned.plan();
            step = pinned.step();
            if (left.kind().afterInitialAggregation) {
                left = left.with(plan, pinned.value()).withLabels(pinned.labels());
            } else {
                right = right.with(plan, pinned.value()).withLabels(pinned.labels());
            }
        }

        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        Expression binaryExpr = binaryOp().asFunction().create(source(), leftExpr, rightExpr, context.configuration());
        if (filterModeVector) {
            plan = new Filter(source(), plan, binaryExpr);
            // A filter keeps the vector's value, wherever the scalar sits, as Prometheus does.
            binaryExpr = PromqlPlan.returnsScalar(left()) ? rightExpr : leftExpr;
        }
        IntermediateResult vector = PromqlPlan.returnsScalar(left()) ? right : left;
        IntermediateResult result = new IntermediateResult(plan, null, resultLabels(vector.labels(), null), step, filter, kind);
        return context.eval(result, binaryExpr);
    }

    /**
     * A collapsed operand's frame: the one {@link TimeSeriesAggregate} it computes directly over the shared source, the
     * value-only {@link Eval}s stacked on it (top-down), the conjunction of the {@link Filter}s stacked on it, and
     * whether it aggregates across series. Two frames share a grain when they group the same way and both collapse
     * per series or both across them - [step] alone means per series under a raw collapse and across all series under
     * {@code sum(...)}, so the grouping names alone do not tell them apart.
     */
    private record Frame(TimeSeriesAggregate aggregate, List<Eval> evals, Expression filter, boolean acrossSeries) {
        boolean sharesGrainWith(Frame other) {
            return acrossSeries == other.acrossSeries && sameLabels(aggregate, other.aggregate);
        }
    }

    /**
     * The frame of a collapsed operand, or null when the operand is not one TimeSeriesAggregate over the shared source
     * under value-only Evals and Filters: a regrouping Aggregate, a join, a union, a relabel (an Eval defining a label
     * column changes tsIdentity), a {@code topk} ranking, or anything else stacked on it leaves the frame.
     */
    private static Frame frame(TranslationContext context, IntermediateResult input) {
        LogicalPlan plan = input.plan();
        List<Eval> evals = new ArrayList<>();
        Expression filter = null;
        while (true) {
            if (plan instanceof Filter f) {
                filter = combineAndNullable(Arrays.asList(filter, f.condition()));
                plan = f.child();
            } else if (plan instanceof Eval eval && definesValueOnly(context, eval)) {
                evals.add(eval);
                plan = eval.child();
            } else {
                break;
            }
        }
        if (plan instanceof TimeSeriesAggregate aggregate && aggregate.child() == context.cmd().child()) {
            return new Frame(aggregate, evals, filter, aggregatesAcrossSeries(aggregate));
        }
        return null;
    }

    /** Whether every field an Eval defines is the sample value - a relabel defines label columns instead. */
    private static boolean definesValueOnly(TranslationContext context, Eval eval) {
        return eval.fields().stream().allMatch(field -> field.name().equals(context.cmd().valueColumnName()));
    }

    /**
     * Whether a collapse aggregates across series: its aggregates apply an {@link AggregateFunction} other than the
     * within-series {@link TimeSeriesAggregateFunction}s (e.g. {@code SUM(rate(...))} against {@code rate(...)} alone).
     */
    private static boolean aggregatesAcrossSeries(TimeSeriesAggregate aggregate) {
        return aggregate.aggregates()
            .stream()
            .anyMatch(
                e -> e instanceof Alias alias
                    && alias.child().anyMatch(f -> f instanceof AggregateFunction && (f instanceof TimeSeriesAggregateFunction) == false)
            );
    }

    /**
     * Broadcasts a scalar operand over a vector (or another scalar) by joining on {@code step} alone. The scalar is the
     * build (one row per step); the vector is the probe and keeps its identities. {@code unique} is false: many probe
     * series share a step with one scalar build row (many-to-one). Result labels are the vector's (empty when both are
     * scalars).
     */
    private IntermediateResult translateAsBroadcastJoin(TranslationContext context, IntermediateResult left, IntermediateResult right) {
        boolean leftScalar = PromqlPlan.returnsScalar(left());
        IntermediateResult probe = leftScalar ? right : left;
        IntermediateResult build = reidentify(context, leftScalar ? left : right);
        // Step-only join: no PackDims join-key needs, so pin on the IntermediateResult is safe.
        if (needsLayoutPin(probe)) {
            probe = pinOperandLayout(context, probe);
        }
        if (needsLayoutPin(build)) {
            build = pinOperandLayout(context, build);
        }
        Expression leftValue = leftScalar ? build.value() : probe.value();
        Expression rightValue = leftScalar ? probe.value() : build.value();
        LogicalPlan join = new HashJoin(
            context.cmd().source(),
            probe.plan(),
            new Project(context.cmd().source(), build.plan(), List.of(build.step(), build.valueColumn())),
            List.of(probe.step()),
            List.of(build.step()),
            List.of(build.valueColumn()),
            false
        );
        return bindResult(context, leftValue, rightValue, probe.step(), join, resultLabels(probe.labels(), null));
    }

    /**
     * Independent pipelines joined afterwards. A scalar operand broadcasts on {@code step} alone; two vectors join on
     * {@code step} + match keys. The join reads keys and result labels from the operands as references: a label comes
     * back as a concrete column wherever the operand can carry it, and a label the operand dropped binds to null.
     * <p>
     * For vector x vector: how the sides are ordered (probe vs build), how fields are placed (match key packed next to
     * step, build value and {@code group_x} labels carried across), and what is projected. The result exposes its step
     * under the enclosing translation's step identity and only the labels it declares.
     */
    public IntermediateResult translateAsLeftJoin(TranslationContext context) {
        return joinIndependentPipelines(
            context,
            context.translateIntermediate(left(), new NameId(), new NameId()),
            context.translateIntermediate(right(), new NameId(), new NameId())
        );
    }

    /** The join over operands already translated under {@code context}'s frame, finished as independent pipelines. */
    private IntermediateResult translateAsLeftJoin(TranslationContext context, IntermediateResult left, IntermediateResult right) {
        return joinIndependentPipelines(
            context,
            context.finishIntermediate(left(), left, new NameId(), new NameId()),
            context.finishIntermediate(right(), right, new NameId(), new NameId())
        );
    }

    private IntermediateResult joinIndependentPipelines(TranslationContext context, IntermediateResult left, IntermediateResult right) {
        // Source-backed scalar() x vector (and scalar x scalar): step-only broadcast - still this join mode, not a fold.
        if (PromqlPlan.returnsScalar(left()) || PromqlPlan.returnsScalar(right())) {
            return translateAsBroadcastJoin(context, left, right);
        }
        // Orientation: the many side probes and keeps its identities; the one side builds and is re-identified, so a
        // self-join of structurally identical operands has distinct attributes on each side.
        boolean probeRight = match.grouping() == Joining.RIGHT;
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = reidentify(context, probeRight ? left : right);
        JoinSide probeSide = emitJoinSide(context, probe, false);
        JoinSide buildSide = emitJoinSide(context, build, true);
        Expression leftValue = probeRight ? buildSide.valueColumn() : probeSide.valueColumn();
        Expression rightValue = probeRight ? probeSide.valueColumn() : buildSide.valueColumn();
        LogicalPlan join = emitHashJoin(context, probeSide, buildSide);
        return bindResult(context, leftValue, rightValue, probeSide.step(), join, resultLabels(probeSide.labels(), buildSide.labels()));
    }

    /**
     * Prometheus' {@code resultMetric}: the many side's labels, without the metric name for arithmetic and {@code bool},
     * with only the {@code on(...)} labels or without the {@code ignoring(...)} ones when one-to-one, and with each
     * {@code group_x} included label written from the one side's. Set operators keep the left series as they are.
     */
    private @Nullable Expression resultLabels(@Nullable Expression many, @Nullable Expression one) {
        if (this instanceof VectorBinarySet) {
            return many;
        }
        Expression labels = many;
        if (dropMetricName && labels != null) {
            labels = new TimeSeriesMetadataFilter(
                labels.source(),
                labels,
                List.of(Literal.keyword(labels.source(), LabelMatcher.NAME)),
                true
            );
        }
        if (match.grouping() == Joining.NONE && labels != null) {
            if (match.filter() == VectorMatch.Filter.ON) {
                labels = new TimeSeriesMetadataFilter(
                    labels.source(),
                    labels,
                    PromqlLabels.labelLiterals(labels.source(), match.filterLabels()),
                    false
                );
            } else if (match.filter() == VectorMatch.Filter.IGNORING) {
                labels = new TimeSeriesMetadataFilter(
                    labels.source(),
                    labels,
                    PromqlLabels.labelLiterals(labels.source(), match.filterLabels()),
                    true
                );
            }
        }
        for (String included : match.groupingLabels()) {
            if (labels == null) {
                labels = VectorBinarySet.emptyLabels(source());
            }
            Expression value = one == null
                ? new Literal(source(), null, DataType.KEYWORD)
                : new TimeSeriesMetadataPath(one.source(), one, Literal.keyword(one.source(), included));
            labels = new TimeSeriesMetadataSet(labels.source(), labels, Literal.keyword(labels.source(), included), value);
        }
        return labels;
    }

    /** The labels two series match on: Prometheus' signature. */
    private Expression signature(@Nullable Expression labels) {
        if (labels == null) {
            return VectorBinarySet.emptyLabels(source());
        }
        if (match.filter() == VectorMatch.Filter.ON) {
            return new TimeSeriesMetadataFilter(
                labels.source(),
                labels,
                PromqlLabels.labelLiterals(labels.source(), match.filterLabels()),
                false
            );
        }
        var ignored = new LinkedHashSet<String>(match.filterLabels());
        ignored.add(LabelMatcher.NAME);
        return new TimeSeriesMetadataFilter(labels.source(), labels, PromqlLabels.labelLiterals(labels.source(), ignored), true);
    }

    /** Whether an operand's pipeline is prone to Eval ghost channels that HashJoin cannot map (#158164). */
    private static boolean needsLayoutPin(IntermediateResult input) {
        return input.plan().anyMatch(UnionAll.class::isInstance)
            || input.valueColumn() instanceof UnresolvedAttribute
            || input.plan().collect(Aggregate.class).size() > 1;
    }

    /**
     * Pins an operand's page layout to a freshly named {@code value}, {@code step}, and labels only. Eval name-shadowing
     * can leave a channel in the page that is absent from {@code output()} (#158164); HashJoin's block mapping then
     * indexes past the page ({@code Cannot project block}). A rename-Eval plus Project forces materialization so a
     * no-op Project of the same attributes cannot be optimized away while ghosts remain in the page.
     */
    private static IntermediateResult pinOperandLayout(TranslationContext context, IntermediateResult input) {
        PromqlCommand cmd = context.cmd();
        String pinName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        Alias pinned = new Alias(input.valueColumn().source(), pinName, input.valueColumn(), new NameId());
        LogicalPlan plan = new Eval(cmd.source(), input.plan(), List.of(pinned));
        Attribute step = input.step();
        List<NamedExpression> projected = new ArrayList<>();
        projected.add(pinned.toAttribute());
        projected.add(step);
        for (Attribute attr : input.plan().output()) {
            if (attr.id().equals(input.valueColumn().id()) || attr.id().equals(step.id())) {
                continue;
            }
            if (attr.name().equals(cmd.valueColumnName())
                || attr.name().equals(cmd.stepColumnName())
                || attr.name().equals(cmd.branchColumnName())
                || attr.name().equals(pinName)) {
                continue;
            }
            projected.add(attr);
        }
        plan = new Project(cmd.source(), plan, projected);
        return new IntermediateResult(plan, pinned.toAttribute(), input.labels(), step, input.pendingFilter(), input.kind());
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private IntermediateResult bindResult(
        TranslationContext context,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        @Nullable Expression labels
    ) {
        PromqlCommand cmd = context.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp().asFunction().create(source(), lhsExpr, rhsExpr, context.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            // A filter keeps the vector's value, wherever the scalar sits, as Prometheus does.
            Expression kept = PromqlPlan.returnsScalar(left()) ? rhsExpr : lhsExpr;
            value = comparison.filterMode() ? kept : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        Alias labelsAlias = labels == null ? null : new Alias(source(), MetadataAttribute.TIMESERIES, labels, new NameId());
        LogicalPlan plan = new Eval(
            cmd.source(),
            join,
            labelsAlias == null ? List.of(valueAlias, stepAlias) : List.of(valueAlias, stepAlias, labelsAlias)
        );
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        // pinnedAttribute keeps Alias id while value is unresolved - toAttribute snapshots never rebind in Projects.
        Attribute valueAttr = TranslationContext.pinnedAttribute(valueAlias);
        Attribute stepAttr = TranslationContext.pinnedAttribute(stepAlias);
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAttr, stepAttr));
        Attribute labelsAttr = labelsAlias == null ? null : labelsAlias.toAttribute();
        if (labelsAttr != null) {
            projected.add(labelsAttr);
        }
        plan = new Project(cmd.source(), plan, projected);

        return new IntermediateResult(plan, valueAttr, labelsAttr, stepAttr, null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The build operand under fresh identities, so a self-join of structurally identical operands has distinct
     * attributes on each side. Its value column is also renamed: {@link HashJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private static IntermediateResult reidentify(TranslationContext context, IntermediateResult input) {
        PromqlCommand cmd = context.cmd();
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        // A nested {@code or} is UnionAll + TopNBy with tightly coupled attribute ids. Rewriting those subtrees
        // (or even re-iding the step column projected above them) desyncs the dedup keys. Nested across-series
        // keep a value Alias Bind has not resolved yet - renaming in-place drops the column. Eval-wrap only;
        // keep the child value so the Alias resolves. {@link #emitHashJoin} / scalar-broadcast Project to the
        // columns they need (projecting here previously dropped a channel and broke page layout at runtime).
        if (input.plan().anyMatch(UnionAll.class::isInstance)
            || input.valueColumn() instanceof UnresolvedAttribute
            || input.plan().collect(Aggregate.class).size() > 1) {
            Alias renamedValue = new Alias(input.valueColumn().source(), valueName, input.valueColumn(), new NameId());
            LogicalPlan plan = new Eval(cmd.source(), input.plan(), List.of(renamedValue));
            return input.with(plan, renamedValue.toAttribute());
        }
        LogicalPlan plan = input.plan()
            .transformExpressionsDown(Expression.class, e -> reidExpr(renamed(e, cmd.valueColumnName(), valueName), ids));
        Expression value = reidExpr(renamed(input.valueColumn(), cmd.valueColumnName(), valueName), ids);
        Attribute step = (Attribute) reidExpr(input.step(), ids);
        Expression labels = input.labels() == null ? null : input.labels().transformDown(Expression.class, e -> reidExpr(e, ids));
        return new IntermediateResult(plan, value, labels, step, input.pendingFilter(), input.kind());
    }

    /**
     * The inner join of two already-packed (and optionally pinned) inputs on step plus the match key.
     * <p>
     * A set operator matches many-to-many: {@code and} keeps each left series once when any right series shares its
     * signature. The build side is reduced to one row per key so a left row matches at most once, and the join does not
     * demand one-to-one matching. Arithmetic and comparison stay one-to-one unless a group modifier says otherwise.
     */
    private LogicalPlan emitHashJoin(TranslationContext context, JoinSide probe, JoinSide build) {
        PromqlCommand cmd = context.cmd();
        // The build side carries its join fields plus what the join adds: its value and the group_x labels. Neither can
        // already be a join field (the step, or the freshly packed key), so the two lists are disjoint.
        List<Attribute> added = new ArrayList<>(List.of(build.valueColumn()));
        if (match.groupingLabels().isEmpty() == false && build.labels() != null) {
            added.add(build.labels());
        }
        List<NamedExpression> projection = new ArrayList<>(build.fields());
        projection.addAll(added);
        LogicalPlan buildPlan = new Project(cmd.source(), build.plan(), projection);
        boolean setOperator = this instanceof VectorBinarySet;
        if (setOperator) {
            var order = new Order(cmd.source(), build.valueColumn(), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
            buildPlan = new TopNBy(
                cmd.source(),
                buildPlan,
                List.of(order),
                new Literal(cmd.source(), 1, DataType.INTEGER),
                new ArrayList<>(build.fields())
            );
        }

        // unless: LEFT so an empty / non-matching build keeps probe rows; Case(IS NULL(right), left) then
        // drops matches. INNER would wipe the left when the right selector matches nothing (promfuzz-vm 981).
        JoinType type = this instanceof VectorBinarySet set && set.op() == VectorBinarySet.SetOp.SUBTRACT
            ? JoinTypes.LEFT
            : JoinTypes.INNER;
        return new HashJoin(
            cmd.source(),
            probe.plan(),
            buildPlan,
            probe.fields(),
            build.fields(),
            added,
            setOperator == false && match.grouping() == Joining.NONE,
            type
        );
    }

    /**
     * One side's plan with its signature next to step, then layout-pinned when needed. The one side's labels move under a
     * name of their own, as the join merges its output by name.
     */
    private JoinSide emitJoinSide(TranslationContext context, IntermediateResult input, boolean oneSide) {
        PromqlCommand cmd = context.cmd();
        var evals = new ArrayList<Alias>();
        // The join keys by the signature as a whole; label pushdown splits it into the columns the join compares.
        Alias signature = new Alias(cmd.source(), SIGNATURE, signature(input.labels()));
        evals.add(signature);
        Attribute labels = null;
        if (input.labels() instanceof Attribute attribute && oneSide == false) {
            labels = attribute;
        } else if (input.labels() != null) {
            Alias alias = new Alias(cmd.source(), oneSide ? ONE_LABELS : MetadataAttribute.TIMESERIES, input.labels());
            evals.add(alias);
            labels = alias.toAttribute();
        }
        LogicalPlan plan = new Eval(cmd.source(), input.plan(), evals);
        List<Attribute> fields = List.of(input.step(), signature.toAttribute());
        Attribute step = input.step();
        Attribute value = input.valueColumn();
        if (needsLayoutPin(input)) {
            IntermediateResult pinned = pinOperandLayout(
                context,
                new IntermediateResult(plan, value, labels, step, input.pendingFilter(), input.kind())
            );
            plan = pinned.plan();
            step = pinned.step();
            value = pinned.valueColumn();
            fields = remapJoinFields(fields, plan);
            if (labels != null) {
                labels = remapJoinFields(List.of(labels), plan).getFirst();
            }
        }
        return new JoinSide(plan, fields, step, value, labels);
    }

    /** Resolves join-field attributes against the (possibly pinned) plan output by name and id. */
    private static List<Attribute> remapJoinFields(List<Attribute> fields, LogicalPlan plan) {
        List<Attribute> output = plan.output();
        List<Attribute> remapped = new ArrayList<>(fields.size());
        for (Attribute field : fields) {
            Attribute found = null;
            for (Attribute attr : output) {
                if (attr.id().equals(field.id()) || attr.name().equals(field.name())) {
                    found = attr;
                    break;
                }
            }
            remapped.add(found != null ? found : field);
        }
        return remapped;
    }

    /** The declared labels without {@code __name__} for arithmetic and {@code bool}: the command's static schema. */
    private List<Attribute> withoutMetricName(List<Attribute> declared) {
        if (dropMetricName == false) {
            return declared;
        }
        if (PromqlLabels.getLabel(declared, LabelMatcher.NAME) == null) {
            return declared;
        }
        return declared.stream().filter(attr -> LabelMatcher.NAME.equals(PromqlLabels.labelName(attr)) == false).toList();
    }

    /** Renames an attribute or alias in a re-identification pass; other expressions pass through unchanged. */
    private static Expression renamed(Expression e, String from, String to) {
        // UnresolvedAttribute.withName needs dataType(); rebuild by name instead during translation.
        if (e instanceof UnresolvedAttribute ua && ua.name().equals(from)) {
            return new UnresolvedAttribute(ua.source(), ua.qualifier(), to, ua.id(), ua.unresolvedMessage());
        }
        if (e instanceof Attribute a && a.name().equals(from)) {
            return a.withName(to);
        }
        if (e instanceof Alias a && a.name().equals(from)) {
            return new Alias(a.source(), to, a.child(), a.id());
        }
        return e;
    }

    /** Re-ids a single attribute/alias (leaving other expressions untouched), reusing the shared map for consistency. */
    private static Expression reidExpr(Expression e, Map<NameId, NameId> ids) {
        if (e instanceof Attribute a) {
            return a.withId(ids.computeIfAbsent(a.id(), k -> new NameId()));
        }
        if (e instanceof Alias a) {
            return a.withId(ids.computeIfAbsent(a.id(), k -> new NameId()));
        }
        return e;
    }

    /**
     * Whether two collapses key by the same labels besides the step: the same label container expression, so that
     * {@code max without (job)} and {@code sum without (job, instance)} do not fold (promfuzz-vm 820).
     */
    private static boolean sameLabels(Aggregate left, Aggregate right) {
        List<Expression> leftKeys = left.groupings().subList(1, left.groupings().size());
        List<Expression> rightKeys = right.groupings().subList(1, right.groupings().size());
        if (leftKeys.size() != rightKeys.size()) {
            return false;
        }
        for (int i = 0; i < leftKeys.size(); i++) {
            if (Alias.unwrap(leftKeys.get(i)).semanticEquals(Alias.unwrap(rightKeys.get(i))) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Folds two operands that share a grain into one plan: the left operand's aggregate takes the right one's aggregate
     * functions as well (each under its own deferred selector predicate), and the right operand's Evals are restacked
     * on top. The right operand's Filter is applied by the caller ({@link #translateOverSharedFrame}): arithmetic drops
     * rows; set ops null the right value so presence stays visible.
     */
    private LogicalPlan emitBinaryOperatorAggregateExpression(
        IntermediateResult left,
        Frame leftFrame,
        IntermediateResult right,
        Frame rightFrame
    ) {
        var names = new TemporaryNameGenerator.Monotonic();
        TimeSeriesAggregate leftAgg = leftFrame.aggregate();
        TimeSeriesAggregate rightAgg = rightFrame.aggregate();

        var uniqueAggregates = new LinkedHashSet<Expression>();
        uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
        uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.pendingFilter()));

        // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
        // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
        // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
        var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
            if (e instanceof Alias a) {
                return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
            }
            return e;
        }).toList();

        LogicalPlan result = left.plan().transformDown(Aggregate.class, agg -> {
            if (agg == leftAgg) {
                return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
            }
            return agg;
        });

        for (Eval eval : rightFrame.evals().reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

}
