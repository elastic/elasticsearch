/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
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
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.subtract;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

    /** One side of the join: its plan with the key columns defined, and the fields the join matches on. */
    private record Input(LogicalPlan plan, List<Attribute> fields) {}

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

    private List<Attribute> computeOutputAttributes() {
        // Between an instant vector and a scalar,
        // the operator is applied to the value of every data sample in the vector.
        // Therefore, we're returning any grouping attributes (like those created for by (...) and _timeseries) from the vector.
        // If both the left and right are a scalar, this works, too
        // as both outputs will be empty (scalars don't have any grouping attributes).
        if (PromqlPlan.returnsScalar(left())) {
            return dropMetricName ? withoutMetricNameColumns(right().output()) : right().output();
        }
        if (PromqlPlan.returnsScalar(right())) {
            return dropMetricName ? withoutMetricNameColumns(left().output()) : left().output();
        }
        Set<String> outputLabels;
        // Labels the translation guarantees as result columns even when no operand declares them as attributes (an
        // operand can be opaque - its identity packed into `_timeseries` - and the translation materializes or
        // null-fills the columns): the match keys named by on(...) and the group modifier labels. Ones that resolve
        // against neither operand are declared as synthesized references.
        Set<String> guaranteed = new HashSet<>();
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        if (match.grouping() == VectorMatch.Joining.LEFT) {
            // group_left keeps the left ("many") label set and copies only explicitly included labels from the right.
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.grouping() == VectorMatch.Joining.RIGHT) {
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
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.removeAll(match.filterLabels());
        } else if (leftLabels.equals(rightLabels)) {
            // Same label set on both sides: the result carries the left operand's columns, like every other
            // one-to-one match.
            return dropMetricName ? withoutMetricNameColumns(leftAttrs) : leftAttrs;
        } else {
            // Default matching between different label sets: a pair matches only where the labels one side lacks are
            // absent on the other side too (a Prometheus signature has no entry for an absent label), and like every
            // one-to-one match the result carries the left operand's labels.
            outputLabels = new HashSet<>(leftLabels);
        }

        List<Attribute> result = new ArrayList<>();
        for (String label : outputLabels) {
            Attribute attr = findAttribute(label, leftAttrs, rightAttrs);
            if (attr != null) {
                result.add(attr);
            } else if (guaranteed.contains(label)) {
                result.add(new ReferenceAttribute(source(), label, DataType.KEYWORD));
            }
        }
        if (dropMetricName) {
            result.removeIf(VectorBinaryOperator::isMetricNameColumn);
        }

        return result;
    }

    /**
     * The columns without the metric name, however the relation spells it ({@code __name__} or a passthrough
     * {@code labels.__name__}). Arithmetic and {@code bool} comparisons drop the metric name from the result,
     * whichever operand carried it.
     */
    private static List<Attribute> withoutMetricNameColumns(List<Attribute> columns) {
        return columns.stream().filter(attribute -> isMetricNameColumn(attribute) == false).toList();
    }

    private static boolean isMetricNameColumn(Attribute attribute) {
        return LabelMatcher.NAME.equals(PromqlLabels.labelName(attribute));
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
        if (hasPackedLabels(leftAttrs) || hasPackedLabels(rightAttrs)) {
            return false;
        }
        return extractLabelNames(leftAttrs).equals(extractLabelNames(rightAttrs)) == false;
    }

    /** Whether the operand carries a packed {@code _timeseries} column, i.e. does not name every label it exposes. */
    private static boolean hasPackedLabels(List<Attribute> attrs) {
        return attrs.stream().anyMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
    }

    private Set<String> extractLabelNames(List<Attribute> attrs) {
        Set<String> labels = new HashSet<>();
        for (Attribute attr : attrs) {
            String name = attr.name();
            if (name.equals("value") == false) {
                labels.add(name);
            }
        }
        return labels;
    }

    private Attribute findAttribute(String name, List<Attribute> left, List<Attribute> right) {
        for (Attribute attr : left) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        for (Attribute attr : right) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        return null;
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
        PromqlDataType leftType = getType(left());
        PromqlDataType rightType = getType(right());
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

    /** Translates explicit vector matching as a join; other binary operators compose over a shared frame. */
    @Override
    public IntermediateResult translate(TranslationContext context) {
        if (match().filter() == VectorMatch.Filter.NONE && match().grouping() == Joining.NONE) {
            boolean scalarOperand = left().resolved() && getType(left()) == SCALAR || right().resolved() && getType(right()) == SCALAR;
            boolean nestedMatch = anyMatchVectorBinaryOperator(left()) || anyMatchVectorBinaryOperator(right());
            // Operands over one label set fold into a shared aggregate. Different concrete label sets match like
            // Prometheus does, pair by pair on the actual labels, which only the join expresses.
            if (scalarOperand || (nestedMatch == false && hasMismatchedLabelSets() == false)) {
                return translateAggregate(context);
            }
        }
        return translateJoin(context);
    }

    /**
     * Composes the operator as an expression over the operands' shared aggregate; the operands pass the requirement through.
     * A name-dropping operator (arithmetic, {@code bool} comparison) requires nothing of {@code __name__} from its operands
     * and discards the column from the result; a filter comparison returns the left side unchanged, metric name included.
     */
    private IntermediateResult translateAggregate(TranslationContext context) {
        // IN: required, on both operands; a name-dropping operator over a `__name__`-dimension source pairs
        // without the metric name, two vectors pair on the rest. Layouts without such a dimension pair within
        // one physical series, so there is no name to exclude.
        boolean dropName = dropMetricName && context.sourceExposesMetricName();
        TranslationContext leftTranslation = dropName
            ? context.withRequired(subtract(context.required(), List.of(LabelMatcher.NAME)))
            : context;
        TranslationContext rightTranslation = dropName
            ? context.withRequired(subtract(context.required(), List.of(LabelMatcher.NAME)))
            : context;
        IntermediateResult left = leftTranslation.translate(left());
        if (dropName) {
            left = left.with(left.plan(), subtract(left.schema(), List.of(LabelMatcher.NAME)), left.value());
        }
        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        if (this instanceof VectorBinaryComparison comp && comp.filterMode()) {
            return left.with(left.plan(), left.schema(), leftExpr);
        }

        IntermediateResult right = rightTranslation.translate(right());
        if (dropName) {
            right = right.with(right.plan(), subtract(right.schema(), List.of(LabelMatcher.NAME)), right.value());
        }
        // Two range functions pair over one series sample by sample; a side that counts no sample has no element.
        boolean seriesPair = left.kind().afterInitialAggregation == false
            && right.kind().afterInitialAggregation == false
            && getType(left()) != SCALAR
            && getType(right()) != SCALAR;
        if (seriesPair) {
            leftExpr = new ToDouble(left.value().source(), TranslationContext.emptySampleCountAsNull(left.value()));
        }
        Expression binaryExpr = binaryOp().asFunction()
            .create(source(), leftExpr, vectorOperandExpression(right, seriesPair), context.configuration());

        // Scalar operands compose as expressions; only two vectors pair or fuse. Two raw vectors pair per
        // series-minus-name first: a metric ingested as its own series never shares a physical series with another
        // metric, so folding both into one shared frame would find one side null on every row.
        if (dropMetricName && getType(left()) != SCALAR && getType(right()) != SCALAR) {
            boolean leftRaw = isVectorBeforeInitialAgg(left(), left);
            boolean rightRaw = isVectorBeforeInitialAgg(right(), right);
            if (leftRaw && rightRaw && pairsPerSeries(context)) {
                return collapseRawOperands(context, left, right);
            }
            // A raw vector next to an aggregated operand (a nested `a / (b + c)`) collapses first: per series
            // when the other side is a single collapse, or straight to the other side's grain when it is a
            // regrouped aggregation, so the two fuse below as aggregates over one grouping.
            if (leftRaw && right.kind().afterInitialAggregation) {
                left = collapseRawSide(context, left, right, dropName);
                leftExpr = new ToDouble(left.value().source(), left.value());
            }
            if (rightRaw && left.kind().afterInitialAggregation) {
                right = collapseRawSide(context, right, left, dropName);
            }
            binaryExpr = binaryOp().asFunction()
                .create(source(), leftExpr, vectorOperandExpression(right, seriesPair), context.configuration());
        }

        LogicalPlan plan;
        Expression filter;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            if (dropMetricName
                && getType(left()) != SCALAR
                && getType(right()) != SCALAR
                && aggregatesCorrespond(left, right, dropName) == false) {
                // Fusion merges two aggregates over one source level by level. An operand with a different aggregate
                // skeleton (an aggregate over a collapsed pair next to a single collapse, `sum(a / b) / sum(a)`) has
                // no level-wise counterpart to merge into, so the sides match through the join instead, each as its
                // own finished table.
                return translateJoin(context);
            }
            plan = emitBinaryOperatorAggregateExpression(left, right, dropMetricName);
            filter = null;
        } else {
            plan = left.kind().afterInitialAggregation ? left.plan() : right.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        TranslationSchema shape = left.schema().equals(TranslationSchema.EMPTY) == false ? left.schema() : right.schema();
        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        // OUT: the vector operand's labels (left when both are), without the metric name when dropped.
        if (dropName) {
            shape = subtract(shape, List.of(LabelMatcher.NAME));
        }
        IntermediateResult result = new IntermediateResult(plan, shape, null, left.step(), filter, kind);
        return context.eval(result, binaryExpr);
    }

    /**
     * Whether two raw operands pair per series before composing: only over a {@code __name__}-dimension source,
     * where a shared frame never pairs because each metric lives in its own series. Over layouts without one the
     * shared frame already pairs within one physical series, and every operand stays lazy so the enclosing
     * aggregate folds both sides per series; an eager per-series collapse would only add packings that later
     * regroups can drop.
     */
    private static boolean pairsPerSeries(TranslationContext context) {
        return context.sourceExposesMetricName();
    }

    /**
     * The grain raw operands pair at: the enclosing requirement's labels stay concrete columns (an outer BY regroups
     * by them), while every other label but the metric name packs into series identity, plus whatever the enclosing
     * requirement already excludes (an outer WITHOUT regroups the paired rows and needs its keys excluded up
     * front). A coarser grain would merge distinct series.
     */
    private static TranslationSchema seriesGrain(TranslationContext context) {
        TranslationSchema requirement = context.required().isOpen() ? context.required() : finite(context.required().labels());
        TranslationSchema grain = subtract(requirement, List.of(LabelMatcher.NAME));
        // Pairs need series identity even under a closed requirement (sum, sum by (k)): without the
        // name-exclusion packing the collapse merges distinct pairs before the operator applies. Layouts
        // without a __name__ dimension pair within one physical series and need no packing.
        if (grain.skips().isEmpty() && context.sourceExposesMetricName()) {
            grain = union(grain, open(List.of(LabelMatcher.NAME)));
        }
        return grain;
    }

    /**
     * Collapses a raw operand next to an already-aggregated one, preferring series grain so no series merge. When
     * the other side regrouped coarser and series grain has no level-wise counterpart to fuse into, the collapse
     * goes straight to the other side's top grouping instead; Max over the raw rows equals Max over per-series
     * maxima, so the fused value is unchanged.
     */
    private static IntermediateResult collapseRawSide(
        TranslationContext context,
        IntermediateResult raw,
        IntermediateResult agg,
        boolean dropName
    ) {
        TranslationSchema series = seriesGrain(context);
        IntermediateResult collapsed = context.collapse(raw, series, new Max(raw.value().source(), raw.value()));
        if (aggregatesCorrespond(collapsed, agg, dropName)) {
            return collapsed;
        }
        Aggregate top = agg.plan().collect(Aggregate.class).getFirst();
        var keys = new LinkedHashSet<String>();
        List<? extends Expression> groupings = top.groupings();
        for (int i = 1; i < groupings.size(); i++) {
            if (groupings.get(i) instanceof NamedExpression ne) {
                keys.add(ne.name());
            }
        }
        Set<String> available = new HashSet<>();
        raw.plan().output().forEach(attr -> available.add(attr.name()));
        TranslationSchema schema = available.containsAll(keys) ? finite(keys) : raw.schema();
        return context.collapse(raw, schema, new Max(raw.value().source(), raw.value()));
    }

    /**
     * Whether the fusion can merge the two tables level by level: both sides have the same aggregate levels outermost
     * first, and each pair groups alike, so every merged function keeps a grouping that computes it. Anything else
     * has no level-wise counterpart and must match through the join instead.
     */
    private static boolean aggregatesCorrespond(IntermediateResult left, IntermediateResult right, boolean dropName) {
        var leftAggs = left.plan().collect(Aggregate.class);
        var rightAggs = right.plan().collect(Aggregate.class);
        if (leftAggs.size() != rightAggs.size() || leftAggs.isEmpty()) {
            return false;
        }
        for (int i = 0; i < leftAggs.size(); i++) {
            if (groupingNames(leftAggs.get(i), dropName).equals(groupingNames(rightAggs.get(i), dropName)) == false) {
                return false;
            }
        }
        return true;
    }

    private static Set<String> groupingNames(Aggregate agg, boolean dropName) {
        List<Expression> groupings = dropName ? withoutMetricName(agg.groupings()) : agg.groupings();
        Set<String> names = new HashSet<>();
        for (Expression grouping : groupings) {
            if (grouping instanceof NamedExpression ne) {
                names.add(ne.name());
            }
        }
        return names;
    }

    /** A raw (not yet collapsed) instant-vector operand, as opposed to a scalar or an aggregated table. */
    private static boolean isVectorBeforeInitialAgg(LogicalPlan operand, IntermediateResult translated) {
        return translated.kind().afterInitialAggregation == false && getType(operand) != SCALAR;
    }

    /**
     * The operand's value as a double expression, read from its current table: collapsing the operand first (a raw
     * vector next to an aggregated one) replaces the table, so the expression must be built after, not reused.
     */
    private static Expression vectorOperandExpression(IntermediateResult operand, boolean seriesPair) {
        Expression value = seriesPair ? TranslationContext.emptySampleCountAsNull(operand.value()) : operand.value();
        return new ToDouble(value.source(), value);
    }

    /**
     * Pairs two raw vector operands per series and step before applying the operator. The collapse's first phase runs per
     * physical time series, and a metric ingested as its own documents (one per sample, {@code labels.__name__} a
     * dimension) never shares a physical series with another metric: computed row by row, the operator would see one
     * operand null in every row. Aggregating each operand within its group first is exact - the group is every label but
     * {@code __name__}, so it holds at most one series per metric - and puts both values in one row. The eager collapse
     * hands an aggregated table to any enclosing aggregate, which regroups it like any other collapsed operand.
     */
    private IntermediateResult collapseRawOperands(TranslationContext context, IntermediateResult left, IntermediateResult right) {
        // Each side keeps its operand's source, so the paired expression still reads as `a <op> b` over the selectors.
        Source leftSource = left.value().source();
        Source rightSource = right.value().source();
        // Each operand's pending filter is attached to the inner TSAF so it fires in the first phase of the
        // TimeSeriesAggregate, where source fields (like `labels.__name__`) are still present. Combining the two
        // filters with AND on the source relation would exclude all rows for cross-metric binary ops: a row matches
        // `labels.__name__ = "metric_a"` or `labels.__name__ = "metric_b"` but never both simultaneously.
        Expression leftMax = applySeriesFilter(
            new Max(leftSource, TranslationContext.emptySampleCountAsNull(left.value())),
            left.pendingFilter()
        );
        Expression rightMax = applySeriesFilter(
            new Max(rightSource, TranslationContext.emptySampleCountAsNull(right.value())),
            right.pendingFilter()
        );
        Expression leftExpr = new ToDouble(leftSource, leftMax);
        Expression rightExpr = new ToDouble(rightSource, rightMax);
        Expression paired = binaryOp().asFunction().create(source(), leftExpr, rightExpr, context.configuration());
        // Pair at series grain; an enclosing aggregate regroups the paired rows to its own grain.
        TranslationSchema grain = seriesGrain(context);
        IntermediateResult raw = new IntermediateResult(right.plan(), grain, null, right.step(), null, Kind.BEFORE_INITIAL_AGGREGATE);
        return context.collapse(raw, grain, paired);
    }

    private static Expression applySeriesFilter(Expression expr, Expression filter) {
        if (filter == null) {
            return expr;
        }
        return expr.transformDown(TimeSeriesAggregateFunction.class, f -> f.withFilter(filter));
    }

    /**
     * Attaches an operand's pending filter to the time-series functions of its aggregates only. The selector matchers
     * reference source fields, which exist in the per-series first phase of the time-series aggregate but not in its
     * second phase; an outer aggregate nested in an expression (the {@code Max} of a paired collapse) would keep a filter
     * over vanished columns.
     */
    private static List<? extends Expression> withSeriesFilter(List<? extends Expression> aggregates, Expression filter) {
        if (filter == null) {
            return aggregates;
        }
        return aggregates.stream()
            .map(e -> e.transformDown(TimeSeriesAggregateFunction.class, function -> function.withFilter(filter)))
            .toList();
    }

    /** The grouping keys without the {@code __name__} label, however the relation spells it. */
    private static List<Expression> withoutMetricName(List<Expression> groupings) {
        return groupings.stream().filter(g -> (g instanceof NamedExpression ne && isMetricName(ne.toAttribute())) == false).toList();
    }

    private static boolean isMetricName(Attribute attribute) {
        return LabelMatcher.NAME.equals(PromqlLabels.labelName(attribute));
    }

    /**
     * Translates a vector-matched join operator into an {@link InnerJoin}: each operand becomes an independent series
     * pipeline, joined on shared {@code step} + label keys, and the result value is computed on the joined rows.
     * The operands compile against the labels the join requires, like any other schema push-down: a required label
     * comes back as a concrete column wherever the operand can carry it, and a label the operand dropped stays
     * absent and null-fills at the join.
     * <p>
     * The block has three rules: how the sides are ordered (which operand probes, which builds and is re-identified), how
     * the fields are placed (each side's match key packed next to step, the build side's value and {@code group_x} labels
     * carried across, every result label bound to the operand carrying it or to null) and what is projected (the build
     * side down to its join fields, the result down to value, step and the schema's labels). The result exposes its step
     * column under the enclosing translation's step identity, and a label the match dropped null-fills rather than leaking
     * from an operand.
     */
    public IntermediateResult translateJoin(TranslationContext context) {
        // A join result is finite: its label set is the operator schema plus whatever the enclosing translation asks
        // for by name (null-filled when the match dropped it). Packed columns stop here as they do at a `by`.
        TranslationSchema schema = union(finite(TranslationContext.mapFinite(output())), finite(context.required().labels()));
        TranslationSchema childSchema = schema;
        VectorMatch match = match();
        if (match.filter() == VectorMatch.Filter.ON) {
            childSchema = union(childSchema, finite(match.filterLabels()));
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            // The key is each operand's own label set minus the ignored labels: a packed column for an opaque operand.
            childSchema = union(childSchema, open(match.filterLabels()));
        } else {
            // No on/ignoring: the key is each operand's whole label set. The verifier admits only operands with
            // concrete label sets here, so the operator's declared output already names every label of both sides
            // and the schema needs no widening.
            assert match.filter() == VectorMatch.Filter.NONE : "unexpected vector match filter " + match.filter();
            assert hasPackedLabels(left().output()) == false && hasPackedLabels(right().output()) == false
                : "invariant: an unmatched join needs operands with concrete label sets [" + sourceText() + "]";
        }
        TranslationContext childTranslation = context.withRequired(childSchema);
        List<Attribute> declared = output();
        IntermediateResult left = childTranslation.translateIntermediate(left(), new NameId(), new NameId());
        IntermediateResult right = childTranslation.translateIntermediate(right(), new NameId(), new NameId());
        // Orientation: the probe side keeps its identities; the build side is re-identified so a self-join of
        // structurally identical operands has distinct attributes on each side.
        boolean probeRight = match.grouping() == Joining.RIGHT;
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = reidentify(context.cmd(), probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        LogicalPlan join = emitJoin(context.cmd(), probe, build, keyLabels(left, right));
        List<NamedExpression> output = bindOutput(schema, declared, probe, build);
        return bindResult(context, schema, leftValue, rightValue, probe.step(), join, output);
    }

    /**
     * The labels both sides pack into the match key, in one shared order: the on(...) labels as written, otherwise the
     * union of the operands' labels minus the ignored ones, sorted by name. A side that lacks a key label packs null
     * there, so the key behaves like a Prometheus signature: a label absent on both sides does not discriminate, a label
     * present on one side only never matches. Operands over different label sets therefore evaluate to the empty
     * vector, and the order in which each operand declares its labels is irrelevant.
     */
    private List<String> keyLabels(IntermediateResult left, IntermediateResult right) {
        if (match.filter() == VectorMatch.Filter.ON) {
            return List.copyOf(match.filterLabels());
        }
        var names = new TreeSet<>(left.schema().labels());
        names.addAll(right.schema().labels());
        names.removeAll(match.filterLabels());
        // A Prometheus signature never includes the metric name: operands of different metrics pair on their other labels.
        names.remove(LabelMatcher.NAME);
        return List.copyOf(names);
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private IntermediateResult bindResult(
        TranslationContext context,
        TranslationSchema schema,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        List<NamedExpression> output
    ) {
        PromqlCommand cmd = context.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp().asFunction().create(source(), lhsExpr, rhsExpr, context.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? lhsExpr : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(defined(output));
        LogicalPlan plan = new Eval(cmd.source(), join, definitions);
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        output.forEach(column -> projected.add(column.toAttribute()));
        plan = new Project(cmd.source(), plan, projected);

        return new IntermediateResult(plan, schema, valueAlias.toAttribute(), stepAlias.toAttribute(), null, Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The build operand under fresh identities, so a self-join of structurally identical operands has distinct
     * attributes on each side. Its value column is also renamed: {@link InnerJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private static IntermediateResult reidentify(PromqlCommand cmd, IntermediateResult input) {
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        LogicalPlan plan = input.plan()
            .transformExpressionsDown(Expression.class, e -> reidExpr(renamed(e, cmd.valueColumnName(), valueName), ids));
        Expression value = reidExpr(renamed(input.valueColumn(), cmd.valueColumnName(), valueName), ids);
        Attribute step = (Attribute) reidExpr(input.step(), ids);
        return new IntermediateResult(plan, input.schema(), value, step, input.pendingFilter(), input.kind());
    }

    /** The inner join of the two operands on step plus the packed match key. */
    private LogicalPlan emitJoin(PromqlCommand cmd, IntermediateResult probe, IntermediateResult build, List<String> keyLabels) {
        Input probeInput = emitInput(cmd, probe, keyLabels);
        Input buildInput = emitInput(cmd, build, keyLabels);

        // The build side carries its join fields plus what the join adds: its value and the group_x labels. Neither can
        // already be a join field (the step, or the freshly packed key), so the two lists are disjoint.
        List<Attribute> added = addedFields(build);
        List<NamedExpression> projection = new ArrayList<>(buildInput.fields());
        projection.addAll(added);
        LogicalPlan buildPlan = new Project(cmd.source(), buildInput.plan(), projection);

        return new InnerJoin(
            cmd.source(),
            probeInput.plan(),
            buildPlan,
            probeInput.fields(),
            buildInput.fields(),
            added,
            match.grouping() == Joining.NONE
        );
    }

    /** The columns the join adds from the build side: its value and the labels a group_x modifier copies over. */
    private List<Attribute> addedFields(IntermediateResult build) {
        List<Attribute> fields = new ArrayList<>();
        fields.add(build.valueColumn());
        for (String name : match.groupingLabels()) {
            Attribute field = build.label(name);
            if (field != null) {
                fields.add(field);
            }
        }
        return fields;
    }

    /** One side's plan with its match key defined and packed next to step; step alone when the key is empty. */
    private Input emitInput(PromqlCommand cmd, IntermediateResult input, List<String> keyLabels) {
        List<NamedExpression> key = joinKey(input, keyLabels);
        List<Alias> nullFills = defined(key);
        LogicalPlan plan = nullFills.isEmpty() ? input.plan() : new Eval(cmd.source(), input.plan(), nullFills);
        if (key.isEmpty()) {
            return new Input(plan, List.of(input.step()));
        }
        List<Attribute> keyColumns = key.stream().map(NamedExpression::toAttribute).toList();
        Attribute packed = new ReferenceAttribute(cmd.source(), null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        return new Input(new PackDims(cmd.source(), plan, keyColumns, packed), List.of(input.step(), packed));
    }

    /**
     * The operand's match key columns: its packed columns surviving the ignored labels (an opaque operand under
     * ignoring), then the shared key labels, each as the operand's own column or a null where it lacks the label.
     */
    private List<NamedExpression> joinKey(IntermediateResult input, List<String> keyLabels) {
        var key = new ArrayList<NamedExpression>();
        if (match.filter() != VectorMatch.Filter.ON) {
            TranslationSchema surviving = intersect(input.schema(), match.filterLabels());
            for (Set<String> skip : TranslationContext.finestFirst(surviving.skips())) {
                Attribute packed = input.packed(skip);
                assert packed != null : "invariant: packing " + skip + " must be carried by the operand";
                key.add(packed);
            }
        }
        for (String name : keyLabels) {
            Attribute attribute = input.label(name);
            key.add(attribute != null ? attribute : TranslationContext.emitNullExpression(TranslationContext.mapToRef(name)));
        }
        return key;
    }

    /** The join result's label columns: every schema label bound to the operand carrying it, or to null. */
    private List<NamedExpression> bindOutput(
        TranslationSchema schema,
        List<Attribute> declared,
        IntermediateResult probe,
        IntermediateResult build
    ) {
        var output = new ArrayList<NamedExpression>();
        for (String name : schema.labels()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            Attribute declaredAttr = TranslationContext.find(declared, name);
            if (declaredAttr == null) {
                output.add(TranslationContext.emitNullExpression(TranslationContext.mapToRef(name)));
                continue;
            }
            // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
            // projection binds it by identity.
            Attribute attribute = match.groupingLabels().contains(name) ? build.label(name) : probe.label(name);
            output.add(attribute != null ? attribute : TranslationContext.emitNullExpression(declaredAttr));
        }
        return output;
    }

    /** The columns among {@code columns} defined inline (aliases) rather than carried by the plan. */
    private static List<Alias> defined(List<? extends NamedExpression> columns) {
        return columns.stream().filter(Alias.class::isInstance).map(Alias.class::cast).toList();
    }

    /** Renames an attribute or alias in a re-identification pass; other expressions pass through unchanged. */
    private static Expression renamed(Expression e, String from, String to) {
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
     * Fold left and right aggregates into a single plan. When the operator drops the metric name, a {@code __name__}
     * grouping is dropped from both sides first: Prometheus pairs series on their labels without the metric name, and over
     * the remote-write layout the two operands never share one (each metric is its own {@code __name__}), so grouping on
     * it would put the sides in disjoint groups. A selector names one metric, so within an operand the grouping is
     * constant and dropping it merges nothing.
     */
    private LogicalPlan emitBinaryOperatorAggregateExpression(IntermediateResult left, IntermediateResult right, boolean dropName) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            List<Expression> leftGroupings = dropName ? withoutMetricName(leftAgg.groupings()) : leftAgg.groupings();
            List<Expression> rightGroupings = dropName ? withoutMetricName(rightAgg.groupings()) : rightAgg.groupings();
            Set<String> leftGroupingNames = new HashSet<>();
            for (Expression grouping : leftGroupings) {
                if (grouping instanceof NamedExpression ne) {
                    leftGroupingNames.add(ne.name());
                }
            }
            Set<String> rightGroupingNames = new HashSet<>();
            for (Expression grouping : rightGroupings) {
                if (grouping instanceof NamedExpression ne) {
                    rightGroupingNames.add(ne.name());
                }
            }
            boolean groupingsCompatible = leftGroupings.size() == rightGroupings.size() && leftGroupingNames.equals(rightGroupingNames);

            if (groupingsCompatible == false) {
                throw new VerificationException("binary operations between vectors with mismatched grouping keys are not yet supported");
            }

            // Grouping columns match by name. Relation fields share one attribute on both sides, but a packing alias
            // (`_timeseries$__name__`) is minted per collapse: rebind the right side's keys to the left side's so the
            // fused aggregate carries each key once and every reference resolves against the surviving grouping.
            Map<String, Attribute> leftKeys = new HashMap<>();
            for (Expression grouping : leftAgg.groupings()) {
                if (grouping instanceof NamedExpression ne) {
                    leftKeys.put(ne.name(), ne.toAttribute());
                }
            }
            Map<NameId, Attribute> rightToLeft = new HashMap<>();
            for (Expression grouping : rightAgg.groupings()) {
                if (grouping instanceof NamedExpression ne && leftKeys.containsKey(ne.name())) {
                    rightToLeft.put(ne.toAttribute().id(), leftKeys.get(ne.name()));
                }
            }
            List<? extends Expression> rightAggregates = rightAgg.aggregates()
                .stream()
                .map(e -> e.transformUp(Attribute.class, a -> rightToLeft.getOrDefault(a.id(), a)))
                .toList();

            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(
                dropName
                    ? withSeriesFilter(leftAgg.aggregates(), left.pendingFilter())
                    : withFilter(leftAgg.aggregates(), left.pendingFilter())
            );
            uniqueAggregates.addAll(
                dropName ? withSeriesFilter(rightAggregates, right.pendingFilter()) : withFilter(rightAggregates, right.pendingFilter())
            );
            if (dropName) {
                // the dropped grouping is no longer an output column either
                uniqueAggregates.removeIf(e -> e instanceof Attribute a && isMetricName(a));
            }

            // Only the aggregate functions need fresh names: both operands define `value`. Grouping columns keep their
            // own names - the command projection finds a passthrough label (`labels.pod`) by its canonical name when the
            // analyzer bound the declared output to the bare attribute instead, and a renamed column would not map.
            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                if (e instanceof Alias a) {
                    return (NamedExpression) new Alias(a.source(), names.next(a.name()), a.child(), a.id());
                }
                return e;
            }).toList();

            return leftAgg.with(leftAgg.child(), leftGroupings, newAggregates);
        });

        // Input expressions are already below the surviving aggregate. Only replay result expressions above it.
        var rightEvals = right.plan().collect(Eval.class);
        Stream<Eval> replayed = dropName
            ? rightEvals.stream().filter(eval -> rightAgg.collect(Eval.class).contains(eval) == false)
            : rightEvals.stream();
        for (Eval eval : replayed.toList().reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

    private static boolean anyMatchVectorBinaryOperator(LogicalPlan plan) {
        return plan.anyMatch(p -> {
            if (p instanceof VectorBinaryOperator vbo) {
                VectorMatch match = vbo.match();
                return match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE;
            }
            return false;
        });
    }
}
