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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataSet;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.finite;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
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
            return right().output();
        }
        if (PromqlPlan.returnsScalar(right())) {
            return left().output();
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
            return leftAttrs;
        } else {
            // Default matching between different label sets: a pair matches only where the labels one side lacks are
            // absent on the other side too (a Prometheus signature has no entry for an absent label), and like every
            // one-to-one match the result carries the left operand's labels.
            outputLabels = new HashSet<>(leftLabels);
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
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

        return result;
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
        boolean containerFunctions = context.containerFunctions();
        if (this instanceof VectorBinarySet set && set.op() == VectorBinarySet.SetOp.UNION && containerFunctions) {
            return translateUnion(context);
        }
        if (this instanceof VectorBinarySet) {
            return translateMatched(context);
        }
        if (match().filter() == VectorMatch.Filter.NONE && match().grouping() == Joining.NONE) {
            boolean scalarOperand = left().resolved() && getType(left()) == SCALAR || right().resolved() && getType(right()) == SCALAR;
            boolean nestedMatch = anyMatchVectorBinaryOperator(left()) || anyMatchVectorBinaryOperator(right());
            // Operands over one label set fold into a shared aggregate. Different concrete label sets match like
            // Prometheus does, pair by pair on the actual labels, which only the join expresses.
            if (scalarOperand || (nestedMatch == false && hasMismatchedLabelSets() == false)) {
                if (containerFunctions && requiresIndependentPipelines(context)) {
                    return translateContainerJoin(context);
                }
                return translateAggregate(context);
            }
        }
        return translateMatched(context);
    }

    /**
     * Translates the operator as a join: on the operands' label containers when an operand's labels stay packed, or for a set
     * operator, which matches series many-to-many.
     */
    public IntermediateResult translateMatched(TranslationContext context) {
        if (context.containerFunctions() && this instanceof VectorBinarySet set && set.op() == VectorBinarySet.SetOp.UNION) {
            return translateUnion(context);
        }
        if (context.containerFunctions()
            && (this instanceof VectorBinarySet || hasPackedLabels(left().output()) || hasPackedLabels(right().output()))) {
            return translateContainerJoin(context);
        }
        return translateJoin(context);
    }

    /**
     * Whether the operands cannot fold into one shared aggregate, which the verifier rejects without the label container
     * functions: a source-backed scalar, which broadcasts on the step; operands disagreeing on {@code offset}, as one
     * aggregate has one evaluation time; an operand aggregated across series beside a raw one, or nested aggregations,
     * which no single aggregate computes; a {@code topk}-like reduction, which ranks after its own aggregate; a filter
     * comparison of two vectors, which pairs them like any match; and a filter comparison of different metrics stored as
     * separate series, which keeps the metric name and so can never share a series.
     */
    private boolean requiresIndependentPipelines(TranslationContext context) {
        boolean leftScalar = PromqlPlan.returnsScalar(left());
        boolean rightScalar = PromqlPlan.returnsScalar(right());
        if (leftScalar && rightScalar) {
            return hasSourceBackedExpression(left()) && hasSourceBackedExpression(right());
        }
        if (leftScalar || rightScalar) {
            return hasSourceBackedExpression(leftScalar ? left() : right());
        }
        if (this instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            return true;
        }
        // Different metrics stored as separate series never share a series, so no shared per-series aggregate pairs them.
        if (context.cmd().storesMetricName()
            && readsDifferentMetrics()
            && (hasPackedLabels(left().output()) || hasPackedLabels(right().output()))) {
            return true;
        }
        // A relabel changes the series' labels, which one shared aggregate does not carry for both operands.
        if (left().anyMatch(MetadataManipulationFunction.class::isInstance)
            || right().anyMatch(MetadataManipulationFunction.class::isInstance)) {
            return true;
        }
        if (hasSourceBackedExpression(left()) == false || hasSourceBackedExpression(right()) == false) {
            return false;
        }
        return PromqlCommand.collectAllOffsetsForBranch(left()).equals(PromqlCommand.collectAllOffsetsForBranch(right())) == false
            || aggregatesAcrossSeries(left()) != aggregatesAcrossSeries(right())
            || nestsAggregations(left())
            || nestsAggregations(right())
            || left().anyMatch(AcrossSeriesReduction.class::isInstance)
            || right().anyMatch(AcrossSeriesReduction.class::isInstance);
    }

    private static boolean hasSourceBackedExpression(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Selector && (p instanceof LiteralSelector) == false);
    }

    /** Whether the operands select different metrics, each its own set. */
    private boolean readsDifferentMetrics() {
        Set<String> leftNames = metricNames(left());
        Set<String> rightNames = metricNames(right());
        return leftNames.isEmpty() == false && rightNames.isEmpty() == false && leftNames.equals(rightNames) == false;
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

    private static boolean isAcrossSeries(LogicalPlan plan) {
        return plan instanceof AcrossSeriesAggregate || plan instanceof AcrossSeriesReduction || plan instanceof ScalarConversionFunction;
    }

    private static boolean aggregatesAcrossSeries(LogicalPlan plan) {
        return plan.anyMatch(VectorBinaryOperator::isAcrossSeries);
    }

    private static boolean nestsAggregations(LogicalPlan plan) {
        return plan.anyMatch(
            p -> isAcrossSeries(p) && p instanceof UnaryPlan unary && unary.child().anyMatch(VectorBinaryOperator::isAcrossSeries)
        );
    }

    /** The join key column: the labels two series match on. */
    private static final String SIGNATURE = "$prom_sig";
    /** The one side's labels, under a name of their own since the join merges its output by name. */
    private static final String ONE_LABELS = "$prom_one";
    /** The result labels, from which the result's label columns and packings read. */
    private static final String RESULT_LABELS = "$prom_labels";

    /**
     * Translates the operator as a join of independent pipelines matched on their label containers, with the label container
     * functions: each operand's labels are one container - its packing with its label columns written over it - so an
     * operand whose labels stay packed in {@code _timeseries} matches like any other. Two series match on Prometheus'
     * signature, the {@code on(...)} labels or every label but the {@code ignoring(...)} ones and the metric name, and the
     * result labels are the many side's, as Prometheus' {@code resultMetric} computes them; the columns the enclosing
     * translation requires read out of them. A scalar operand joins on the step alone.
     */
    private IntermediateResult translateContainerJoin(TranslationContext context) {
        return finish(context, joinOnLabels(context));
    }

    /** A finished table of the operator's result, its labels {@code null} for a scalar. */
    private record LabeledTable(LogicalPlan plan, Attribute value, Attribute step, Attribute labels) {}

    /**
     * The result table, exposing the labels the operator declares, and whatever the enclosing translation requires, read out
     * of the result labels.
     */
    private IntermediateResult finish(TranslationContext context, LabeledTable table) {
        if (table.labels() == null) {
            return new IntermediateResult(
                table.plan(),
                TranslationSchema.EMPTY,
                table.value(),
                table.step(),
                null,
                Kind.AFTER_INITIAL_AGGREGATE
            );
        }
        var declared = new TreeSet<String>();
        for (Attribute attribute : output()) {
            if (MetadataAttribute.isTimeSeriesAttributeName(attribute.name()) == false) {
                declared.add(TranslationContext.mapFinite(attribute));
            }
        }
        var columns = new LinkedHashSet<String>(context.required().labels());
        columns.addAll(declared);
        TranslationSchema wanted = new TranslationSchema(columns, context.required().skips());
        return context.fromLabels(table.plan(), table.labels(), wanted, declared, table.value(), table.step());
    }

    /** The join of the operands on their label containers, projected to the result value, step and labels. */
    private LabeledTable joinOnLabels(TranslationContext context) {
        PromqlCommand cmd = context.cmd();
        TranslationContext operands = context.withRequired(open());
        IntermediateResult left = operands.translateIntermediate(left(), new NameId(), new NameId());
        IntermediateResult right = operands.translateIntermediate(right(), new NameId(), new NameId());
        boolean leftScalar = PromqlPlan.returnsScalar(left());
        boolean rightScalar = PromqlPlan.returnsScalar(right());
        // Orientation: the many side probes and keeps its identities; the one side builds and is re-identified, so a
        // self-join of structurally identical operands has distinct attributes on each side. A scalar always builds.
        boolean probeRight = leftScalar || match.grouping() == Joining.RIGHT;
        IntermediateResult probe = probeRight ? right : left;
        IntermediateResult build = reidentify(cmd, probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();
        Expression probeLabels = leftScalar && rightScalar ? null : context.labels(probe);

        LogicalPlan join;
        Expression labels;
        if (leftScalar || rightScalar) {
            // A scalar has one row per step, which every series of the vector shares.
            LogicalPlan buildPlan = new Project(cmd.source(), build.plan(), List.of(build.step(), build.valueColumn()));
            join = new HashJoin(
                cmd.source(),
                probe.plan(),
                buildPlan,
                List.of(probe.step()),
                List.of(build.step()),
                List.of(build.valueColumn()),
                false
            );
            labels = probeLabels == null ? null : resultLabels(probeLabels, null);
        } else {
            Alias probeSignature = new Alias(cmd.source(), SIGNATURE, signature(probeLabels));
            LogicalPlan probePlan = new Eval(cmd.source(), probe.plan(), List.of(probeSignature));
            Expression buildLabels = context.labels(build);
            Alias buildSignature = new Alias(cmd.source(), SIGNATURE, signature(buildLabels));
            var buildColumns = new ArrayList<Alias>(List.of(buildSignature));
            List<Attribute> added = new ArrayList<>(List.of(build.valueColumn()));
            Attribute oneLabels = null;
            if (match.groupingLabels().isEmpty() == false) {
                Alias one = new Alias(cmd.source(), ONE_LABELS, buildLabels);
                buildColumns.add(one);
                oneLabels = one.toAttribute();
                added.add(oneLabels);
            }
            LogicalPlan buildPlan = new Eval(cmd.source(), build.plan(), buildColumns);
            List<Attribute> buildFields = List.of(build.step(), buildSignature.toAttribute());
            var projection = new ArrayList<NamedExpression>(buildFields);
            projection.addAll(added);
            buildPlan = new Project(cmd.source(), buildPlan, projection);
            boolean setOperator = this instanceof VectorBinarySet;
            if (setOperator) {
                // A set operator matches many-to-many: a left series is kept once whichever right series share its signature.
                var order = new Order(cmd.source(), build.valueColumn(), Order.OrderDirection.ASC, Order.NullsPosition.LAST);
                buildPlan = new TopNBy(
                    cmd.source(),
                    buildPlan,
                    List.of(order),
                    new Literal(cmd.source(), 1, DataType.INTEGER),
                    new ArrayList<>(buildFields)
                );
            }
            // unless keeps the left series no right one matches, so it keeps every left row of the join
            boolean unless = this instanceof VectorBinarySet set && set.op() == VectorBinarySet.SetOp.SUBTRACT;
            join = new HashJoin(
                cmd.source(),
                probePlan,
                buildPlan,
                List.of(probe.step(), probeSignature.toAttribute()),
                buildFields,
                added,
                setOperator == false && match.grouping() == Joining.NONE,
                unless ? JoinTypes.LEFT : JoinTypes.INNER
            );
            labels = resultLabels(probeLabels, oneLabels);
        }

        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value;
        Expression filter = null;
        if (this instanceof VectorBinarySet set) {
            // and keeps the left value where a right series matches, unless where none does
            Expression matched = set.op() == VectorBinarySet.SetOp.SUBTRACT
                ? new IsNull(source(), rightValue)
                : new IsNotNull(source(), rightValue);
            value = new Case(source(), matched, List.of(lhsExpr));
        } else {
            value = binaryOp().asFunction().create(source(), lhsExpr, rhsExpr, context.configuration());
            if (this instanceof VectorBinaryComparison comparison) {
                filter = comparison.filterMode() ? value : null;
                // A filter keeps the vector's value, wherever the scalar sits, as Prometheus does.
                Expression kept = leftScalar ? rhsExpr : lhsExpr;
                value = comparison.filterMode() ? kept : new ToDouble(value.source(), value);
            }
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Attribute step = probe.step();
        Alias stepAlias = new Alias(step.source(), step.name(), step, context.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        Alias labelsAlias = labels == null ? null : new Alias(source(), RESULT_LABELS, labels, new NameId());
        LogicalPlan plan = join;
        if (labelsAlias != null) {
            plan = new Eval(cmd.source(), plan, List.of(labelsAlias));
            if (match.grouping() != Joining.NONE) {
                // Prometheus fails a group_left/group_right match before it compares values: copying the one side's labels
                // over the many side's can leave two many side series with the same labels.
                plan = TranslationContext.uniqueSeries(cmd.source(), plan, step, List.of(labelsAlias.toAttribute()));
            }
        }
        plan = new Eval(cmd.source(), plan, List.of(valueAlias, stepAlias));
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        if (labelsAlias != null) {
            projected.add(labelsAlias.toAttribute());
        }
        plan = new Project(cmd.source(), plan, projected);
        return new LabeledTable(
            plan,
            valueAlias.toAttribute(),
            stepAlias.toAttribute(),
            labelsAlias == null ? null : labelsAlias.toAttribute()
        );
    }

    /**
     * {@code a or b}: every series of {@code a}, and each series of {@code b} no series of {@code a} matches - with the
     * matching modifiers, or on every label but the metric name - so {@code a or<M> b} is {@code a} beside
     * {@code b unless<M> a}.
     */
    private IntermediateResult translateUnion(TranslationContext context) {
        PromqlCommand cmd = context.cmd();
        Source source = cmd.source();
        IntermediateResult left = context.withRequired(open()).translateIntermediate(left(), new NameId(), new NameId());
        Attribute leftLabels = null;
        LogicalPlan leftPlan = left.plan();
        Alias labels = new Alias(source, RESULT_LABELS, context.labels(left));
        leftPlan = new Eval(source, leftPlan, List.of(labels));
        leftLabels = labels.toAttribute();
        VectorBinaryOperator unmatched = new VectorBinarySet(source(), right(), left(), match(), VectorBinarySet.SetOp.SUBTRACT);
        LabeledTable right = unmatched.joinOnLabels(context);
        var branches = new ArrayList<LogicalPlan>();
        for (var branch : List.of(new LabeledTable(leftPlan, left.valueColumn(), left.step(), leftLabels), right)) {
            // Each branch as the same columns: value, step and labels, without the rows that have no sample.
            LogicalPlan plan = new Filter(source, branch.plan(), new IsNotNull(source, branch.value()));
            plan = new Eval(
                source,
                plan,
                List.of(
                    new Alias(source, cmd.valueColumnName(), branch.value()),
                    new Alias(source, cmd.stepColumnName(), branch.step()),
                    new Alias(source, RESULT_LABELS, branch.labels())
                )
            );
            branches.add(
                new Project(source, plan, plan.output().stream().filter(a -> isUnionColumn(cmd, a)).map(a -> (NamedExpression) a).toList())
            );
        }
        List<Attribute> output = VectorBinarySet.unionOutputByName(branches);
        LogicalPlan union = new UnionAll(source, branches, output);
        Attribute value = TranslationContext.find(output, cmd.valueColumnName());
        Attribute step = TranslationContext.find(output, cmd.stepColumnName());
        Attribute unionLabels = TranslationContext.find(output, RESULT_LABELS);
        // The step under an identity of its own: an enclosing join keeps a union's identities as they are (see reidentify), so
        // it must not share the enclosing frame's.
        Alias stepAlias = new Alias(source, step.name(), step, new NameId());
        LogicalPlan plan = new Eval(source, union, List.of(stepAlias));
        return finish(context, new LabeledTable(plan, value, stepAlias.toAttribute(), unionLabels));
    }

    private static boolean isUnionColumn(PromqlCommand cmd, Attribute attribute) {
        String name = attribute.name();
        return name.equals(cmd.valueColumnName()) || name.equals(cmd.stepColumnName()) || name.equals(RESULT_LABELS);
    }

    /** The labels two series match on: Prometheus' signature. */
    private Expression signature(Expression labels) {
        if (match.filter() == VectorMatch.Filter.ON) {
            return new TimeSeriesMetadataFilter(labels.source(), labels, labelLiterals(labels.source(), match.filterLabels()), false);
        }
        var ignored = new TreeSet<String>(match.filterLabels());
        ignored.add(LabelMatcher.NAME);
        return new TimeSeriesMetadataFilter(labels.source(), labels, labelLiterals(labels.source(), ignored), true);
    }

    /**
     * Prometheus' {@code resultMetric}: the many side's labels, without the metric name for arithmetic and {@code bool},
     * with only the {@code on(...)} labels or without the {@code ignoring(...)} ones when one-to-one, and with each
     * {@code group_x} included label written from the one side's. Set operators keep the left series as they are.
     */
    private Expression resultLabels(Expression many, Attribute one) {
        if (this instanceof VectorBinarySet) {
            return many;
        }
        Expression labels = many;
        if (dropMetricName) {
            labels = new TimeSeriesMetadataFilter(
                labels.source(),
                labels,
                labelLiterals(labels.source(), List.of(LabelMatcher.NAME)),
                true
            );
        }
        if (match.grouping() == Joining.NONE && match.filter() != VectorMatch.Filter.NONE) {
            boolean keep = match.filter() == VectorMatch.Filter.ON;
            labels = new TimeSeriesMetadataFilter(
                labels.source(),
                labels,
                labelLiterals(labels.source(), match.filterLabels()),
                keep == false
            );
        }
        for (String included : match.groupingLabels()) {
            Expression value = one == null
                ? new Literal(source(), null, DataType.KEYWORD)
                : new TimeSeriesMetadataPath(one.source(), one, Literal.keyword(one.source(), included));
            labels = new TimeSeriesMetadataSet(labels.source(), labels, Literal.keyword(labels.source(), included), value);
        }
        return labels;
    }

    private static List<Expression> labelLiterals(Source source, Collection<String> labels) {
        return new TreeSet<>(labels).stream().<Expression>map(label -> Literal.keyword(source, label)).toList();
    }

    /**
     * A filter comparison of a vector against a scalar that reads no source: the vector, aggregated per series, keeps the
     * series the comparison holds for, with its own value whichever side the scalar is on, as Prometheus does.
     */
    private IntermediateResult translateFilter(TranslationContext context, VectorBinaryComparison comparison) {
        boolean scalarLeft = PromqlPlan.returnsScalar(left());
        // The comparison filters each series, so the vector keeps one row per series up to the filter.
        TranslationContext perSeries = context.withRequired(union(context.required(), open()));
        IntermediateResult vector = perSeries.translate(scalarLeft ? right() : left());
        IntermediateResult scalar = context.translate(scalarLeft ? left() : right());
        IntermediateResult table = vector.kind().afterInitialAggregation
            ? vector
            : context.collapse(vector, vector.schema(), vector.value());
        Expression value = new ToDouble(table.value().source(), table.value());
        Expression threshold = new ToDouble(scalar.value().source(), scalar.value());
        Expression condition = scalarLeft
            ? comparison.op().asFunction().create(source(), threshold, value, context.configuration())
            : comparison.op().asFunction().create(source(), value, threshold, context.configuration());
        return table.with(new Filter(source(), table.plan(), condition), table.schema(), value);
    }

    /** Composes a binary operator as an expression over the operands' shared aggregate. */
    private IntermediateResult translateAggregate(TranslationContext context) {
        if (this instanceof VectorBinaryComparison comp && comp.filterMode()) {
            return translateFilter(context, comp);
        }
        // Arithmetic and a bool comparison drop the metric name; so do both operands, folding into one frame.
        TranslationContext outer = context;
        context = dropMetricName ? context.withoutMetricName() : context;
        IntermediateResult left = context.translate(left());
        Expression leftExpr = new ToDouble(left.value().source(), left.value());

        IntermediateResult right = context.translate(right());
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        Expression binaryExpr = binaryOp().asFunction().create(source(), leftExpr, rightExpr, context.configuration());
        if (this instanceof VectorBinaryComparison) {
            // bool yields 1 or 0: a number, which an enclosing aggregate reads like any other value
            binaryExpr = new ToDouble(source(), binaryExpr);
        }

        LogicalPlan plan;
        Expression filter;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            if (outer.containerFunctions() && (sharesGrouping(left, right) == false || combines(left) || combines(right))) {
                // Aggregates over different groupings, such as two different without, or a union or join of operands of its
                // own, pair like any match.
                return translateContainerJoin(outer);
            }
            plan = emitBinaryOperatorAggregateExpression(left, right);
            filter = null;
        } else {
            plan = left.kind().afterInitialAggregation ? left.plan() : right.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        TranslationSchema shape = left.schema().equals(TranslationSchema.EMPTY) == false ? left.schema() : right.schema();
        Kind kind = left.kind().afterInitialAggregation || right.kind().afterInitialAggregation
            ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        IntermediateResult result = new IntermediateResult(plan, shape, null, left.step(), filter, kind);
        return context.eval(result, binaryExpr);
    }

    /**
     * Translates a vector-matched join operator into an {@link HashJoin}: each operand becomes an independent series
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
        if (match.grouping() != Joining.NONE) {
            // Prometheus fails a group_left/group_right match before it compares values: copying the one side's labels over
            // the many side's can leave two many side series with the same labels.
            List<Attribute> labels = output.stream().map(NamedExpression::toAttribute).toList();
            plan = TranslationContext.uniqueSeries(cmd.source(), plan, stepAlias.toAttribute(), labels);
        }
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
     * attributes on each side. Its value column is also renamed: {@link HashJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private static IntermediateResult reidentify(PromqlCommand cmd, IntermediateResult input) {
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        if (input.plan().anyMatch(UnionAll.class::isInstance)) {
            // A union keeps its output aligned with its branches by attribute ids, which re-identifying its subtree would
            // desync; only its value moves under a name of its own.
            Alias renamed = new Alias(input.valueColumn().source(), valueName, input.valueColumn(), new NameId());
            LogicalPlan plan = new Eval(cmd.source(), input.plan(), List.of(renamed));
            return new IntermediateResult(plan, input.schema(), renamed.toAttribute(), input.step(), input.pendingFilter(), input.kind());
        }
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

        return new HashJoin(
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

    /** Whether an operand combines pipelines of its own, a union or a join, rather than computing one aggregate. */
    private static boolean combines(IntermediateResult operand) {
        return operand.plan().anyMatch(p -> p instanceof UnionAll || p instanceof HashJoin);
    }

    /** Whether both operands' aggregates group by the same columns, so they fold into one. */
    private static boolean sharesGrouping(IntermediateResult left, IntermediateResult right) {
        List<Aggregate> leftAggregates = left.plan().collect(Aggregate.class);
        List<Aggregate> rightAggregates = right.plan().collect(Aggregate.class);
        if (leftAggregates.isEmpty() || rightAggregates.isEmpty()) {
            return true;
        }
        return groupingNames(leftAggregates.getFirst()).equals(groupingNames(rightAggregates.getFirst()))
            && leftAggregates.getFirst().groupings().size() == rightAggregates.getFirst().groupings().size();
    }

    private static Set<String> groupingNames(Aggregate aggregate) {
        Set<String> names = new HashSet<>();
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof NamedExpression ne) {
                names.add(ne.name());
            }
        }
        return names;
    }

    /** Fold left and right aggregates into a single plan. */
    private LogicalPlan emitBinaryOperatorAggregateExpression(IntermediateResult left, IntermediateResult right) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            Set<String> leftGroupingNames = new HashSet<>();
            for (Expression grouping : leftAgg.groupings()) {
                if (grouping instanceof NamedExpression ne) {
                    leftGroupingNames.add(ne.name());
                }
            }
            Set<String> rightGroupingNames = new HashSet<>();
            for (Expression grouping : rightAgg.groupings()) {
                if (grouping instanceof NamedExpression ne) {
                    rightGroupingNames.add(ne.name());
                }
            }
            boolean groupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                && leftGroupingNames.equals(rightGroupingNames);

            if (groupingsCompatible == false) {
                throw new VerificationException("binary operations between vectors with mismatched grouping keys are not yet supported");
            }

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

            return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
        });

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
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
