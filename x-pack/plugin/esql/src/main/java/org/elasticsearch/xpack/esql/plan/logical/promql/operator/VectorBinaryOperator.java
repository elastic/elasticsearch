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
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesReduction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.any;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.of;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.sub;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationConstraint.union;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.nullAlias;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.ref;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    /** The packed identity of a series with no labels left, as {@code TimeSeriesMetadataFieldBlockLoader} emits it. */
    private static final String EMPTY_PACKING = "{}";

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

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
        List<Attribute> matched = computeMatchedAttributes();
        if (dropMetricName == false) {
            return matched;
        }
        // Arithmetic and `bool` comparisons drop the metric name from the result, whichever operand carried it and
        // however it is spelled (`__name__` or a passthrough `labels.__name__`).
        return matched.stream().filter(attribute -> LabelMatcher.NAME.equals(PromqlLabels.labelName(attribute)) == false).toList();
    }

    /** The result columns the match produces, before the operator's own metric-name handling. */
    private List<Attribute> computeMatchedAttributes() {
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
            // Same label set on both sides: the result carries the left operand's columns, like every other
            // one-to-one match.
            return leftAttrs;
        } else if (hasPackedLabels(leftAttrs) && hasPackedLabels(rightAttrs) == false) {
            // Default matching of a packed operand against a closed one pairs only where the packed series carries exactly
            // the closed side's labels, so the result's label set is the closed side's, whichever side it is.
            outputLabels = new HashSet<>(rightLabels);
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

    // ---------- translation ----------

    /** Explicit vector matching translates as a join; other binary operators compose over the operands' shared frame. */
    @Override
    public TranslationResult translate(TranslationContext translation) {
        if (match.filter() == VectorMatch.Filter.NONE && match.grouping() == Joining.NONE) {
            boolean scalarOperand = left().resolved() && getType(left()) == SCALAR || right().resolved() && getType(right()) == SCALAR;
            boolean nestedMatch = anyMatchVectorBinaryOperator(left()) || anyMatchVectorBinaryOperator(right());
            // Operands over one label set fold into a shared aggregate: two raw selectors pair per series inside one
            // collapse, two closed aggregates over the same keys fuse. Everything else matches like Prometheus does, pair
            // by pair on the actual label sets, which only the join expresses: different closed label sets, a closed
            // operand against a packed one, and a packed operand that is already a table (a reduction).
            boolean fusable = nestedMatch == false
                && hasMismatchedLabelSets() == false
                && closedAgainstPacked() == false
                && reductionOperand() == false;
            if (scalarOperand || fusable) {
                return translateFused(translation);
            }
        }
        return translateJoin(translation);
    }

    /** One operand names its labels while the other packs them: only the join can compare the two label sets. */
    private boolean closedAgainstPacked() {
        return hasPackedLabels(left().output()) != hasPackedLabels(right().output());
    }

    /** A reduction ({@code topk}) is a finished table over packed series: it pairs through the join, not a shared collapse. */
    private boolean reductionOperand() {
        return left().anyMatch(AcrossSeriesReduction.class::isInstance) || right().anyMatch(AcrossSeriesReduction.class::isInstance);
    }

    private static boolean anyMatchVectorBinaryOperator(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof VectorBinaryOperator vbo
                && (vbo.match.filter() != VectorMatch.Filter.NONE || vbo.match.grouping() != Joining.NONE)
        );
    }

    /**
     * Composes the operator as an expression over the operands' shared aggregate; the operands pass the requirement through.
     * A name-dropping operator (arithmetic, {@code bool} comparison) requires nothing of {@code __name__} from its operands
     * and discards the column from the result; a filter comparison returns the left side unchanged, metric name included.
     */
    private TranslationResult translateFused(TranslationContext translation) {
        List<String> name = List.of(LabelMatcher.NAME);
        TranslationConstraint required = translation.required();
        // IN: required, on both operands; a name-dropping operator: - `__name__`, two vectors pair on any + required - `__name__`
        boolean vectors = getType(left()) != SCALAR && getType(right()) != SCALAR;
        TranslationConstraint below = dropMetricName ? sub(vectors ? union(required, any()) : required, of(name)) : required;
        boolean scalarTableAgainstVector = (isScalarTable(left()) && getType(right()) != SCALAR)
            || (isScalarTable(right()) && getType(left()) != SCALAR);
        if (scalarTableAgainstVector) {
            return translateBroadcast(translation, below);
        }
        TranslationResult left = translation.translate(left(), below);
        Expression leftExpr = new ToDouble(left.value().source(), left.value());
        if (this instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            return left.with(left.plan(), leftExpr);
        }
        TranslationResult right = translation.translate(right(), below);
        if (vectors && (left.kind().constant || right.kind().constant)) {
            // A constant vector is its own table, one `{}` row per step: like any two tables over different sources, it
            // pairs with the other vector through the join, which compares the label sets ({} matches {} alone).
            return translateJoin(translation);
        }
        if (dropMetricName) {
            boolean leftRaw = isVectorBeforeInitialAgg(left(), left);
            boolean rightRaw = isVectorBeforeInitialAgg(right(), right);
            if (leftRaw && rightRaw) {
                return collapseRawOperands(translation, left, right);
            }
            // A raw vector next to an aggregated operand (a nested `a / (b + c)`) collapses first, over the same
            // identity, so the two fuse below as aggregates over one grouping.
            if (leftRaw && right.kind().afterInitialAggregation) {
                left = translation.aggregate(left, new Max(left.value().source(), left.value()));
                leftExpr = new ToDouble(left.value().source(), left.value());
            }
            if (rightRaw && left.kind().afterInitialAggregation) {
                right = translation.aggregate(right, new Max(right.value().source(), right.value()));
            }
        }
        Expression rightExpr = new ToDouble(right.value().source(), right.value());
        Expression binaryExpr = binaryOp.asFunction().create(source(), leftExpr, rightExpr, translation.configuration());

        LogicalPlan plan;
        Expression filter;
        TranslationResult ir;
        if (left.kind().afterInitialAggregation && right.kind().afterInitialAggregation) {
            if (left.plan().collect(Aggregate.class).size() != 1 || right.plan().collect(Aggregate.class).size() != 1) {
                // Fusion merges two aggregates over one source into one aggregate. An operand that is itself an
                // aggregate over a collapsed pair (`sum by (k) (a / b)`) has two levels and cannot be merged, so the
                // sides match through the join instead, each as its own finished table.
                return translateJoin(translation);
            }
            plan = fuse(left, right, dropMetricName);
            ir = left;
            filter = null;
        } else {
            ir = getType(left()) != SCALAR ? left : getType(right()) != SCALAR ? right : left.kind().afterInitialAggregation ? left : right;
            plan = ir.plan();
            filter = combineAndNullable(Arrays.asList(left.pendingFilter(), right.pendingFilter()));
        }
        // a constant table against a scalar expression stays a constant table
        Kind kind = ir.kind().constant ? Kind.CONSTANT
            : left.kind().afterInitialAggregation || right.kind().afterInitialAggregation ? Kind.AFTER_INITIAL_AGGREGATE
            : Kind.BEFORE_INITIAL_AGGREGATE;
        // OUT: the vector operand's labels (left when both are) - `__name__` for a name-dropping operator
        Map<TranslationColumn, Attribute> labels = dropMetricName ? ir.drop(name).labels() : ir.labels();
        TranslationResult result = translation.eval(new TranslationResult(plan, labels, null, ir.step(), filter, kind), binaryExpr);
        return vectors && kind == Kind.AFTER_INITIAL_AGGREGATE ? dropUnmatched(result) : result;
    }

    /**
     * A series without a partner is not part of a vector operation's result. The paired collapse and the fused aggregate
     * compute the operator row by row and leave a null where one side is missing; the rows go here, before an enclosing
     * aggregate could count them as a group ({@code count by (k) (a / b)} has no group for an unmatched series).
     */
    private static TranslationResult dropUnmatched(TranslationResult paired) {
        Attribute value = paired.valueColumn();
        return paired.with(new Filter(value.source(), paired.plan(), new IsNotNull(value.source(), value)), value);
    }

    /** A raw (not yet collapsed) instant-vector operand, as opposed to a scalar or an aggregated table. */
    private static boolean isVectorBeforeInitialAgg(LogicalPlan operand, TranslationResult translated) {
        return getType(operand) != SCALAR && translated.kind() == Kind.BEFORE_INITIAL_AGGREGATE;
    }

    /**
     * A scalar operand computed from a vector ({@code scalar(sum(m))}, {@code scalar(m{..}) + 1}, {@code scalar(vector(1))}):
     * a table of one value per step, as opposed to a literal or {@code time()}, which are expressions over any row.
     */
    private static boolean isScalarTable(LogicalPlan operand) {
        return getType(operand) == SCALAR && operand.anyMatch(ScalarConversionFunction.class::isInstance);
    }

    /**
     * A computed scalar applies to every element of the vector operand (Prometheus: "the operator is applied to the value
     * of every data sample in the vector"). The scalar table has one row per step and no labels, so the two operands join
     * on the step alone, every vector row matching the step's one scalar; the result carries the vector operand's labels
     * without the metric name.
     */
    private TranslationResult translateBroadcast(TranslationContext translation, TranslationConstraint below) {
        boolean scalarLeft = isScalarTable(left());
        LogicalPlan vectorNode = scalarLeft ? right() : left();
        LogicalPlan scalarNode = scalarLeft ? left() : right();
        // IN: the vector operand under the operator's own requirement; the scalar operand exposes no labels
        TranslationResult vector = translation.translateOperand(vectorNode, below);
        TranslationResult scalar = reidentify(translation.cmd(), translation.translateOperand(scalarNode, of()));

        Source source = translation.cmd().source();
        LogicalPlan scalarPlan = new Project(source, scalar.plan(), List.of(scalar.step(), scalar.valueColumn()));
        LogicalPlan join = new InnerJoin(
            source,
            vector.plan(),
            scalarPlan,
            List.of(vector.step()),
            List.of(scalar.step()),
            List.of(scalar.valueColumn()),
            false
        );
        Expression leftValue = scalarLeft ? scalar.value() : vector.value();
        Expression rightValue = scalarLeft ? vector.value() : scalar.value();
        // OUT: the vector operand's labels, - `__name__` for a name-dropping operator
        Map<TranslationColumn, Attribute> labels = dropMetricName ? vector.drop(List.of(LabelMatcher.NAME)).labels() : vector.labels();
        return bindResult(translation, leftValue, rightValue, vector.step(), join, new Output(labels, List.of()));
    }

    /**
     * Pairs two raw vector operands per series and step before applying the operator. The collapse's first phase runs per
     * physical time series, and a metric ingested as its own documents (one per sample, {@code labels.__name__} a
     * dimension) never shares a physical series with another metric: computed row by row, the operator would see one
     * operand null in every row. Aggregating each operand within its group first is exact - the group is every label but
     * {@code __name__}, so it holds at most one series per metric - and puts both values in one row. The eager collapse
     * hands an aggregated table to any enclosing aggregate, which regroups it like any other collapsed operand.
     */
    private TranslationResult collapseRawOperands(TranslationContext translation, TranslationResult left, TranslationResult right) {
        // Each side keeps its operand's source, so the paired expression still reads as `a <op> b` over the selectors.
        Source leftSource = left.value().source();
        Source rightSource = right.value().source();
        // Each operand's pending filter is attached to the inner TSAF so it fires in the first phase of the
        // TimeSeriesAggregate, where source fields (like `labels.__name__`) are still present. Combining the two
        // filters with AND on the source relation would exclude all rows for cross-metric binary ops: a row matches
        // `labels.__name__ = "metric_a"` or `labels.__name__ = "metric_b"` but never both simultaneously.
        Expression leftMax = applySeriesFilter(new Max(leftSource, emptyCountAsNull(left.value())), left.pendingFilter());
        Expression rightMax = applySeriesFilter(new Max(rightSource, emptyCountAsNull(right.value())), right.pendingFilter());
        Expression leftExpr = new ToDouble(leftSource, leftMax);
        Expression rightExpr = new ToDouble(rightSource, rightMax);
        Expression paired = binaryOp.asFunction().create(source(), leftExpr, rightExpr, translation.configuration());
        // Pair over every label but `__name__`; an enclosing aggregate regroups the paired rows.
        assert right.shape().excludes(List.of(LabelMatcher.NAME))
            : "[INVARIANT]: a name-dropping operator pairs its operands without `__name__`, got " + right.labels();
        TranslationResult raw = new TranslationResult(
            right.plan(),
            right.labels(),
            null,
            right.step(),
            null,
            Kind.BEFORE_INITIAL_AGGREGATE
        );
        return dropUnmatched(translation.aggregate(raw, paired));
    }

    private static Expression applySeriesFilter(Expression expr, Expression filter) {
        if (filter == null) {
            return expr;
        }
        return expr.transformDown(TimeSeriesAggregateFunction.class, f -> f.withFilter(filter));
    }

    /**
     * A count is at least 1 for an element: {@code count} counts a group's series, {@code count_over_time} a series'
     * samples in the window, and a group or series with none is no element. Fused with the other operand's aggregate, a
     * count reads 0 in a group that only the other operand's rows create, so a 0 is the absence of an element: it becomes
     * null and the pair drops like any unmatched one. Other aggregates are null over no rows already.
     */
    private static Expression emptyCountAsNull(Expression value) {
        return value.transformUp(e -> {
            if (e instanceof Count || e instanceof CountOverTime) {
                Expression empty = new Equals(e.source(), e, new Literal(e.source(), 0L, DataType.LONG));
                return new Case(e.source(), empty, List.of(Literal.NULL, e));
            }
            return e;
        });
    }

    private static List<? extends Expression> emptyCountsAsNull(List<? extends Expression> aggregates) {
        return aggregates.stream()
            .map(e -> e instanceof Alias a ? new Alias(a.source(), a.name(), emptyCountAsNull(a.child()), a.id()) : e)
            .toList();
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
     * Folds the left and right aggregates into a single plan. When the operator drops the metric name, a {@code __name__}
     * grouping is dropped from both sides first: Prometheus pairs series on their labels without the metric name, and over
     * the remote-write layout the two operands never share one (each metric is its own {@code __name__}), so grouping on
     * it would put the sides in disjoint groups. A selector names one metric, so within an operand the grouping is
     * constant and dropping it merges nothing.
     */
    private static LogicalPlan fuse(TranslationResult left, TranslationResult right, boolean dropMetricName) {
        var names = new TemporaryNameGenerator.Monotonic();
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();
        List<Expression> rightGroupings = dropMetricName ? withoutMetricName(rightAgg.groupings()) : rightAgg.groupings();

        var result = left.plan().transformDown(Aggregate.class, leftAgg -> {
            List<Expression> leftGroupings = dropMetricName ? withoutMetricName(leftAgg.groupings()) : leftAgg.groupings();
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
            uniqueAggregates.addAll(emptyCountsAsNull(withSeriesFilter(leftAgg.aggregates(), left.pendingFilter())));
            uniqueAggregates.addAll(emptyCountsAsNull(withSeriesFilter(rightAggregates, right.pendingFilter())));
            if (dropMetricName) {
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

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

    /**
     * Translates explicit vector matching as an {@link InnerJoin}: each operand becomes an independent series pipeline,
     * joined on shared {@code step} + label keys, and the result value is computed on the joined rows. The operands
     * compile against the labels the join requires, like any other requirement push-down: a required label comes back as
     * a concrete column wherever the operand can carry it, and a label the operand dropped stays absent and null-fills at
     * the join. The block has three rules: how the sides are ordered (which operand probes, which builds and is
     * re-identified), how the fields are placed (each side's match key packed next to step, the build side's value and
     * {@code group_x} labels carried across, every result label bound to the operand carrying it or to null) and what is
     * projected (the build side down to its join fields, the result down to value, step and the required labels).
     */
    private TranslationResult translateJoin(TranslationContext translation) {
        // TODO: revisit once expression like foo on(a,b) / bar is supported
        // IN: declared output labels + required names (explicit matching: operands have concrete label sets, so every
        // label is named); key on top: on (k) -> k; ignoring (i) -> any - i; none -> every label either operand names
        // plus the packing of whatever else a packed operand carries
        TranslationConstraint declared = union(of(PromqlLabels.labelNames(output())), of(translation.required().names()));
        DynamicColumnList residual = null;
        TranslationConstraint in;
        switch (match.filter()) {
            case ON -> in = union(declared, of(match.filterLabels()));
            case IGNORING -> in = union(declared, sub(any(), of(match.filterLabels())));
            case NONE -> {
                // Prometheus compares whole label sets (the name aside): the key is every label either operand names, null
                // where a side lacks it, and the packing of everything else, which a pair needs empty - a packed series
                // matches a closed one only when it carries no further label, and two packed operands when the rest agrees.
                var keys = new TreeSet<>(declared.names());
                keys.addAll(labelNames(left().output()));
                keys.addAll(labelNames(right().output()));
                keys.remove(LabelMatcher.NAME);
                declared = of(keys);
                var except = new HashSet<>(keys);
                except.add(LabelMatcher.NAME);
                residual = new DynamicColumnList(except);
                in = union(declared, sub(any(), of(except)));
            }
            default -> throw new IllegalStateException("unknown vector match filter [" + match.filter() + "]");
        }
        TranslationResult left = translation.translateOperand(left(), in);
        TranslationResult right = translation.translateOperand(right(), in);

        // Orientation: the probe side keeps its identities; the build side is re-identified so a self-join of
        // structurally identical operands has distinct attributes on each side.
        PromqlCommand cmd = translation.cmd();
        boolean probeRight = match.grouping() == Joining.RIGHT;
        TranslationResult probe = probeRight ? right : left;
        TranslationResult build = reidentify(cmd, probeRight ? left : right);
        Expression leftValue = probeRight ? build.value() : probe.value();
        Expression rightValue = probeRight ? probe.value() : build.value();

        LogicalPlan join = emitJoin(translation, probe, build, keyLabels(left, right), residual);
        // OUT: every declared label bound to the operand carrying it, or null; the probe's remaining packed labels if any
        Output output = bindOutput(declared, probe, build, residual);
        return bindResult(translation, leftValue, rightValue, probe.step(), join, output);
    }

    /** The label names an operand declares as columns, its packed identity aside. */
    private static List<String> labelNames(List<Attribute> output) {
        return output.stream()
            .filter(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()) == false)
            .map(PromqlLabels::labelName)
            .distinct()
            .toList();
    }

    /** One side of the join: its plan with the key columns defined, and the fields the join matches on. */
    private record Input(LogicalPlan plan, List<Attribute> fields) {}

    /** The join result's label columns, and the ones defined as null rather than taken from an operand. */
    private record Output(Map<TranslationColumn, Attribute> labels, List<Alias> nullFills) {}

    /**
     * The labels both sides pack into the match key, in one shared order: the on(...) labels as written, otherwise the
     * union of the operands' labels minus the ignored ones, sorted by name. A side that lacks a key label packs null
     * there, so the key behaves like a Prometheus signature: a label absent on both sides does not discriminate, a label
     * present on one side only never matches. Operands over different label sets therefore evaluate to the empty
     * vector, and the order in which each operand declares its labels is irrelevant.
     */
    private List<String> keyLabels(TranslationResult left, TranslationResult right) {
        if (match.filter() == VectorMatch.Filter.ON) {
            return List.copyOf(match.filterLabels());
        }
        var names = new TreeSet<>(left.statics());
        names.addAll(right.statics());
        names.removeAll(match.filterLabels());
        // A Prometheus signature never includes the metric name: operands of different metrics pair on their other labels.
        names.remove(LabelMatcher.NAME);
        return List.copyOf(names);
    }

    /**
     * The operator's value computed on the joined rows, then the finished table: value and step exposed under this
     * frame's identities, null-fills defined, comparison filter mode applied, and everything else projected away.
     */
    private TranslationResult bindResult(
        TranslationContext translation,
        Expression leftValue,
        Expression rightValue,
        Attribute step,
        LogicalPlan join,
        Output output
    ) {
        PromqlCommand cmd = translation.cmd();
        Expression lhsExpr = new ToDouble(leftValue.source(), leftValue);
        Expression rhsExpr = new ToDouble(rightValue.source(), rightValue);
        Expression value = binaryOp.asFunction().create(source(), lhsExpr, rhsExpr, translation.configuration());
        Expression filter = null;
        if (this instanceof VectorBinaryComparison comparison) {
            filter = comparison.filterMode() ? value : null;
            value = comparison.filterMode() ? lhsExpr : new ToDouble(value.source(), value);
        }
        // Expose the step under the enclosing frame's step identity so enclosing translations (union branches, parent
        // aggregates) resolve it by id, not just by name.
        Alias stepAlias = new Alias(step.source(), step.name(), step, translation.stepAttr().id());
        Alias valueAlias = new Alias(source(), cmd.valueColumnName(), value, new NameId());
        List<Alias> definitions = new ArrayList<>(List.of(valueAlias, stepAlias));
        definitions.addAll(output.nullFills());
        LogicalPlan plan = new Eval(cmd.source(), join, definitions);
        if (filter != null) {
            plan = new Filter(source(), plan, filter);
        }
        List<NamedExpression> projected = new ArrayList<>(List.of(valueAlias.toAttribute(), stepAlias.toAttribute()));
        projected.addAll(output.labels().values());
        plan = new Project(cmd.source(), plan, projected);

        return new TranslationResult(
            plan,
            output.labels(),
            valueAlias.toAttribute(),
            stepAlias.toAttribute(),
            null,
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    /**
     * The build operand under fresh identities, so a self-join of structurally identical operands has distinct
     * attributes on each side. Its value column is also renamed: {@link InnerJoin#output()} merges output by NAME (see
     * {@code NamedExpressions#mergeOutputAttributes}), so a build-side column still called {@code value} would shadow
     * the probe side's value column that the operator's expression references.
     */
    private static TranslationResult reidentify(PromqlCommand cmd, TranslationResult input) {
        Map<NameId, NameId> ids = new HashMap<>();
        String valueName = TemporaryNameGenerator.locallyUniqueTemporaryName(cmd.valueColumnName());
        LogicalPlan plan = input.plan()
            .transformExpressionsDown(Expression.class, e -> reidExpr(renamed(e, cmd.valueColumnName(), valueName), ids));
        Expression value = reidExpr(renamed(input.valueColumn(), cmd.valueColumnName(), valueName), ids);
        Attribute step = (Attribute) reidExpr(input.step(), ids);
        var reidentified = new LinkedHashMap<TranslationColumn, Attribute>();
        input.labels().forEach((column, attribute) -> reidentified.put(column, (Attribute) reidExpr(attribute, ids)));
        return new TranslationResult(plan, reidentified, value, step, input.pendingFilter(), input.kind());
    }

    /** The inner join of the two operands on step plus the packed match key. */
    private LogicalPlan emitJoin(
        TranslationContext translation,
        TranslationResult probe,
        TranslationResult build,
        List<String> keyLabels,
        DynamicColumnList residual
    ) {
        PromqlCommand cmd = translation.cmd();
        Input probeInput = emitInput(translation, probe, keyLabels, residual);
        Input buildInput = emitInput(translation, build, keyLabels, residual);

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
    private List<Attribute> addedFields(TranslationResult build) {
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

    /**
     * One side's plan with its match key defined and packed next to step; step alone when the key is empty. The key is the
     * shared key labels, each as the operand's own column or a null where it lacks the label, plus the packings an opaque
     * operand carries that already exclude the ignored labels.
     */
    private Input emitInput(TranslationContext translation, TranslationResult input, List<String> keyLabels, DynamicColumnList residual) {
        Source source = translation.cmd().source();
        // KEY: the shared key labels (null where lacking) + the packing of the remaining labels (null where the side names
        // every label; a packed side's `{}` when nothing remains) - or, under ignoring, an opaque operand's packings
        TranslationResult keyed = translation.bind(input, of(keyLabels), source);
        var key = new LinkedHashMap<>(keyed.labels());
        LogicalPlan plan = keyed.plan();
        if (residual != null) {
            Attribute packing = input.labels().get(residual);
            if (packing == null) {
                // a side naming every label has nothing left: the packing of no labels, as the block loader emits it
                Alias fill = new Alias(source, residual.name(), Literal.keyword(source, EMPTY_PACKING));
                plan = new Eval(source, plan, List.of(fill));
                packing = fill.toAttribute();
            }
            key.put(residual, packing);
        } else if (match.filter() != VectorMatch.Filter.ON) {
            input.labels().forEach((column, attribute) -> {
                if (column instanceof DynamicColumnList packing && packing.except().containsAll(match.filterLabels())) {
                    key.put(column, attribute);
                }
            });
        }
        if (key.isEmpty()) {
            return new Input(plan, List.of(input.step()));
        }
        List<Attribute> fields = keyed.with(plan, key, keyed.value()).attributes();
        Attribute packed = new ReferenceAttribute(source, null, PackDims.PACKED_FIELD_NAME, DataType.KEYWORD);
        return new Input(new PackDims(source, plan, fields, packed), List.of(input.step(), packed));
    }

    /** The join result's label columns: every required label bound to the operand carrying it, or to null. */
    private Output bindOutput(
        TranslationConstraint required,
        TranslationResult probe,
        TranslationResult build,
        DynamicColumnList residual
    ) {
        var columns = new LinkedHashMap<TranslationColumn, Attribute>();
        var nullFills = new ArrayList<Alias>();
        // Two packed operands pair on their remaining labels, which the result keeps as its open identity; against a closed
        // operand the remainder is `{}` in every matched row and the closed side's labels say it all.
        if (residual != null) {
            Attribute packing = probe.labels().get(residual);
            if (packing != null && build.labels().get(residual) != null) {
                columns.put(residual, packing);
            }
        }
        for (String name : required.names()) {
            // A label the match semantics dropped (e.g. on(...) narrowing) may still be required by an enclosing
            // translation; it must come back null rather than leak through from an operand.
            Attribute declaredAttr = PromqlLabels.find(output(), name);
            if (declaredAttr == null) {
                nullFills.add(nullAlias(ref(name)));
                columns.put(new Static(name), nullFills.getLast().toAttribute());
                continue;
            }
            Attribute attribute = match.groupingLabels().contains(name) ? build.label(name) : probe.label(name);
            if (attribute == null) {
                // Null-fill under the operator's own attribute when the carrying operand lacks the label, so the command
                // projection binds it by identity. The join may still carry that id from the other operand (a group_x
                // label the probe has and the build lacks): the definition deliberately shadows it, which is why these
                // null-fills are stated here rather than derived from what the join output lacks.
                nullFills.add(nullAlias(declaredAttr));
                attribute = nullFills.getLast().toAttribute();
            }
            columns.put(new Static(name), attribute);
        }
        return new Output(columns, nullFills);
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
}
