/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.SelectorV2;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContextV2.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;

/**
 * Shared state and assembly helpers for one PromQL translation. Every result carries its series' labels as one label
 * container ({@link IntermediateResult#labels}): translators state what they do to labels as calls over it
 * ({@code ts_unset}, {@code ts_keep}, {@code ts_set}, {@code ts_path}) and never decide which labels become columns, which
 * a pushdown rule does after translation. This context also preserves aggregation boundaries and assembles the command
 * output.
 */
public record TranslationContextV2(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {

    // An unbounded range query has no start or end; the step bucket takes the widest representable range.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    public TranslationContextV2(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, null);
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * intermediate results travel back up.
     */
    public IntermediateResult translate(LogicalPlan node) {
        return switch (node) {
            case InstantSelector selector -> SelectorV2.translate(selector, this);
            case RangeSelector selector -> SelectorV2.translate(selector, this);
            case LiteralSelector selector -> SelectorV2.translate(selector, this);
            case ValueTransformationFunction function -> FunctionLabelTranslation.translate(function, this);
            case VectorConversionFunction function -> FunctionLabelTranslation.translate(function, this);
            case WithinSeriesAggregate function -> FunctionLabelTranslation.translate(function, this);
            case ScalarFunction function -> FunctionLabelTranslation.translate(function, this);
            case ScalarConversionFunction function -> FunctionLabelTranslation.translate(function, this);
            default -> throw new QlIllegalArgumentException("PromQL plan node [{}] does not translate in V2 yet", node.nodeName());
        };
    }

    /** The labels of a vector without labels. */
    public static Expression emptyLabels(Source source) {
        return new ToTimeSeriesMetadata(source, Literal.keyword(source, "{}"));
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** The labels of a series read from the source: its whole label container. */
    public static Expression sourceLabels(Source source) {
        return new ToTimeSeriesMetadata(source, new TimeSeriesWithout(source));
    }

    /** Translates an independent branch with its own value and step identities. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContextV2(cmd, analyzer, step, branchTime);
        return run.finishIntermediate(branch, run.translate(branch), valueId);
    }

    /**
     * Finishes a branch already translated under this frame as an independent pipeline with its own step and value
     * identities: the same result as {@link #translateIntermediate(LogicalPlan, NameId, NameId)} without translating it
     * again. A branch translates under this frame only when its selectors share the frame's evaluation time, so the
     * translation stands; what the branch must not share with its siblings are the identities of the frame's step bucket
     * and shifted timestamp, which are re-keyed to fresh ones.
     */
    public IntermediateResult finishIntermediate(LogicalPlan branch, IntermediateResult translated, NameId stepId, NameId valueId) {
        Map<NameId, NameId> ids = new HashMap<>();
        Alias step = stepBucketAlias;
        if (step != null) {
            ids.put(step.id(), stepId);
            step = step.withId(stepId);
        }
        Expression branchTime = time;
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Attribute fresh = ref.withId(new NameId());
            ids.put(ref.id(), fresh.id());
            branchTime = fresh;
        }
        var run = new TranslationContextV2(cmd, analyzer, step, branchTime);
        return run.finishIntermediate(branch, reid(translated, ids), valueId);
    }

    /** The result with every attribute and alias keyed in {@code ids} re-keyed to its mapped identity. */
    private static IntermediateResult reid(IntermediateResult ir, Map<NameId, NameId> ids) {
        if (ids.isEmpty()) {
            return ir;
        }
        Function<Expression, Expression> rekey = e -> {
            if (e instanceof Attribute a && ids.containsKey(a.id())) {
                return a.withId(ids.get(a.id()));
            }
            if (e instanceof Alias a && ids.containsKey(a.id())) {
                return a.withId(ids.get(a.id()));
            }
            return e;
        };
        LogicalPlan plan = ir.plan().transformExpressionsDown(Expression.class, rekey);
        Expression value = ir.value().transformDown(Expression.class, rekey);
        Expression labels = ir.labels() == null ? null : ir.labels().transformDown(Expression.class, rekey);
        Attribute step = (Attribute) rekey.apply(ir.step());
        Expression filter = ir.pendingFilter() == null ? null : ir.pendingFilter().transformDown(Expression.class, rekey);
        return new IntermediateResult(plan, value, labels, step, filter, ir.kind());
    }

    /**
     * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
     * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public IntermediateResult eval(IntermediateResult t, Expression value) {
        if (t.kind().afterInitialAggregation == false) {
            return t.with(t.plan(), value);
        }
        Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
        return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute());
    }

    /**
     * The initial aggregate: a raw table collapsed to one row per step and series by the innermost
     * {@link TimeSeriesAggregate}, {@code function} applied in it. Passing the table's own value collapses it as is.
     */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, Expression function) {
        Expression labels = input.labels();
        Expression series = labels == null
            ? null
            : labels.collectFirstChildren(e -> e instanceof ToTimeSeriesMetadata t && t.field() instanceof TimeSeriesWithout)
                .stream()
                .findFirst()
                .orElse(null);
        if (series == null) {
            return emitInitialAggregate(input, labels, function);
        }
        // One row per series: the source labels are its identity, so a function over its samples is one value per group. The
        // changes made to the labels so far then apply to the collapsed series.
        IntermediateResult collapsed = emitInitialAggregate(input, series, perSeries(function));
        Expression key = collapsed.labels();
        return collapsed.withLabels(labels.transformUp(Expression.class, e -> e.semanticEquals(series) ? key : e));
    }

    /** A bare per-series function over a collapse keyed by series: one value per group, which {@code VALUES} reads. */
    private static Expression perSeries(Expression function) {
        Holder<Boolean> bare = new Holder<>(false);
        function.forEachDownMayReturnEarly((e, exit) -> {
            if (e instanceof TimeSeriesAggregateFunction) {
                bare.set(true);
                exit.set(true);
            } else if (e instanceof AggregateFunction) {
                exit.set(true);
            }
        });
        return bare.get() ? new Values(function.source(), function) : function;
    }

    /** The initial aggregate keyed by {@code labels}, the label container of each result series; null keys by step alone. */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, @Nullable Expression labels, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        Alias key = labelsKey(labels);
        LogicalPlan plan = new TimeSeriesAggregate(
            cmd.promqlPlan().source(),
            input.plan(),
            groupings(stepBucketAlias, key),
            aggregates(value, input.step(), key),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND,
            false
        );
        return new IntermediateResult(
            plan,
            value.toAttribute(),
            key == null ? null : key.toAttribute(),
            input.step(),
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    /** Regroups a collapsed table by {@code labels}, the label container of each result series; null groups by step alone. */
    public IntermediateResult emitAggregate(IntermediateResult input, @Nullable Expression labels, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a emitAggregate takes a collapsed table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        Alias key = labelsKey(labels);
        Attribute step = input.step();
        LogicalPlan plan = new Aggregate(cmd.source(), input.plan(), groupings(step, key), aggregates(value, step, key));
        return new IntermediateResult(
            plan,
            value.toAttribute(),
            key == null ? null : key.toAttribute(),
            step,
            input.pendingFilter(),
            Kind.AFTER_INITIAL_AGGREGATE
        );
    }

    private @Nullable Alias labelsKey(@Nullable Expression labels) {
        return labels == null ? null : new Alias(labels.source(), MetadataAttribute.TIMESERIES, labels);
    }

    /**
     * The source-time pushdown predicate. Expressed over the <b>raw</b> source timestamp (not the offset-shifted
     * evaluation timestamp) so it can push down to the index; the branch offset is instead folded into the bounds.
     * Expressing it over the shifted timestamp while also adjusting the bounds would apply the offset twice.
     */
    private Expression emitBySrcTimeFilter(LogicalPlan branch) {
        if (cmd.start().value() == null || cmd.end().value() == null) {
            return null;
        }
        var source = cmd.source();
        var offset = cmd.collectFirstOffsetForBranch(branch);
        var timestamp = cmd.timestamp();
        var window = cmd.sourceFilterWindow();
        var lo = new Sub(source, cmd.start(), Literal.timeDuration(source, window.plus(offset)), configuration());
        var hi = new Sub(source, cmd.end(), Literal.timeDuration(source, offset), configuration());
        return new And(source, new GreaterThanOrEqual(source, timestamp, lo), new LessThanOrEqual(source, timestamp, hi));
    }

    /** Pushes a branch's deferred predicate to the source before aggregation. */
    private LogicalPlan pushDownSrcTimestampFilter(LogicalPlan plan, Expression filterCondition) {
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof Filter f && f.child() instanceof EsRelation) {
                return new Filter(f.source(), f.child(), new And(f.source(), f.condition(), filterCondition));
            } else if (p instanceof EsRelation) {
                return new Filter(cmd.source(), p, filterCondition);
            }
            return p;
        });
    }

    /** The value column definition: the translateIntermediate's value expression, cast to double unless it provably is one. */
    private Alias emitValueDoubleCastExpression(Expression valueExpr, NameId valueId) {
        if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
            valueExpr = new ToDouble(cmd.source(), valueExpr);
        }
        return new Alias(cmd.source(), cmd.valueColumnName(), valueExpr, valueId);
    }

    /**
     * The {@code step} bucket for a branch: the {@link TStep} grouping key shared across all aggregation groupings,
     * derived from the (possibly offset-shifted) evaluation timestamp - so an {@code offset} shifts which samples
     * fall into each fixed output bucket without moving the buckets. {@code stepId} names the synthetic column.
     */
    private Alias emitStepBucketExpression(NameId stepId, Expression time) {
        Expression size;
        Expression start;
        Expression end;
        if (cmd.isInstantQuery()) {
            size = Literal.timeDuration(cmd.source(), cmd.resolveInstantQueryWindow());
            start = new Sub(cmd.source(), cmd.start(), size, configuration());
            end = cmd.end();
        } else {
            size = cmd.resolveTimeBucketSize();
            start = cmd.start().value() != null ? cmd.start() : Literal.dateTime(cmd.source(), EPOCH_MIN);
            end = cmd.end().value() != null ? cmd.end() : Literal.dateTime(cmd.source(), EPOCH_MAX);
        }
        var tstep = new TStep(size.source(), size, start, end, time, configuration());
        return new Alias(tstep.source(), cmd.stepColumnName(), tstep, stepId);
    }

    private boolean canCreateStepBucket() {
        if (cmd.timestamp() == null || cmd.timestamp().resolved() == false) {
            return cmd.isRangeQuery() == false || cmd.buckets() == null || cmd.buckets().value() == null;
        }
        return true;
    }

    private static List<Expression> groupings(Expression step, @Nullable Alias key) {
        return key == null ? List.of(step) : List.of(step, key);
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, @Nullable Alias key) {
        return key == null ? List.of(value, step) : List.of(value, step, key.toAttribute());
    }

    /** Finishes a branch after its operands have contributed their deferred source predicates. */
    private IntermediateResult finishIntermediate(LogicalPlan branch, IntermediateResult translated, NameId valueId) {
        IntermediateResult ir = doTranslateTryInline(translated);

        var plan = ir.plan();
        var value = ir.value();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources - skip over HashJoin.
        Expression timeFilter = plan.anyMatch(p -> p instanceof HashJoin) ? null : emitBySrcTimeFilter(branch);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }

        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                IntermediateResult collapsed = emitInitialAggregate(ir.with(plan, value), value);
                plan = collapsed.plan();
                value = collapsed.value();
                ir = collapsed;
            }
            // Filter-mode comparisons (metric > x) apply their Filter inside VectorBinaryOperator.translateOverSharedFrame
            // so nested parents (label_replace, aggregations, ...) see a filtered vector. Vector-matched comparisons
            // already filter inside the join path.
        }

        // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? ir.kind() : Kind.AFTER_INITIAL_AGGREGATE;
        // Same pin as bindResult: unresolved toAttribute freezes in Projects.
        return new IntermediateResult(plan, pinnedAttribute(valueAlias), ir.labels(), ir.step(), null, kind);
    }

    /** Same id as alias while unresolved - Projects must not freeze a toAttribute snapshot. */
    public static Attribute pinnedAttribute(Alias alias) {
        if (alias.resolved()) {
            return alias.toAttribute();
        }
        return new ReferenceAttribute(alias.source(), null, alias.name(), DataType.DOUBLE, Nullability.TRUE, alias.id(), true);
    }

    /** Folds a value depending only on the evaluation step into a local relation. */
    private IntermediateResult doTranslateTryInline(IntermediateResult result) {
        Attribute stepAttr = stepAttr();
        if (result.kind().constant
            || cmd.start().value() == null
            || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new IntermediateResult(plan, value, null, step, result.pendingFilter(), Kind.CONSTANT);
    }

    /** Defines the offset-adjusted evaluation timestamp at the source. */
    private LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
        Alias evaluationTime = evaluationTimestamp(branch);
        return evaluationTime == null
            ? plan
            : plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(evaluationTime)));
    }

    /** Source readers and fused branches use the same offset-adjusted timestamp definition. */
    private @Nullable Alias evaluationTimestamp(LogicalPlan branch) {
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(branch);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            return new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
        }
        return null;
    }

    // -- core --

    /**
     * The synthesized attribute of a PromQL node ({@link PromqlPlan#translate}): its translated subtree and the state not
     * yet represented by plan operators. Mid-descent the value is a (possibly not yet materialized) expression parents
     * compose into larger expressions; a finished table's value is a defined column ({@link #valueColumn()}).
     */
    public record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* A sample expression until initial aggregation materializes it as a column. */
        Expression value,
        /* The series' label container, composed like the value until an aggregate keys by it; null for no labels. */
        @Nullable Expression labels,
        Attribute step,
        /* Deferred source predicates can be fused into the operand's aggregate functions. */
        Expression pendingFilter,
        Kind kind
    ) {
        /** The lifecycle of a result. A constant is always a finished (aggregation-free) local relation. */
        public enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true);

            /** A local relation needs no source filtering or aggregation. */
            public final boolean constant;
            /** Value expressions above this boundary must materialize before being consumed. */
            public final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(LogicalPlan plan, Expression value, Expression labels, Attribute step, Expression selectorFilter) {
            this(plan, value, labels, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This result rebuilt around a new plan and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return new IntermediateResult(plan, value, labels, step, pendingFilter, kind);
        }

        /** This result with its series' labels changed: {@code labels} are calls over the current ones. */
        public IntermediateResult withLabels(@Nullable Expression labels) {
            return new IntermediateResult(plan, value, labels, step, pendingFilter, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }
    }

    /**
     * A count is at least 1 for an element: {@code count} counts a group's series, {@code count_over_time} a series'
     * samples in the window, and a group or series with none is no element. Maps a count of 0 to null so the element
     * drops like any other without a value.
     */
    public static Expression zeroCountAsNull(Expression count) {
        Expression empty = new Equals(count.source(), count, new Literal(count.source(), 0L, DataType.LONG));
        return new Case(count.source(), empty, List.of(Literal.NULL, count));
    }

    /**
     * A range function over a series without a sample in the window has no element in Prometheus. A series can still
     * reach the aggregate without one: its rows carry only other fields, or an operand's matchers reject them inside an
     * aggregate shared with another operand. There {@code count_over_time} reads 0 and {@code present_over_time} false;
     * both map to null so the element drops like any other without a value.
     * <p>
     * {@code absent_over_time} is the mirror: it has an element only for a series without a sample, so only true stays.
     * A series the operand never selects reads true too - its matchers reject every row, or only another operand's metric
     * has values - so true stays only for a series the operand selects.
     */
    public static Expression emptyRangeAsNull(Expression function, Expression time) {
        if (function instanceof CountOverTime) {
            return zeroCountAsNull(function);
        }
        if (function instanceof PresentOverTime present) {
            return new Case(present.source(), present, List.of(Literal.TRUE, Literal.NULL));
        }
        if (function instanceof AbsentOverTime absent) {
            Expression absentFromSelected = new And(absent.source(), selectedBy(absent, time), absent);
            return new Case(absent.source(), absentFromSelected, List.of(Literal.TRUE, Literal.NULL));
        }
        return function;
    }

    /**
     * Whether the operand of {@code absent} reads a row of the series in the window; the operand's matchers apply once its
     * filter attaches to its time-series functions. A metric selector selects a series only where the metric has a value:
     * rows are read for every metric the aggregate references
     * ({@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics}), so a row without a value of
     * this one can come from another operand's metric. For a metric field {@code absent} therefore never stays true.
     */
    private static Expression selectedBy(AbsentOverTime absent, Expression time) {
        if (absent.field() instanceof Attribute field && field.resolved() && field.isMetric()) {
            return new Not(absent.source(), absent);
        }
        return new PresentOverTime(absent.source(), time, Literal.TRUE, absent.window());
    }

    /**
     * The command's ES|QL plan: the query translated, projected to the command's declared output, each declared label read
     * off the result's label container.
     */
    public LogicalPlan translateFinal() {
        IntermediateResult result = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
        boolean constant = result.kind().constant;
        return finish(emitFinalProjection(result.plan(), result.labels(), constant), constant);
    }

    /** Keeps only steps within the query range; steps are anchored at {@code start} and offset-independent. */
    private LogicalPlan emitByStepFilter(LogicalPlan plan) {
        var source = cmd.source();
        var step = cmd.stepAttribute();
        var start = cmd.start();
        var end = cmd.end();
        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    public static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    /** Every command drops missing samples and bounds its evaluation steps after exposing the public columns. */
    private LogicalPlan finish(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), plan, cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * Projects the command output. Value and step bind to the finished table's columns by name. Every label the command
     * declares reads the result's label container {@code labels}, under its declared name, id and type, and the declared
     * {@code _timeseries} is the container without them. A constant table carries no labels. Typed {@code _timeseries}
     * stays internal: it binds under an id of its own and the command exposes its keyword form.
     */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, @Nullable Expression labels, boolean constant) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(attr.name(), attr);
        }
        var projected = new ArrayList<NamedExpression>(cmd.output());
        var evals = new ArrayList<Alias>();
        List<Attribute> declared = cmd.promqlPlan().output();
        // A column exposed only to order the result is read from the container but stays in _timeseries too
        Set<NameId> orderingOnly = new HashSet<>();
        cmd.promqlPlan().forEachDown(LogicalPlan.class, p -> {
            if (p instanceof ResultOrderingFunction ordering) {
                ordering.orderingOnlyColumns().forEach(column -> orderingOnly.add(column.id()));
            }
        });
        List<String> named = PromqlLabels.labelNames(
            declared.stream().filter(a -> PromqlLabels.isTagsLabel(a) == false && orderingOnly.contains(a.id()) == false).toList()
        );
        Source source = cmd.promqlPlan().source();
        Alias rendered = null;
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i).toAttribute();
            // the value and step lead the declared output, then the result's labels
            if (i >= 2 && constant == false) {
                Attribute label = declared.get(i - 2);
                if (PromqlLabels.isTagsLabel(label)) {
                    Expression rest = labels == null
                        ? TranslationContextV2.emptyLabels(source)
                        : new TimeSeriesMetadataFilter(labels.source(), labels, PromqlLabels.labelLiterals(labels.source(), named), true);
                    Alias labelContainer = new Alias(source, attr.name(), rest);
                    evals.add(labelContainer);
                    rendered = new Alias(
                        source,
                        attr.name(),
                        new ToString(source, labelContainer.toAttribute(), configuration()),
                        attr.id()
                    );
                    projected.set(i, labelContainer.toAttribute());
                    continue;
                }
                Expression value = labels == null
                    ? new Literal(source, null, attr.dataType())
                    : asType(
                        new TimeSeriesMetadataPath(
                            labels.source(),
                            labels,
                            Literal.keyword(labels.source(), PromqlLabels.labelName(label))
                        ),
                        attr.dataType());
                Alias alias = new Alias(source, attr.name(), value, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
                continue;
            }
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                if (lookupAttr.resolved()) {
                    // Remap the finished table's column onto the command's id via Eval, then project the attribute.
                    evals.add(alias);
                    projected.set(i, alias.toAttribute());
                } else {
                    // Union branches often expose an unresolved value/step until Bind resolves aggregates under them.
                    // Wrapping that in Eval makes Alias.toAttribute() an UnresolvedAttribute that shadows the real
                    // column by name once the child resolves (Unknown column [value]/result]). Keep the Alias in
                    // the Project so ResolveRefs rebinds it after the union output is refreshed.
                    projected.set(i, alias);
                }
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        plan = new Project(cmd.source(), plan, projected);
        if (rendered == null) {
            return plan;
        }
        plan = new Eval(cmd.source(), plan, List.of(rendered));
        // in the declared order: the Eval appends the rendered column
        var exposed = new ArrayList<NamedExpression>(projected.size());
        for (NamedExpression projection : projected) {
            exposed.add(projection.name().equals(rendered.name()) ? rendered.toAttribute() : projection.toAttribute());
        }
        return new Project(cmd.source(), plan, exposed);
    }

    /** A label's text as the declared label's type: a label read from the container is text, a dimension may not be. */
    private Expression asType(Expression text, DataType type) {
        if (type == DataType.KEYWORD || type == DataType.NULL) {
            return text;
        }
        return EsqlDataTypeConverter.converterFunctionFactory(type).apply(text.source(), text, configuration());
    }

}
