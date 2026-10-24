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
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.LabelEval;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.Count.zeroCountAsNull;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.intersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.open;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.project;

/**
 * Shared state and assembly helpers for the aggregation prototype. Requirements descend the tree; results carry
 * either named labels or one complete metadata value. This context binds and transforms labels, preserves
 * aggregation boundaries, and assembles the command output.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* Labels to read from the source; null requests all labels, an empty set requests none. */
    @Nullable Set<String> requiredLabels,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    public TranslationContext {
        if (requiredLabels != null) {
            requiredLabels = Collections.unmodifiableSet(new LinkedHashSet<>(requiredLabels));
        }
    }

    /** The command exposes every label of the result series: the full open label space. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, null, null);
    }

    /** Translate an independent child without changing the enclosing branch's timing or aggregation state. */
    public TranslationContext withPushedDown(@Nullable Collection<String> labels) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, labels == null ? null : new LinkedHashSet<>(labels), time);
    }

    /** Schema adapter for operators excluded from the aggregation prototype. */
    public TranslationContext withRequirement(TranslationSchema childRequired) {
        childRequired.requireExclusive();
        return withPushedDown(childRequired.isOpen() ? null : childRequired.labels());
    }

    /** Schema adapter for operators excluded from the aggregation prototype. */
    public TranslationSchema required() {
        return requiredLabels == null ? open() : TranslationSchema.finite(requiredLabels);
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * requirements travel down the PromQL tree and intermediate results travel back up.
     */
    public IntermediateResult translate(LogicalPlan node) {
        if (node instanceof PromqlPlan promql) {
            return promql.translate(this);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** Finishes a branch for operators excluded from the aggregation prototype. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        IntermediateResult result = translateBranch(branch, stepId);
        LogicalPlan plan = result.plan();
        if (result.pendingFilter() != null) {
            plan = pushDownSrcTimestampFilter(plan, result.pendingFilter());
        }
        Alias value = emitValueDoubleCastExpression(result.value(), valueId);
        Kind kind = result.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
        return result.rebuild(new Eval(cmd.source(), plan, List.of(value)), value.toAttribute(), null, kind);
    }

    /** Translate once, preserving physical series until an explicit aggregation has consumed them. */
    private IntermediateResult translateBranch(LogicalPlan branch, NameId stepId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, requiredLabels, branchTime);
        IntermediateResult result = run.translate(branch);
        return result.kind().afterInitialAggregation ? result : run.collapseSeries(result);
    }

    /** Finishes an admitted aggregation tree and projects its logical labels to the declared command output. */
    public LogicalPlan translateFinal() {
        if (cmd.child() instanceof EsRelation relation && relation.concreteQualifiedIndices().isEmpty()) {
            return new LocalRelation(cmd.source(), cmd.output(), EmptyLocalSupplier.EMPTY);
        }
        IntermediateResult result = translateBranch(cmd.promqlPlan(), cmd.stepId());
        LogicalPlan plan = emitNullsFilter(cmd.source(), emitFinalProjection(result), cmd.valueAttribute());
        return emitByStepFilter(plan);
    }

    /**
     * Expressions compose lazily up the tree until they cross an aggregation boundary: once the plan below is
     * aggregated, the expression must materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public IntermediateResult eval(IntermediateResult t, Expression value) {
        if (t.kind().afterInitialAggregation == false) {
            return t.with(t.plan(), value);
        }
        Alias alias = new Alias(value.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("value"), value);
        return t.with(new Eval(cmd.source(), t.plan(), List.of(alias)), alias.toAttribute());
    }

    /**
     * Adapts a child's labels for its consumer. Named columns are reused; metadata is read through LabelEval or
     * rewritten with TS_UNSET. This is the integration point for label representations, so callers only declare
     * which labels they need. Published bindings are authoritative: incidental source columns cannot restore
     * labels removed by a child. Including no labels clears the grouping; excluding none removes only the metric name.
     */
    public IntermediateResult addInput(Source source, IntermediateResult input, List<Attribute> labels, boolean remove) {
        if (remove) {
            var removed = new LinkedHashSet<>(PromqlLabels.labelNames(labels));
            removed.add(LabelMatcher.NAME);
            if (input.metadata() != null) {
                Attribute metadata = input.metadata();
                var unset = new TimeSeriesUnset(
                    source,
                    metadata,
                    removed.stream().map(name -> (Expression) Literal.keyword(source, name)).toList()
                );
                // Replace the carrier so the result still has exactly one complete metadata value.
                var alias = new Alias(source, metadata.name(), unset);
                return input.withMetadata(new Eval(source, input.plan(), List.of(alias)), alias.toAttribute());
            }
            var bindings = new LinkedHashMap<>(input.labelBindings());
            removed.forEach(bindings::remove);
            return input.withLabels(input.plan(), bindings);
        }
        var aliases = new ArrayList<Alias>();
        var bindings = new LinkedHashMap<String, Attribute>();
        Attribute metadata = input.metadata();
        for (Attribute label : labels) {
            String name = PromqlLabels.labelName(label);
            Attribute column = bindings.get(name);
            if (column == null) {
                Expression field = metadata != null ? metadata : input.labelColumn(name);
                if (field == null) {
                    field = new Literal(label.source(), null, label.dataType());
                }
                Expression value = LabelEval.create(label.source(), field, name, label.dataType(), configuration());
                if (value instanceof Attribute attribute) {
                    column = attribute;
                } else {
                    var alias = new Alias(label.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("label"), value);
                    aliases.add(alias);
                    column = alias.toAttribute();
                }
                bindings.put(name, column);
            }
            if (column.dataType() != label.dataType()) {
                throw new IllegalArgumentException("Conflicting types for label [" + name + "]");
            }
        }
        LogicalPlan plan = aliases.isEmpty() ? input.plan() : new Eval(source, input.plan(), aliases);
        return input.withLabels(plan, bindings);
    }

    /**
     * Legacy schema algebra retained for the operator implementations excluded from this prototype.
     */
    public TranslationSchema regroupWithout(TranslationSchema child, List<String> keys) {
        TranslationSchema schema = intersect(child, keys);
        assert child.isOpen() == false || schema.isOpen()
            : "invariant: required [" + required() + "] must declare a metadata column excluding " + keys + ", got " + child;
        return schema.isOpen() ? project(schema, required().labels()) : schema;
    }

    /**
     * The initial aggregate: a raw table collapsed to one row per step and schema column by the innermost
     * {@link TimeSeriesAggregate}, {@code function} applied in it. Passing the table's own value collapses it as is.
     */
    public IntermediateResult collapse(IntermediateResult input, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        Alias value = new Alias(function.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("value"), function);
        return table(emitCollapse(input, value, null), input, value);
    }

    /** A selector or range function returns one sample per physical series, even when normalized labels coincide. */
    private IntermediateResult collapseSeries(IntermediateResult input) {
        Attribute tsid = find(input.plan().output(), MetadataAttribute.TSID_FIELD);
        LogicalPlan plan = input.plan();
        if (tsid == null) {
            tsid = new MetadataAttribute(plan.source(), MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false);
            Attribute series = tsid;
            plan = plan.transformUp(EsRelation.class, relation -> {
                var attributes = new ArrayList<>(relation.output());
                attributes.add(series);
                return relation.withAttributes(attributes);
            });
        }
        input = input.with(plan, input.value());
        Alias value = new Alias(input.value().source(), TemporaryNameGenerator.locallyUniqueTemporaryName("value"), input.value());
        return table(emitCollapse(input, value, tsid), input, value);
    }

    /**
     * An aggregate over a collapsed table: regrouped by its label bindings with {@code function} as the value. The regroup
     * preserves complete named label values; metadata already forms a single scalar grouping key.
     */
    public IntermediateResult regroup(IntermediateResult input, Expression function) {
        return regroup(input, input.metadata() == null, function);
    }

    private IntermediateResult regroup(IntermediateResult input, boolean dimensions, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a regroup takes a collapsed table";
        Alias value = new Alias(function.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("value"), function);
        return table(emitRegroup(input, value, dimensions && input.metadata() == null), input, value);
    }

    /** Schema adapter for operators excluded from the aggregation prototype. */
    public IntermediateResult collapse(IntermediateResult input, TranslationSchema schema, Expression function) {
        return collapse(input.with(input.plan(), schema, input.value()), function);
    }

    /** Schema adapter for operators excluded from the aggregation prototype. */
    public IntermediateResult regroup(IntermediateResult input, TranslationSchema schema, boolean packed, Expression function) {
        return regroup(input.with(input.plan(), schema, input.value()), packed, function);
    }

    private static IntermediateResult table(LogicalPlan plan, IntermediateResult input, Alias value) {
        return input.rebuild(plan, value.toAttribute(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The innermost aggregate preserves per-series evaluation before grouping by the requested result labels.
     */
    private LogicalPlan emitCollapse(IntermediateResult input, Alias value, @Nullable Attribute series) {
        Source source = cmd.promqlPlan().source();
        LogicalPlan plan = input.plan();
        boolean groupsBySeries = input.metadata() != null || input.labelBindings().isEmpty() == false;
        Expression agg = value.child();
        // Over-time functions become phase-1 references. The second phase needs Values unless an across-series
        // aggregate survives, possibly inside a scalar wrapper that the regular aggregate rules extract into Eval.
        // Ungrouped constants retain their existing literal path through TranslateTimeSeriesAggregate.
        if (groupsBySeries && hasAcrossSeriesAggregate(agg) == false) {
            value = value.replaceChild(new Values(agg.source(), agg));
        }

        // The first phase still groups raw samples by _tsid. These keys only define the second phase's label set.
        var keys = new ArrayList<>(input.keys());
        if (series != null) {
            keys.add(series);
        }

        return new TimeSeriesAggregate(
            source,
            plan,
            groupings(stepBucketAlias, keys),
            aggregates(value, input.step(), keys),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
    }

    /**
     * Regroups an already-aggregated table using its current label bindings. Named labels use {@link PackDims}
     * to prevent multi-valued dimensions from splitting rows and double-counting. Metadata is already one scalar key.
     */
    private LogicalPlan emitRegroup(IntermediateResult input, Alias value, boolean dimensions) {
        Source source = cmd.source();
        Attribute step = input.step();
        LogicalPlan plan = input.plan();
        if (hasAcrossSeriesAggregate(value.child()) == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        List<Attribute> keys = input.keys();

        if (dimensions == false || keys.isEmpty()) {
            return new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
        }
        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keys, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keys.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        return new Project(source, unpackDims, projections);
    }

    /** Stops at aggregate boundaries: functions inside an over-time aggregate do not survive as second-phase aggregates. */
    private static boolean hasAcrossSeriesAggregate(Expression expression) {
        if (expression instanceof AggregateFunction) {
            return expression instanceof TimeSeriesAggregateFunction == false;
        }
        return expression.children().stream().anyMatch(TranslationContext::hasAcrossSeriesAggregate);
    }

    /** Defines public result expressions together, then projects their attributes and the surviving labels. */
    private LogicalPlan emitFinalProjection(IntermediateResult result) {
        var aliases = new ArrayList<Alias>();
        Alias value = emitValueDoubleCastExpression(result.value(), cmd.valueId());
        aliases.add(value);
        var projected = new ArrayList<NamedExpression>();
        for (Attribute declared : cmd.output()) {
            Attribute actual;
            if (declared.id().equals(cmd.valueId())) {
                actual = value.toAttribute();
            } else if (declared.id().equals(cmd.stepId())) {
                actual = result.step();
            } else if (result.metadata() != null) {
                var rendered = new Alias(
                    declared.source(),
                    declared.name(),
                    new ToString(cmd.source(), result.metadata(), configuration()),
                    declared.id()
                );
                aliases.add(rendered);
                actual = rendered.toAttribute();
            } else {
                actual = result.labelColumn(PromqlLabels.labelName(declared));
            }
            if (actual == null) {
                throw new IllegalStateException("Missing output column [" + declared.name() + "]");
            }
            if (actual.semanticEquals(declared) && actual.name().equals(declared.name())) {
                projected.add(actual);
            } else {
                Alias alias = new Alias(declared.source(), declared.name(), actual, declared.id());
                projected.add(alias);
            }
        }
        return new Project(cmd.source(), new Eval(cmd.source(), result.plan(), aliases), projected);
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

    /** Filters raw samples before defining the evaluation timestamp; source setup happens once at the selector. */
    public LogicalPlan prepareSource(LogicalPlan plan, Selector selector, Expression matcher) {
        Expression filter = combineAndNullable(Arrays.asList(matcher, emitBySrcTimeFilter(selector)));
        if (filter != null) {
            plan = new Filter(cmd.source(), plan, filter);
        }
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(selector);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            var time = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
            plan = new Eval(cmd.source(), plan, List.of(time));
        }
        return plan;
    }

    /** Adapts deferred predicates from operators excluded from the aggregation prototype. */
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
        return new Alias(cmd.source(), TemporaryNameGenerator.locallyUniqueTemporaryName("value"), valueExpr, valueId);
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

    private static List<Expression> groupings(Expression step, List<? extends NamedExpression> keys) {
        var groupings = new ArrayList<Expression>(keys.size() + 1);
        groupings.add(step);
        groupings.addAll(keys);
        return groupings;
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, List<? extends NamedExpression> keys) {
        var aggregates = new ArrayList<NamedExpression>(keys.size() + 2);
        aggregates.add(value);
        aggregates.add(step);
        aggregates.addAll(keys);
        return aggregates;
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    // -- core --

    /**
     * A translated subtree and the state that is not yet represented by plan operators. Label bindings are
     * authoritative: either named columns or one complete metadata value. Incidental source and scratch columns
     * in the plan's output do not become labels.
     */
    public record IntermediateResult(
        LogicalPlan plan,
        /* A sample expression until initial aggregation materializes it as a column. */
        Expression value,
        Attribute step,
        /* Deferred predicate for excluded legacy operators. Aggregation selectors filter their source directly. */
        Expression pendingFilter,
        Kind kind,
        Map<String, Attribute> labelBindings,
        @Nullable Attribute metadata
    ) {
        public IntermediateResult {
            labelBindings = Collections.unmodifiableMap(new LinkedHashMap<>(labelBindings));
            labelBindings.forEach((name, attribute) -> {
                Objects.requireNonNull(name);
                Objects.requireNonNull(attribute);
            });
            if (metadata != null) {
                if (labelBindings.isEmpty() == false) {
                    throw new IllegalArgumentException("Time-series labels must be either named columns or one complete metadata value");
                }
                if (metadata.dataType() != DataType.TIMESERIES_DATA_TYPE) {
                    throw new IllegalArgumentException("Expected typed time-series metadata");
                }
            }
        }

        /** The lifecycle of a result. Constant steps and known-empty input are both source-independent. */
        public enum Kind {
            BEFORE_INITIAL_AGGREGATE(false, false),
            AFTER_INITIAL_AGGREGATE(true, false),
            CONSTANT(true, true),
            EMPTY(true, true);

            /** A local relation needs no source filtering or aggregation. */
            public final boolean constant;
            /** Value expressions above this boundary must materialize before being consumed. */
            public final boolean afterInitialAggregation;

            Kind(boolean afterInitialAggregation, boolean constant) {
                this.afterInitialAggregation = afterInitialAggregation;
                this.constant = constant;
            }
        }

        /** Rebuilds the plan and sample while retaining published labels by attribute identity. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return rebuild(plan, value, pendingFilter, kind);
        }

        /** Publishes named labels, leaving any incidental metadata column outside the result's label bindings. */
        public IntermediateResult withLabels(LogicalPlan plan, Map<String, Attribute> labels) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, null);
        }

        /** Replaces the complete metadata carrier after a scalar label transformation. */
        public IntermediateResult withMetadata(LogicalPlan plan, Attribute metadata) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind, Map.of(), Objects.requireNonNull(metadata));
        }

        private IntermediateResult rebuild(LogicalPlan plan, Expression value, Expression pendingFilter, Kind kind) {
            if (plan == this.plan) {
                return value == this.value && pendingFilter == this.pendingFilter && kind == this.kind
                    ? this
                    : new IntermediateResult(plan, value, step, pendingFilter, kind, labelBindings, metadata);
            }
            var available = new HashMap<NameId, Attribute>();
            plan.output().forEach(attribute -> available.put(attribute.id(), attribute));
            var labels = new LinkedHashMap<String, Attribute>();
            labelBindings.forEach((name, previous) -> {
                Attribute current = available.get(previous.id());
                if (current == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                labels.put(name, current);
            });
            Attribute currentMetadata = metadata == null ? null : available.get(metadata.id());
            if (metadata != null && currentMetadata == null) {
                throw new IllegalStateException("Missing time-series metadata column [" + metadata.name() + "]");
            }
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, currentMetadata);
        }

        /** Physical grouping keys in logical label declaration order. */
        private List<Attribute> keys() {
            return metadata == null ? List.copyOf(labelBindings.values()) : List.of(metadata);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }

        /** Resolves only published named labels, never incidental source columns. */
        public Attribute labelColumn(String name) {
            return labelBindings.get(name);
        }

        /** Legacy spelling retained for operators excluded from the aggregation prototype. */
        public Attribute label(String name) {
            return labelColumn(name);
        }

        /** Derived schema for operators excluded from the aggregation prototype; no schema state is retained. */
        public TranslationSchema schema() {
            return metadata == null ? TranslationSchema.finite(labelBindings.keySet()) : open();
        }

        /** Binds an excluded legacy operator's declared columns once, at the adapter boundary. */
        public IntermediateResult(
            LogicalPlan plan,
            TranslationSchema schema,
            Expression value,
            Attribute step,
            Expression pendingFilter,
            Kind kind
        ) {
            this(plan, value, step, pendingFilter, kind, legacyBindings(plan, schema), legacyMetadata(plan, schema));
        }

        /** Legacy constructor for a raw input. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step) {
            this(plan, schema, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** Legacy constructor for a raw input with a selector predicate. */
        public IntermediateResult(LogicalPlan plan, TranslationSchema schema, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, schema, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** Adapts an excluded operator's output declaration without retaining a parallel schema. */
        public IntermediateResult with(LogicalPlan plan, TranslationSchema schema, Expression value) {
            schema.requireExclusive();
            if (schema.isOpen()) {
                return metadata == null
                    ? new IntermediateResult(plan, schema, value, step, pendingFilter, kind)
                    : rebuild(plan, value, pendingFilter, kind);
            }
            var available = new HashMap<NameId, Attribute>();
            plan.output().forEach(attribute -> available.put(attribute.id(), attribute));
            var labels = new LinkedHashMap<String, Attribute>();
            for (String name : schema.labels()) {
                Attribute previous = labelBindings.get(name);
                Attribute current = previous == null ? find(plan.output(), name) : available.get(previous.id());
                if (current == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                labels.put(name, current);
            }
            return new IntermediateResult(plan, value, step, pendingFilter, kind, labels, null);
        }

        private static Map<String, Attribute> legacyBindings(LogicalPlan plan, TranslationSchema schema) {
            schema.requireExclusive();
            var labels = new LinkedHashMap<String, Attribute>();
            for (String name : schema.labels()) {
                Attribute attribute = find(plan.output(), name);
                if (attribute == null) {
                    throw new IllegalStateException("Missing column for promoted label [" + name + "]");
                }
                labels.put(name, attribute);
            }
            return labels;
        }

        private static Attribute legacyMetadata(LogicalPlan plan, TranslationSchema schema) {
            schema.requireExclusive();
            if (schema.isOpen() == false) {
                return null;
            }
            var columns = plan.output().stream().filter(a -> a.resolved() && a.dataType() == DataType.TIMESERIES_DATA_TYPE).toList();
            if (columns.size() != 1) {
                throw new IllegalStateException("Expected one time-series metadata column, found [" + columns.size() + "]");
            }
            return columns.getFirst();
        }
    }

    // -- helpers --

    /** Legacy metadata lookup retained for the operator implementations excluded from this prototype. */
    public Attribute metadata(IntermediateResult table, Set<String> skip) {
        String name = MetadataAttribute.TIMESERIES + (skip.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(skip)));
        return find(table.plan().output(), name);
    }

    /** A missing label's reference, subsequently defined as null by its consumer. */
    public static Attribute stub(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /** Legacy metadata exclusions ordered from the most detailed label set to the least detailed. */
    public static List<Set<String>> coarse(Set<Set<String>> skips) {
        return skips.stream().sorted(Comparator.comparingInt(Set::size)).toList();
    }

    /** A null-valued column under the attribute's own name and id, typed like the attribute (keyword when unresolved). */
    public static Alias emitNullExpression(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /** Finds a canonical label, preferring its backing field to a same-named bare reference. */
    public static Attribute find(List<Attribute> attributes, String label) {
        Attribute bareMatch = null;
        for (Attribute attribute : attributes) {
            if (PromqlLabels.labelName(attribute).equals(label)) {
                if (attribute.name().equals(label) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
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
}
