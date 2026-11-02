/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;

/**
 * Shared state and assembly helpers for one PromQL translation. Translators read the labels of their input through
 * references ({@link UnresolvedPromqlAttribute}) and never look at which columns their input carries:
 * {@code BindPromqlLabelReferences} binds the references right after translation. This context also preserves aggregation
 * boundaries and assembles the command output.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {

    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, null);
    }

    /**
     * Dispatches to the node that owns the translation. The source relation is the leaf of the produced ES|QL subtree;
     * intermediate results travel back up.
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

    /** Translates an independent branch with its own value and step identities. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, branchTime);
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
        var run = new TranslationContext(cmd, analyzer, step, branchTime);
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
        Attribute step = (Attribute) rekey.apply(ir.step());
        Expression filter = ir.pendingFilter() == null ? null : ir.pendingFilter().transformDown(Expression.class, rekey);
        return new IntermediateResult(plan, value, step, filter, ir.kind());
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
     * The initial aggregate: a raw table collapsed to one row per step by the innermost {@link TimeSeriesAggregate},
     * {@code function} applied in it. Passing the table's own value collapses it as is; the result keys by what its
     * consumers read.
     */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, Expression function) {
        return emitInitialAggregate(input, List.of(), function);
    }

    /** The initial aggregate keyed by {@code keys}, references to the labels it groups by. */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, List<Attribute> keys, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: a collapse takes a raw table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        LogicalPlan plan = new TimeSeriesAggregate(
            cmd.promqlPlan().source(),
            input.plan(),
            groupings(stepBucketAlias, keys),
            aggregates(value, input.step(), keys),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
        return new IntermediateResult(plan, value.toAttribute(), input.step(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * Regroups a collapsed table by {@code keys}, references to the labels it groups by. The emitAggregate packs its dimensions first
     * when it keys by a {@code ts_metadata} view or the operator asks for it ({@code packDimensions}).
     */
    public IntermediateResult emitAggregate(IntermediateResult input, List<Attribute> keys, boolean packDimensions, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: a emitAggregate takes a collapsed table";
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        boolean pack = packDimensions || keys.stream().anyMatch(key -> key instanceof UnresolvedPromqlAttribute ref && ref.isMetadata());
        LogicalPlan plan;
        Source source = cmd.source();
        Attribute step = input.step();
        LogicalPlan plan1 = input.plan();
        if (pack == false || keys.isEmpty()) {
            plan = new Aggregate(source, plan1, groupings(step, keys), aggregates(value, step, keys));
        } else {// TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this emitAggregate re-packs them.
            Attribute packedAttribute = PackDims.newPackedAttribute(source);
            PackDims packDims = new PackDims(source, plan1, keys, packedAttribute);
            Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
            Aggregate agg = new Aggregate(
                source,
                packDims,
                groupings(step, List.of(packedGrouping)),
                aggregates(value, step, List.of(packedGrouping.toAttribute()))
            );// The unpacked dimensions are the keys; BindPromqlLabelReferences lays them out once they are bound.
            UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), keys);
            List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
            projections.addAll(keys);
            plan = new Project(source, unpackDims, projections);
        }
        return new IntermediateResult(plan, value.toAttribute(), input.step(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
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
            start = cmd.start().value() != null ? cmd.start() : Literal.dateTime(cmd.source(), PromqlCommand.EPOCH_MIN);
            end = cmd.end().value() != null ? cmd.end() : Literal.dateTime(cmd.source(), PromqlCommand.EPOCH_MAX);
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
            // so nested parents (label_replace, aggregations, …) see a filtered vector. Vector-matched comparisons
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
        return new IntermediateResult(plan, pinnedAttribute(valueAlias), ir.step(), null, kind);
    }

    /** Same id as alias while unresolved — Projects must not freeze a toAttribute snapshot. */
    public static Attribute pinnedAttribute(Alias alias) {
        if (alias.resolved()) {
            return alias.toAttribute();
        }
        return new ReferenceAttribute(alias.source(), null, alias.name(), DataType.DOUBLE, Nullability.TRUE, alias.id(), true);
    }

    /** Folds a value depending only on the evaluation step into a local relation. */
    private IntermediateResult doTranslateTryInline(IntermediateResult result) {
        Attribute stepAttr = cmd.stepAttribute();
        if (result.kind().constant
            || cmd.start().value() == null
            || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new IntermediateResult(plan, value, step, result.pendingFilter(), Kind.CONSTANT);
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

        /** A raw input whose value may still contain per-series aggregate expressions. */
        public IntermediateResult(LogicalPlan plan, Expression value, Attribute step) {
            this(plan, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(LogicalPlan plan, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This result rebuilt around a new plan and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }
    }
}
