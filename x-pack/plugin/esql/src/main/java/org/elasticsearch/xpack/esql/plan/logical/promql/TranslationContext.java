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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
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
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.DistinctBy;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * Shared state and assembly helpers for one PromQL translation. Every result carries the labels of its series as one label
 * container ({@link IntermediateResult#labels()}): a node states what it does to labels as label container functions over
 * it, and never decides which labels become attributes of their own, which
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.ScalarizeTimeSeriesMetadataPaths} does after the
 * translation. This context also places the aggregation boundaries and assembles the command output.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, null);
    }

    /**
     * The keys of the source field holding {@code label}, see {@link PromqlLabels#path}. The root aliases of a passthrough object
     * are not source fields, so the longest field name holding the label is the one. A label no dimension holds, as a relabel
     * writes one, is a field of its own name.
     */
    public List<String> labelPath(String label) {
        String field = null;
        for (Attribute attribute : cmd.child().output()) {
            if (attribute instanceof FieldAttribute dimension
                && dimension.isDimension()
                && PromqlLabels.labelName(dimension).equals(label)
                && (field == null || dimension.fieldName().string().length() > field.length())) {
                field = dimension.fieldName().string();
            }
        }
        return field == null ? List.of(label) : PromqlLabels.path(field, analyzer.indexResolution().values());
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

    /** The labels of a series read from the source: its whole label container. */
    public static Expression sourceLabels(Source source) {
        return new TimeSeriesWithout(source, List.of());
    }

    /**
     * {@code labels} without the metric name, as most Prometheus functions and operators leave a series. Only a source storing
     * the metric name as a dimension holds one to drop.
     */
    public @Nullable Expression withoutMetricName(@Nullable Expression labels) {
        return cmd.storesMetricName() ? PromqlLabels.withoutMetricName(labels) : labels;
    }

    /** The labels of a vector without labels. */
    public static Expression emptyLabels(Source source) {
        return Literal.keyword(source, "{}");
    }

    /** Translates one merge branch with its own step bucket and evaluation time. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, branchTime);
        return run.translateIntermediate(branch, valueId);
    }

    /** Finishes the command, including independently translated union branches and the declared output projection. */
    public LogicalPlan translateFinal() {
        if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                IntermediateResult matched = op.translateMatched(this);
                return doTranslateFinal(emitFinalProjection(matched.plan(), matched.labels()), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(cmd.promqlPlan(), branches);

        if (branches.size() == 1) {
            IntermediateResult result = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            boolean constant = result.kind().constant;
            return doTranslateFinal(emitFinalProjection(result.plan(), result.labels()), constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        LogicalPlan union = doTranslateUnion(branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList());
        Attribute labels = union.output().stream().filter(a -> a.name().equals(MetadataAttribute.TIMESERIES)).findFirst().orElseThrow();
        return doTranslateFinal(emitFinalProjection(union, labels), false);
    }

    /* Shared by every `final` translation root */
    private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), plan, cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * Union combinator over independently translated tabular results. Every branch exposes its labels as {@code _timeseries};
     * {@link UnionAll} aligns the branches by name, then {@link TopNBy} keeps one row per step and series, the one of the
     * lowest branch. Series compare by their labels without the metric name, which Prometheus' {@code or} leaves out.
     */
    private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
        // Already validated against max_branch_count_per_merge by PromqlCommand.verify.
        int maxBranches = analyzer.maxBranchCountPerMerge();
        assert intermediateResults.size() <= maxBranches
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be <= the max_branch_count_per_merge limit ["
                + maxBranches
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            var ir = intermediateResults.get(i);
            Expression labels = ir.labels() == null ? emptyLabels(source) : ir.labels();
            LogicalPlan branchPlan = emitNullsFilter(source, ir.plan(), ir.valueColumn());
            var branchLabels = new Alias(source, MetadataAttribute.TIMESERIES, labels);
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branchLabels, branchTagExpression));
            // Each branch executes as an independent sub plan whose result pages cross an exchange, and the
            // consumer assumes their layout matches output() exactly. An Eval below (e.g. the value double-cast)
            // can name-shadow an existing column: the shadowed attribute leaves output() but its channel stays
            // in the page. An explicit projection pins the page layout to the branch output (see #158164).
            List<NamedExpression> pinned = List.of(
                ir.valueColumn(),
                ir.step(),
                branchLabels.toAttribute(),
                branchTagExpression.toAttribute()
            );
            branchPlans.add(new Project(source, tagged, pinned));
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
        // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        LogicalPlan plan = union;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(MetadataAttribute.TIMESERIES)) {
                Alias signature = new Alias(source, "$prom_sig", PromqlLabels.withoutMetricName(attr));
                plan = new Eval(source, plan, List.of(signature));
                groupings.add(signature.toAttribute());
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, plan, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /**
     * Translates independent query fragment into intermediate result (IR).
     * Think of IR as table
     */
    private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        IntermediateResult ir = doTranslateTryInline(translate(branch));

        var plan = ir.plan();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources - skip over HashJoin.
        Expression timeFilter = plan.anyMatch(p -> p instanceof HashJoin) ? null : emitBySrcTimeFilter(branch);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }
        ir = ir.with(plan, ir.value());

        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                ir = emitInitialAggregate(ir, ir.value());
            }
        }

        // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(ir.value(), valueId);
        plan = new Eval(cmd.source(), ir.plan(), List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
        return new IntermediateResult(plan, pinned(valueAlias), ir.labels(), ir.step(), null, kind);
    }

    /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
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
        return new IntermediateResult(plan, value, null, step, result.pendingFilter(), Kind.CONSTANT);
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
     * The initial aggregate of a raw table: one row per step and series, {@code function} applied in it. The series' source
     * labels are its identity; the changes made to the labels so far then apply to the aggregated series. Passing the
     * table's own value aggregates it as is.
     */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, Expression function) {
        Expression labels = input.labels();
        Expression series = labels == null
            ? null
            : labels.collectFirstChildren(e -> e instanceof TimeSeriesWithout).stream().findFirst().orElse(null);
        if (series == null) {
            return emitInitialAggregate(input, labels, function);
        }
        IntermediateResult collapsed = emitInitialAggregate(input, series, function);
        Expression key = collapsed.labels();
        return collapsed.withLabels(labels.transformUp(Expression.class, e -> e.semanticEquals(series) ? key : e));
    }

    /**
     * The initial aggregate of a raw table keyed by {@code labels}, the label container of each result series, with
     * {@code function} applied in it; null keys by step alone.
     */
    public IntermediateResult emitInitialAggregate(IntermediateResult input, @Nullable Expression labels, Expression function) {
        assert input.kind().afterInitialAggregation == false : "invariant: an initial aggregate takes a raw table";
        // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
        // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
        // AggregateFunction inside the Aggregate node:
        // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
        // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
        // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
        // Only keyed ones wrap: without a key (e.g. constants like vector(5)) TranslateTimeSeriesAggregate passes Literals
        // straight to phase 1.
        boolean wrapWithValues = (function instanceof AggregateFunction == false) || (function instanceof TimeSeriesAggregateFunction);
        if (labels != null && wrapWithValues) {
            function = new Values(function.source(), function);
        }
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

    /**
     * Regroups an aggregated table by {@code labels}, the label container of each result series, with {@code function}
     * applied; null groups by step alone. The container keys packed, so a label of several values keys as a whole, and is
     * unpacked above the aggregate under the key's own id.
     */
    public IntermediateResult emitAggregate(IntermediateResult input, @Nullable Expression labels, Expression function) {
        assert input.kind().afterInitialAggregation : "invariant: an aggregate takes an aggregated table";
        if (function instanceof AggregateFunction == false) {
            function = new Values(function.source(), function);
        }
        Source source = cmd.source();
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        // Named apart from the input's label container, which the function may read and a same-named key would shadow.
        Alias key = labels == null
            ? null
            : new Alias(labels.source(), TemporaryNameGenerator.locallyUniqueTemporaryName(MetadataAttribute.TIMESERIES), labels);
        Attribute step = input.step();
        if (key == null) {
            LogicalPlan plan = new Aggregate(source, input.plan(), groupings(step, null), aggregates(value, step, null));
            return new IntermediateResult(plan, value.toAttribute(), null, step, input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
        }
        PackDims pack = new PackDims(
            source,
            new Eval(source, input.plan(), List.of(key)),
            List.of(key.toAttribute()),
            PackDims.newPackedAttribute(source)
        );
        Alias packed = PackDims.newPackedGrouping(source, pack.packed());
        Aggregate aggregate = new Aggregate(source, pack, groupings(step, packed), aggregates(value, step, packed));
        Attribute unpacked = new ReferenceAttribute(key.source(), null, key.name(), DataType.KEYWORD, Nullability.TRUE, key.id(), false);
        LogicalPlan plan = new Project(
            source,
            new UnpackDims(source, aggregate, packed.toAttribute(), List.of(unpacked)),
            List.of(value.toAttribute(), step, unpacked)
        );
        return new IntermediateResult(plan, value.toAttribute(), unpacked, step, input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
    }

    private static @Nullable Alias labelsKey(@Nullable Expression labels) {
        return labels == null ? null : new Alias(labels.source(), MetadataAttribute.TIMESERIES, labels);
    }

    /**
     * {@code plan} failing the query where two of its series share a label set at a step, as Prometheus fails a result
     * vector holding one label set twice. {@code labels} is the label container of each series.
     */
    public static LogicalPlan uniqueSeries(Source source, LogicalPlan plan, Attribute step, Attribute labels) {
        // Binary, not keyword: a sort the optimizer pushes below the result's projection carries it along. Named apart from
        // a join key packed below.
        Attribute packed = PackDims.newPackedAttribute(
            source,
            TemporaryNameGenerator.locallyUniqueTemporaryName(PackDims.PACKED_FIELD_NAME)
        );
        return new DistinctBy(source, new PackDims(source, plan, List.of(step, labels), packed), packed, true);
    }

    /**
     * The double column {@code alias} defines, under its id even while its value is unresolved: an attribute taken from an
     * unresolved alias is never bound again once a join or a projection holds it.
     */
    public static Attribute pinned(Alias alias) {
        if (alias.resolved()) {
            return alias.toAttribute();
        }
        return new ReferenceAttribute(alias.source(), null, alias.name(), DataType.DOUBLE, Nullability.TRUE, alias.id(), true);
    }

    /**
     * Projects the command output. Value and step bind to the finished table's columns by name. Every label the command
     * declares reads the result's label container {@code labels}, under its declared name, id and type, and the declared
     * {@code _timeseries} is the container without them.
     */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, @Nullable Expression labels) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(attr.name(), attr);
        }
        var projected = new ArrayList<NamedExpression>(cmd.output());
        var evals = new ArrayList<Alias>();
        List<Attribute> declared = cmd.promqlPlan().output();
        // A column exposed only to order the result is read from the container but stays in _timeseries too.
        Set<NameId> orderingOnly = new HashSet<>();
        cmd.promqlPlan().forEachDown(LogicalPlan.class, p -> {
            if (p instanceof ResultOrderingFunction ordering) {
                ordering.orderingOnlyColumns().forEach(column -> orderingOnly.add(column.id()));
            }
        });
        List<String> named = PromqlLabels.labelNames(
            declared.stream()
                .filter(a -> MetadataAttribute.isTimeSeriesAttributeName(a.name()) == false && orderingOnly.contains(a.id()) == false)
                .toList()
        );
        Source source = cmd.promqlPlan().source();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i).toAttribute();
            // the value and step lead the declared output, then the result's labels
            if (i >= 2) {
                Attribute label = declared.get(i - 2);
                Expression value;
                if (MetadataAttribute.isTimeSeriesAttributeName(label.name())) {
                    value = labels == null ? emptyLabels(source) : PromqlLabels.without(labels, named);
                } else {
                    value = labels == null
                        ? new Literal(source, null, attr.dataType())
                        : asType(PromqlLabels.read(labels, PromqlLabels.labelName(label)), attr.dataType());
                }
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
                    // column by name once the child resolves. Keep the Alias in the Project so ResolveRefs rebinds it after
                    // the union output is refreshed.
                    projected.set(i, alias);
                }
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
    }

    /** A label's text as the declared label's type: a label read from the container is text, a dimension may not be. */
    private Expression asType(Expression text, DataType type) {
        if (type == DataType.NULL || DataType.isString(type)) {
            return text;
        }
        Expression cast = EsqlDataTypeConverter.castFromKeyword(type, text, configuration());
        return cast == null ? text : cast;
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

    /** Adds an Eval on top of the source relation materializing the evaluation timestamp (@timestamp + offset). */
    private LogicalPlan pushDownEvaluationTimestampFilter(LogicalPlan plan, LogicalPlan branch) {
        if (time instanceof ReferenceAttribute ref && cmd.timestampColumnName().equals(ref.name())) {
            Expression base = cmd.timestamp();
            if (base.dataType() == DataType.DATE_NANOS) {
                base = new ToDatetime(base.source(), base, configuration());
            }
            var offset = cmd.collectFirstOffsetForBranch(branch);
            var shifted = offset.isZero() ? base : new Add(cmd.source(), base, Literal.timeDuration(cmd.source(), offset), configuration());
            var time = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
            return plan.transformUp(node -> node == cmd.child(), node -> new Eval(cmd.source(), node, List.of(time)));
        }
        return plan;
    }

    /** Pushes the label filter down to the EsRelation, combining with an existing relation filter. */
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

    private static List<Expression> groupings(Expression step, @Nullable NamedExpression key) {
        return key == null ? List.of(step) : List.of(step, key);
    }

    private static List<NamedExpression> aggregates(NamedExpression value, Attribute step, @Nullable NamedExpression key) {
        return key == null ? List.of(value, step) : List.of(value, step, key.toAttribute());
    }

    /**
     * Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. An {@code or}
     * with matching modifiers matches on its own labels, so it is a branch of its own.
     */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION && setOp.match() == VectorMatch.NONE) {
            flattenUnion(setOp.left(), branches);
            flattenUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    /** PromQL drops series with missing data: filter out rows whose value is null (null label columns are valid). */
    private static LogicalPlan emitNullsFilter(Source source, LogicalPlan plan, Attribute value) {
        return new Filter(source, plan, new IsNotNull(value.source(), value));
    }

    // -- core --

    /**
     * The single value flowing through the compiler: a table - an ESQL plan together with its value, step and the label
     * container of its series. Every AST node translates to one and the stitching operations (joins, unions, aggregates, the
     * command coda) compose them. Mid-descent the value and the labels are (possibly not yet materialized) expressions
     * parents compose into larger ones; a finished table's value is a defined column ({@link #valueColumn()}).
     */
    public record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* This node's numeric value: an expression mid-descent, a defined column once aggregated. */
        Expression value,
        /* The label container of each series, composed like the value until an aggregate keys by it; null for no labels. */
        @Nullable Expression labels,
        /* The step column. */
        Attribute step,
        /* Label matcher predicate; flows up until pushed to the relation or folded into an aggregate filter. */
        Expression pendingFilter,
        /* The translator tracks what it built instead of inspecting the plan. */
        Kind kind
    ) {
        /** The lifecycle of an intermediate result. A constant is always a finished (aggregation-free) local relation. */
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
        public IntermediateResult(LogicalPlan plan, Expression value, @Nullable Expression labels, Attribute step) {
            this(plan, value, labels, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(
            LogicalPlan plan,
            Expression value,
            @Nullable Expression labels,
            Attribute step,
            Expression selectorFilter
        ) {
            this(plan, value, labels, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return new IntermediateResult(plan, value, labels, step, pendingFilter, kind);
        }

        /** This table with the labels of its series changed: {@code labels} are label container functions over the current ones. */
        public IntermediateResult withLabels(@Nullable Expression labels) {
            return new IntermediateResult(plan, value, labels, step, pendingFilter, kind);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }
    }

    // -- helpers --

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

}
