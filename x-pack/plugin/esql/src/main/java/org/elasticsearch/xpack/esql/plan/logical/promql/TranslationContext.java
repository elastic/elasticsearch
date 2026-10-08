/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.time.DateUtils;
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
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
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
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext.IntermediateResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate.Grouping.WITHOUT;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels.PROMETHEUS_LABELS_PREFIX;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintExclude;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintIntersect;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintProject;
import static org.elasticsearch.xpack.esql.plan.logical.promql.TranslationSchema.newConstraintUnset;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * Shared state and assembly helpers for one PromQL translation. Nodes own their lowering, while this context
 * carries the top-down label requirements, aggregation boundaries, and command finalization.
 */
public record TranslationContext(
    PromqlCommand cmd,
    AnalyzerContext analyzer,
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    Alias stepBucketAlias,
    /* The label columns the translated subtree MUST expose; strictly top-down. */
    TranslationSchema required,
    /* The current evaluation time (default: @timestamp). */
    Expression time
) {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** The command exposes every label of the result series: the whole metadata. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        this(cmd, analyzer, null, newConstraintUnset(), null);
    }

    /** Translate an independent child without changing the enclosing branch's timing or aggregation state. */
    public TranslationContext withRequired(TranslationSchema childRequired) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, childRequired, time);
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

    /**
     * Whether the translation edits one {@code _timeseries} per node with {@link TimeSeriesUnset}. Every translate function where
     * a series' identity changes ({@code without}, the {@code le} of a classic histogram) branches on this explicitly. Vector
     * matching keys on named labels only (see {@code VectorBinaryOperator#translateJoin}), so it never edits one:
     * <ul>
     *     <li>Every node the query reaches - the minimum transport version across the local cluster and every remote cluster -
     *     can run {@link TimeSeriesUnset}: each node carries its series' one {@code _timeseries}, loaded complete, and unsets the
     *     labels its identity drops right where it drops them.</li>
     *     <li>Any node is older, an older cluster or a mixed one mid-upgrade: the translation asks the source for one
     *     {@code _timeseries} per exclusion set, exactly the plan such a cluster has always run. The coordinator decides once for
     *     the whole command, and {@link TimeSeriesUnset} refuses to serialize to an older node.</li>
     *     <li>A label the command drops may be an alias the plan can't resolve ({@link #resolvesStoredNames}): the command also
     *     asks the source for one {@code _timeseries} per exclusion set, as the source resolves an alias per shard.</li>
     * </ul>
     */
    public boolean supportsTimeSeriesUnset() {
        return supportsTimeSeriesUnset(analyzer.minimumVersion()) && droppedLabels().stream().allMatch(this::resolvesStoredNames);
    }

    static boolean supportsTimeSeriesUnset(TransportVersion minimumVersion) {
        return minimumVersion.supports(TimeSeriesUnset.ESQL_TIMESERIES_METADATA_UNSET);
    }

    /** The labels the command may unset from a series' identity: those of {@code without} and a histogram's {@code le}. */
    private Set<String> droppedLabels() {
        var labels = new TreeSet<String>();
        cmd.promqlPlan().forEachDown(plan -> {
            switch (plan) {
                case AcrossSeriesAggregate aggregate when aggregate.grouping() == WITHOUT -> labels.addAll(
                    asPromotedLabels(aggregate.groupings())
                );
                case HistogramFunctionCall histogram -> labels.add(HistogramFunctionCall.LE_LABEL);
                default -> {
                }
            }
        });
        return labels;
    }

    /** {@link TimeSeriesUnset} of {@code labels}, each named by the field names the source relation stores it under. */
    private Expression timeSeriesUnset(Expression timeseries, Collection<String> labels) {
        var names = new TreeSet<String>();
        labels.forEach(label -> names.addAll(storedNames(label)));
        List<Expression> dimensions = names.stream().<Expression>map(name -> Literal.keyword(cmd.source(), name)).toList();
        return new TimeSeriesUnset(cmd.source(), timeseries, dimensions);
    }

    /** The field names a label is stored under: the label itself and the dimension fields it names ({@code labels.pod}). */
    private Set<String> storedNames(String label) {
        var names = new TreeSet<String>(List.of(label));
        for (FieldAttribute dimension : dimensions()) {
            if (asPromotedLabel(dimension).equals(label)) {
                names.add(dimension.fieldName().string());
            }
        }
        return names;
    }

    /**
     * Whether {@link #storedNames} are all the fields a label may name. A field of the label's own name may be an alias of
     * another dimension, a passthrough alias such as OTel {@code cpu} for {@code attributes.cpu}: the source resolves it per
     * shard, the plan can't. So a label that names a field while another dimension ends in it may name more than its stored
     * names, and is unset at the source instead. An alias to an unrelated name ({@code pod} for {@code kubernetes.pod.name})
     * looks like any field to the plan.
     */
    private boolean resolvesStoredNames(String label) {
        List<FieldAttribute> dimensions = dimensions();
        if (dimensions.stream().noneMatch(dimension -> dimension.fieldName().string().equals(label))) {
            return true;
        }
        Set<String> names = storedNames(label);
        return dimensions.stream()
            .map(dimension -> dimension.fieldName().string())
            .noneMatch(fieldName -> fieldName.endsWith("." + label) && names.contains(fieldName) == false);
    }

    /** The source relation's dimension fields. */
    private List<FieldAttribute> dimensions() {
        return cmd.child()
            .output()
            .stream()
            .filter(attribute -> attribute instanceof FieldAttribute field && field.isDimension())
            .filter(attribute -> attribute instanceof TimeSeriesMetadataAttribute == false)
            .map(FieldAttribute.class::cast)
            .toList();
    }

    /** Translates one merge branch with its own step bucket and evaluation time. */
    public IntermediateResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, required, branchTime);
        return run.translateIntermediate(branch, valueId);
    }

    /** Finishes the command, including independently translated union branches and the declared output projection. */
    public LogicalPlan translateFinal() {
        if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                return doTranslateFinal(op.translateJoin(this).plan(), false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(cmd.promqlPlan(), branches);

        if (branches.size() == 1) {
            IntermediateResult intermediateResult = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            Attribute declared = find(cmd.output(), asMetadataLabel());
            LogicalPlan plan = emitTimeSeriesAlias(intermediateResult, declared != null ? declared.id() : new NameId());
            return doTranslateFinal(plan, intermediateResult.kind().constant);
        }
        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        var intermediateResultPlan = doTranslateUnion(
            branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return doTranslateFinal(intermediateResultPlan, false);
    }

    /* Shared by every `final` translation root */
    private LogicalPlan doTranslateFinal(LogicalPlan plan, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan), cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * A finished table exposes its {@code _timeseries} column under the canonical {@code _timeseries} name. The columns
     * travel under their derived names so nodes can tell them apart; the one surviving at a root is whatever the enclosing
     * regroups left, and the command declares it as {@code _timeseries}.
     */
    private LogicalPlan emitTimeSeriesAlias(IntermediateResult table, NameId id) {
        Attribute finest = finestTimeSeries(table.plan());
        if (finest == null || MetadataAttribute.TIMESERIES.equals(finest.name())) {
            return table.plan();
        }
        return new Eval(cmd.source(), table.plan(), List.of(new Alias(cmd.source(), MetadataAttribute.TIMESERIES, finest, id)));
    }

    /** Whether the plan edits a {@code _timeseries} with {@link TimeSeriesUnset}. */
    private static boolean unsets(LogicalPlan plan) {
        return plan.anyMatch(
            node -> node instanceof Eval eval && eval.fields().stream().anyMatch(field -> field.child() instanceof TimeSeriesUnset)
        );
    }

    /**
     * Union combinator over independently translated tabular results.
     * {@link UnionAll} aligns columns by name and null-fills the missing ones, then
     * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
     */
    private LogicalPlan doTranslateUnion(List<IntermediateResult> intermediateResults) {
        // Already validated against MergePlan.MAX_BRANCHES by PromqlCommand.verify
        assert MergePlan.exceedsMaxBranches(intermediateResults.size()) == false
            : "invariant: merge branch count ["
                + intermediateResults.size()
                + "] must be less of equal MergePlan.MAX_BRANCHES ["
                + MergePlan.MAX_BRANCHES
                + "]";

        var source = cmd.source();
        // Otherwise every branch's _timeseries is as the source loads it, and they compare as they always have.
        boolean canonicalize = supportsTimeSeriesUnset() && intermediateResults.stream().anyMatch(ir -> unsets(ir.plan()));
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            var ir = intermediateResults.get(i);
            if (canonicalize) {
                // The dedup below compares _timeseries across branches, one as loaded and another as edited: unsetting
                // nothing rewrites each in its canonical form, so the same labels compare equal.
                ir = ir.withUnsetLabels(this, List.of());
            }
            LogicalPlan branchPlan = emitNullsFilter(source, emitTimeSeriesAlias(ir, new NameId()), ir.valueColumn());
            var branchTagExpression = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            LogicalPlan tagged = new Eval(source, branchPlan, List.of(branchTagExpression));
            // Each branch executes as an independent sub plan whose result pages cross an exchange, and the
            // consumer assumes their layout matches output() exactly. An Eval below (e.g. the value double-cast)
            // can name-shadow an existing column: the shadowed attribute leaves output() but its channel stays
            // in the page. An explicit projection pins the page layout to the branch output (see #158164).
            branchPlans.add(new Project(source, tagged, tagged.output()));
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll output,
        // so the groupings below remain valid. The command coda projects the synthetic branch tag away.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        var union = new UnionAll(source, branchPlans, unionOutput);

        // Left-preferring dedup: group by every column except the value and the branch tag, keep the lowest branch.
        var groupings = new ArrayList<Expression>();
        Attribute branchAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }
        var order = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        return new TopNBy(source, union, List.of(order), new Literal(source, 1, DataType.INTEGER), groupings);
    }

    /**
     * Translates independent query fragment into intermediate result (IR).
     * Think of IR as table
     */
    private IntermediateResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        IntermediateResult ir = doTranslateTryInline(translate(branch));

        var plan = ir.plan();
        var value = ir.value();
        // A vector match self-filters each operand's own source with that operand's own @timestamp; a combined outer
        // source-time filter would push one operand's @timestamp across both sources - skip over InnerJoin.
        Expression timeFilter = plan.anyMatch(p -> p instanceof InnerJoin) ? null : emitBySrcTimeFilter(branch);
        var filter = combineAndNullable(Arrays.asList(ir.pendingFilter(), timeFilter));
        if (filter != null) {
            plan = pushDownSrcTimestampFilter(plan, filter);
        }

        if (ir.kind().constant == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: with metric references without last_over_time, a plain Aggregate could do (#141501 discussion).
            if (ir.kind().afterInitialAggregation == false) {
                IntermediateResult raw = ir.with(plan, value);
                IntermediateResult collapsed = raw.withCollapse(this, newConstraintForCollapse(raw, required), value);
                plan = collapsed.plan();
                value = collapsed.value();
            }
            if (branch instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
                VectorMatch match = comparison.match();
                if ((match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) == false) {
                    // Filter-mode comparison (metric > x): keep the left operand's value, filter rows by the comparison.
                    // A vector-matched comparison already applied its filter inside the join translation.
                    ToDouble right = new ToDouble(comparison.right().source(), ((LiteralSelector) comparison.right()).literal());
                    var condition = comparison.op().asFunction().create(comparison.source(), value, right, configuration());
                    plan = new Filter(comparison.source(), plan, condition);
                }
            }
        }

        // The value column definition: the translateIntermediate's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
        return new IntermediateResult(plan, valueAlias.toAttribute(), ir.step(), null, kind);
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
        // The folded relation carries no label columns at all.
        return new IntermediateResult(plan, value, step, result.pendingFilter(), Kind.CONSTANT);
    }

    /**
     * The requirement a {@code without} regroup groups by: the child's labels - a raw child's {@link #newConstraintForCollapse}, or
     * what an aggregated child delivers - transposed below the dropped labels. Under a {@code _timeseries} column the labels are derived
     * columns, so only those the enclosing translation asks for are carried; a child of promoted labels only keeps every remaining label
     * because they are its label set.
     */
    public TranslationSchema newConstraintForRegroupWithout(TranslationSchema childLabels, List<String> keys) {
        TranslationSchema regrouped = newConstraintIntersect(childLabels, keys);
        assert childLabels.hasMetadata() == false || regrouped.hasMetadata()
            : "invariant: required [" + required + "] must declare a _timeseries column excluding " + keys + ", got " + childLabels;
        return regrouped.hasMetadata() ? newConstraintProject(regrouped, required.labels()) : regrouped;
    }

    /**
     * {@link #newConstraintForRegroupWithout} when the child's one {@code _timeseries} already has the keys unset
     * ({@link IntermediateResult#withUnsetLabels}): the child's labels without the keys, its {@code _timeseries} kept. As
     * there, under a {@code _timeseries} the labels are derived columns, so only those the enclosing translation asks for
     * are carried.
     */
    public TranslationSchema newConstraintForRegroupUnset(TranslationSchema childLabels, List<String> keys) {
        assert supportsTimeSeriesUnset() : "invariant: TimeSeriesUnset only once every node the query reaches can run it";
        TranslationSchema regrouped = newConstraintExclude(childLabels, keys);
        return regrouped.hasMetadata() ? newConstraintProject(regrouped, required.labels()) : regrouped;
    }

    /**
     * The requirement a raw (not yet aggregated) table collapses by: the labels required of it that the source relation
     * stores as dimensions, every {@code _timeseries} column kept. A required label the relation does not store as a
     * dimension is null-filled by the node that binds it above ({@link IntermediateResult}). A value referencing nothing
     * but the step (a scalar chain like {@code 1 + 2}) carries no series grain: collapsing it by the requirement would
     * multiply rows per series, so it collapses bare.
     */
    public TranslationSchema newConstraintForCollapse(IntermediateResult raw, TranslationSchema requirement) {
        assert raw.kind().afterInitialAggregation == false : "invariant: only a raw table has a raw requirement";
        if (raw.value().references().stream().allMatch(ref -> ref.semanticEquals(cmd.stepAttribute()))) {
            return TranslationSchema.EMPTY;
        }
        List<Attribute> dimensions = cmd.child()
            .output()
            .stream()
            .filter(attribute -> attribute instanceof FieldAttribute field && field.isDimension())
            .filter(attribute -> attribute instanceof TimeSeriesMetadataAttribute == false)
            .toList();
        return newConstraintProject(requirement, asPromotedLabels(dimensions));
    }

    /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
    private LogicalPlan emitFinalProjection(LogicalPlan plan) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name.
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(asPromotedLabel(attr), attr);
        }
        var projected = new ArrayList<>(cmd.output());
        var evals = new ArrayList<Alias>();
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i);
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                evals.add(alias);
                projected.set(i, alias.toAttribute());
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
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

    /** Flattens a left-associative top-level {@code or} chain into branches; branch 0 has the highest precedence. */
    private static void flattenUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
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
     * The single value flowing through the compiler: a table - an ESQL plan together with the pieces parents
     * cannot read off the plan itself. Label requirements travel down as {@link TranslationSchema}s; every
     * label column a parent needs it reads off the child plan's output, under its canonical or derived name.
     * Value and step are the two columns every table has. Every AST node translates to one and the stitching
     * operations (joins, unions, regroups, the command coda) compose them. Mid-descent the value is a (possibly
     * not yet materialized) expression parents compose into larger expressions; a finished table's value is a
     * defined column ({@link #valueColumn()}).
     * <p>
     * Labels use a dual representation like ClickHouse: promoted labels carried directly, each its own column, plus the
     * time-series metadata - every remaining label, encoded in {@code _timeseries} columns. The metadata may overlap
     * the promoted names.
     * <p>
     * Null-fill contract: a table carries a label column only where it can source the label. The node that binds labels
     * into its result null-fills there each one its input lacks: a regroup its grouping keys, a reduction its partitions,
     * a join its match key and result labels. A raw table binds no label it lacks - its collapse groups by the required
     * labels the relation stores ({@link TranslationContext#newConstraintForCollapse}) - so a required label absent from
     * the relation is null-filled by the node that binds it above.
     */
    public record IntermediateResult(
        /* Output ESQL plan: the source relation (cmd.child()) with this node's operators stacked on top. */
        LogicalPlan plan,
        /* This node's numeric value: an expression mid-descent, a defined column once aggregated. */
        Expression value,
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
        public IntermediateResult(LogicalPlan plan, Expression value, Attribute step) {
            this(plan, value, step, null, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** A raw input carrying a selector predicate until source filtering or aggregate assembly consumes it. */
        public IntermediateResult(LogicalPlan plan, Expression value, Attribute step, Expression selectorFilter) {
            this(plan, value, step, selectorFilter, Kind.BEFORE_INITIAL_AGGREGATE);
        }

        /** This table rebuilt around a new plan and value, keeping its other properties. */
        public IntermediateResult with(LogicalPlan plan, Expression value) {
            return new IntermediateResult(plan, value, step, pendingFilter, kind);
        }

        /**
         * This table with {@code labels} unset from its {@code _timeseries}, redefined under the same name; a table carrying
         * no {@code _timeseries} (promoted labels only) is returned as is. Only when
         * {@link TranslationContext#supportsTimeSeriesUnset}.
         */
        public IntermediateResult withUnsetLabels(TranslationContext context, Collection<String> labels) {
            assert context.supportsTimeSeriesUnset() : "invariant: TimeSeriesUnset only once every node the query reaches can run it";
            Attribute timeseries = deliveredSkips(plan).contains(Set.of()) ? find(plan.output(), asMetadataLabel()) : null;
            if (timeseries == null) {
                return this;
            }
            Alias unset = new Alias(context.cmd().source(), asMetadataLabel(), context.timeSeriesUnset(timeseries, labels));
            return with(new Eval(context.cmd().source(), plan, List.of(unset)), value);
        }

        /**
         * This table with {@code value} as its value. Expressions compose lazily up the tree until they cross an aggregation
         * boundary: once the plan below is aggregated, the expression materializes as the value column (an Eval) so parents
         * reference it by attribute.
         */
        public IntermediateResult withEval(TranslationContext context, Expression value) {
            if (kind.afterInitialAggregation == false) {
                return with(plan, value);
            }
            Alias alias = new Alias(value.source(), context.cmd().valueColumnName(), value);
            return with(new Eval(context.cmd().source(), plan, List.of(alias)), alias.toAttribute());
        }

        /**
         * This raw table collapsed to one row per step and required column by the innermost {@link TimeSeriesAggregate},
         * {@code function} applied in it: the initial aggregate. Passing the table's own value collapses it as is. The
         * requirement is the table's {@link TranslationContext#newConstraintForCollapse}, or the label set a node declares
         * itself (a {@code by}).
         */
        public IntermediateResult withCollapse(TranslationContext context, TranslationSchema requirement, Expression function) {
            assert kind.afterInitialAggregation == false : "invariant: a collapse takes a raw table";
            Alias value = new Alias(function.source(), context.cmd().valueColumnName(), function);
            return withLegacyCollapse(context, requirement, value);
        }

        /**
         * This collapsed table regrouped by {@code requirement}, {@code function} as the value: an aggregate over an aggregate.
         * It packs its dimensions first when the requirement has metadata or the operator asks for it ({@code packed}). The
         * requirement is the one the table was translated under, transposed below any dropped labels.
         */
        public IntermediateResult withRegroup(
            TranslationContext context,
            TranslationSchema requirement,
            boolean packed,
            Expression function
        ) {
            assert kind.afterInitialAggregation : "invariant: a regroup takes a collapsed table";
            Alias value = new Alias(function.source(), context.cmd().valueColumnName(), function);
            return withLegacyRegroup(context, requirement, value, requirement.hasMetadata() || packed);
        }

        /**
         * The innermost aggregate owns the physical {@code _timeseries} grouping and materializes every {@code _timeseries}
         * column in the requirement over that column's own skip set.
         */
        private IntermediateResult withLegacyCollapse(TranslationContext context, TranslationSchema requirement, Alias value) {
            Source source = context.cmd().promqlPlan().source();
            LogicalPlan plan = this.plan;
            boolean groupsBySeries = requirement.hasMetadata() || requirement.labels().isEmpty() == false;
            Expression agg = value.child();
            // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner TimeSeriesAggregateFunctions
            // (e.g. LastOverTime) with references to phase-1 results; the phase-2 expression must remain a valid
            // AggregateFunction inside the Aggregate node:
            // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
            // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
            // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
            // Guarded by groupsBySeries because without any series grouping (e.g. constants like vector(5))
            // TranslateTimeSeriesAggregate passes Literals straight to phase 1.
            boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
            if (groupsBySeries && wrapWithValues) {
                value = value.replaceChild(new Values(agg.source(), agg));
            }

            // Every metadata column is materialized under its derived name, finest first, and every promoted label the relation
            // has is a key too. Every column is functionally dependent on the finest one, so grouping by all of them
            // preserves per-series granularity while making the full requirement available to the surrounding query.
            var groupKeys = new ArrayList<NamedExpression>();
            var outKeys = new ArrayList<NamedExpression>();
            for (Set<String> skip : finestFirst(requirement.skips())) {
                List<Expression> excluded = skip.stream().<Expression>map(label -> {
                    Attribute resolved = find(plan.output(), label);
                    return resolved != null ? resolved : mapToRef(label);
                }).toList();
                Alias timeseries = new Alias(source, asMetadataLabel(skip), new TimeSeriesWithout(source, excluded));
                groupKeys.add(timeseries);
                outKeys.add(timeseries.toAttribute());
            }
            for (String label : requirement.labels()) {
                Attribute carrier = find(plan.output(), label);
                if (carrier != null) {
                    groupKeys.add(carrier);
                    outKeys.add(carrier);
                }
            }

            var collapsed = new TimeSeriesAggregate(
                source,
                plan,
                groupings(context.stepBucketAlias(), groupKeys),
                aggregates(value, step, outKeys),
                null,
                context.time(),
                TimeSeriesAggregate.Origin.PROMQL_COMMAND
            );
            return new IntermediateResult(collapsed, value.toAttribute(), step, pendingFilter, Kind.AFTER_INITIAL_AGGREGATE);
        }

        /**
         * Regroups an already-aggregated table. Every regroup first resolves its required columns and null-fills missing
         * grouping columns. A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
         * dimensions from splitting rows and double-counting, then unpacks them afterwards.
         */
        private IntermediateResult withLegacyRegroup(
            TranslationContext context,
            TranslationSchema requirement,
            Alias value,
            boolean requiresPacking
        ) {
            Source source = context.cmd().source();
            LogicalPlan plan = this.plan;
            if (value.child() instanceof AggregateFunction == false) {
                value = value.replaceChild(new Values(value.child().source(), value.child()));
            }
            List<Attribute> available = plan.output();

            var nulls = new ArrayList<Alias>();
            var keys = new ArrayList<Attribute>();
            for (Set<String> skip : finestFirst(requirement.skips())) {
                Attribute carrier = find(available, asMetadataLabel(skip));
                assert carrier != null : "invariant: _timeseries column " + skip + " must be carried by the child";
                keys.add(carrier);
            }
            for (String label : requirement.labels()) {
                Attribute carrier = find(available, label);
                if (carrier == null) {
                    // a declared label the child lacks is absent from every series: grouped under null, like Prometheus
                    nulls.add(emitNullExpression(mapToRef(label)));
                    carrier = nulls.getLast().toAttribute();
                }
                keys.add(carrier);
            }

            if (nulls.isEmpty() == false) {
                plan = new Eval(source, plan, nulls);
            }

            if (requiresPacking == false) {
                plan = new Aggregate(source, plan, groupings(step, keys), aggregates(value, step, keys));
                return regrouped(plan, value);
            }
            // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
            if (keys.isEmpty()) {
                plan = new Aggregate(source, plan, groupings(step, List.of()), aggregates(value, step, List.of()));
                return regrouped(plan, value);
            }
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
                    dim -> new ReferenceAttribute(
                        dim.source(),
                        null,
                        dim.name(),
                        dim.dataType().noText(),
                        Nullability.TRUE,
                        dim.id(),
                        false
                    )
                )
                .toList();
            UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
            List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
            projections.addAll(unpackedDims);
            return regrouped(new Project(source, unpackDims, projections), value);
        }

        /** The regrouped table: regroups genuinely drop columns, so parents read survivors off the regrouped plan. */
        private IntermediateResult regrouped(LogicalPlan plan, Alias value) {
            return new IntermediateResult(plan, value.toAttribute(), step, pendingFilter, Kind.AFTER_INITIAL_AGGREGATE);
        }

        /** The value as a defined column; only valid on a finished table. */
        public Attribute valueColumn() {
            return (Attribute) value;
        }
    }

    // -- helpers --

    /** The canonical name exposed at a finished command's boundary. */
    public static String asMetadataLabel() {
        return asMetadataLabel(Set.of());
    }

    /** The existing internal name distinguishing {@code _timeseries} columns with different exclusions. */
    public static String asMetadataLabel(Set<String> skip) {
        return MetadataAttribute.TIMESERIES + (skip.isEmpty() ? "" : "$" + String.join("$", new TreeSet<>(skip)));
    }

    /** Canonical label names in declaration order, without duplicates. */
    public static List<String> asPromotedLabels(Collection<? extends Attribute> attributes) {
        return attributes.stream().map(TranslationContext::asPromotedLabel).distinct().toList();
    }

    /** Label names ignore the physical field prefix used for Prometheus passthrough dimensions. */
    public static String asPromotedLabel(Attribute attribute) {
        String name = attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
        return name.startsWith(PROMETHEUS_LABELS_PREFIX) ? name.substring(PROMETHEUS_LABELS_PREFIX.length()) : name;
    }

    /** A missing label's reference, subsequently defined as null by its consumer. */
    public static Attribute mapToRef(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /** The skip sets of a schema ordered finest first: the grain-fixing {@code _timeseries} column leads, coarser
     * variants follow. */
    public static List<Set<String>> finestFirst(Set<Set<String>> skips) {
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
            if (asPromotedLabel(attribute).equals(label)) {
                if (attribute.name().equals(label) == false) {
                    return attribute;
                }
                bareMatch = attribute;
            }
        }
        return bareMatch;
    }

    /**
     * The {@code _timeseries} columns a plan carries: output column name to the label set it excludes, read off the
     * {@link TimeSeriesWithout} definitions below. A definition whose name no longer reaches the output
     * (projected away by a join or a regroup by promoted labels) is not carried and is left out.
     */
    private static Map<String, Set<String>> timeSeriesColumns(LogicalPlan plan) {
        var definitions = new LinkedHashMap<String, Set<String>>();
        for (Aggregate aggregate : plan.collect(Aggregate.class)) {
            for (Expression grouping : aggregate.groupings()) {
                if (grouping instanceof Alias alias && alias.child() instanceof TimeSeriesWithout without) {
                    var skip = new LinkedHashSet<String>();
                    for (Expression field : without.children()) {
                        if (field instanceof Attribute attribute) {
                            skip.add(asPromotedLabel(attribute));
                        }
                    }
                    Set<String> previous = definitions.putIfAbsent(alias.name(), Set.copyOf(skip));
                    assert previous == null || previous.equals(skip)
                        : "invariant: _timeseries column ["
                            + alias.name()
                            + "] has one exclusion set, got ["
                            + previous
                            + "] and ["
                            + skip
                            + "]";
                }
            }
        }
        var outputNames = new HashSet<String>();
        for (Attribute attr : plan.output()) {
            outputNames.add(attr.name());
        }
        definitions.keySet().retainAll(outputNames);
        return definitions;
    }

    /** The exclusion sets of the {@code _timeseries} columns a plan carries, in output order. */
    private static Set<Set<String>> deliveredSkips(LogicalPlan plan) {
        Map<String, Set<String>> columns = timeSeriesColumns(plan);
        var delivered = new LinkedHashSet<Set<String>>();
        for (Attribute attr : plan.output()) {
            Set<String> skip = columns.get(attr.name());
            if (skip != null) {
                delivered.add(skip);
            }
        }
        return Collections.unmodifiableSet(delivered);
    }

    /**
     * The label coverage a table delivers, as a constraint: its promoted label columns plus its metadata, its
     * {@code _timeseries} columns, read off the table's plan. The one way a node learns what a translated child carries.
     */
    public static TranslationSchema newConstraintDeliveredBy(IntermediateResult table) {
        return new TranslationSchema(deliveredLabels(table.plan(), table.step(), table.value()), deliveredSkips(table.plan()));
    }

    /**
     * The promoted labels a plan exposes: every output column except the step, the value and the columns it is computed
     * from, and the {@code _timeseries} columns.
     */
    private static Set<String> deliveredLabels(LogicalPlan plan, Attribute step, Expression value) {
        Map<String, Set<String>> columns = timeSeriesColumns(plan);
        Set<NameId> valueInputs = valueInputs(plan, value);
        var labels = new LinkedHashSet<String>();
        for (Attribute attr : plan.output()) {
            if (attr.id().equals(step.id())) {
                continue;
            }
            if (valueInputs.contains(attr.id())) {
                continue;
            }
            if (columns.containsKey(attr.name()) == false) {
                labels.add(asPromotedLabel(attr));
            }
        }
        return Collections.unmodifiableSet(labels);
    }

    /**
     * The value column and, through the {@link Eval}s defining it, every column it is computed from. A binary operator
     * fused into one aggregate keeps both operands' values next to its own: they feed the value and are never labels.
     */
    private static Set<NameId> valueInputs(LogicalPlan plan, Expression value) {
        Map<NameId, Expression> definitions = new HashMap<>();
        plan.forEachDown(Eval.class, eval -> eval.fields().forEach(field -> definitions.putIfAbsent(field.id(), field.child())));
        Set<NameId> inputs = new HashSet<>();
        Deque<Expression> pending = new ArrayDeque<>(List.of(value));
        while (pending.isEmpty() == false) {
            for (Attribute input : pending.pop().references()) {
                Expression definition = definitions.get(input.id());
                if (inputs.add(input.id()) && definition != null) {
                    pending.push(definition);
                }
            }
        }
        return inputs;
    }

    /**
     * The {@code _timeseries} column with the fewest excluded labels, or null when the plan carries none. The
     * grain-fixing {@code _timeseries} of a finished table, read straight off the plan.
     */
    // visible for testing
    static Attribute finestTimeSeries(LogicalPlan plan) {
        Map<String, Set<String>> columns = timeSeriesColumns(plan);
        if (columns.isEmpty()) {
            return null;
        }
        Attribute finest = null;
        int fewest = Integer.MAX_VALUE;
        for (Attribute attr : plan.output()) {
            Set<String> skip = columns.get(attr.name());
            if (skip != null && skip.size() < fewest) {
                fewest = skip.size();
                finest = attr;
            }
        }
        return finest;
    }
}
