/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.time.DateUtils;
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
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry.PromqlContext;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.DynamicColumnList;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationColumn.Static;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationResult.Kind;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch.Joining;

/**
 * One translation of a PromQL command into an ES|QL plan. Runs before {@code TranslateTimeSeriesAggregate} to convert the
 * PromQL nodes into standard ES|QL nodes (TimeSeriesAggregate, Aggregate, Eval, etc.). Examples:
 * <pre>
 * PromQL: sum by (cluster) (rate(http_requests[5m]))
 * Result: TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: time() - avg(sum by (cluster) (rate(http_requests[5m])))
 * Result: Eval[time() - avg_result]
 *           \_ Aggregate[avg(sum_result), groupBy=[step]]
 *                 \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 * </pre>
 * Mechanism: every PromQL node translates itself ({@link PromqlPlan#translate}) under a {@code TranslationContext} that carries
 * what the enclosing node {@link #required() requires} of its labels and the shared services: recursion into a child
 * under a requirement ({@link #translate(LogicalPlan, TranslationConstraint)}), and the two ways a node composes its child's
 * table - {@link #aggregate} (GROUP BY the keys the node requires) and {@link #eval} (a value expression over the
 * child). The PromQL tree is walked top-down and the ES|QL plan assembled bottom-up on the way back; the top-level forms
 * (a single branch, an {@code or} union) stitch finished tables.
 */
public final class TranslationContext {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end): TStep requires explicit bounds,
    // so pass the widest representable range. EPOCH/MAX_MILLIS_BEFORE_9999 avoid time boundary handling in the engine.
    private static final Instant EPOCH_MIN = Instant.EPOCH;
    private static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    private final PromqlCommand cmd;
    private final AnalyzerContext analyzer;
    /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
    private final Alias stepBucketAlias;
    /* The label columns the subtree being translated MUST deliver. */
    private final TranslationConstraint required;
    /* The current evaluation time (default: @timestamp). */
    private final Expression time;

    /** The translation of a whole command: the query output exposes every label of the result series. */
    public TranslationContext(PromqlCommand cmd, AnalyzerContext analyzer) {
        // IN: every label of the result series
        this(cmd, analyzer, null, TranslationConstraint.any(), null);
    }

    private TranslationContext(
        PromqlCommand cmd,
        AnalyzerContext analyzer,
        Alias stepBucketAlias,
        TranslationConstraint required,
        Expression time
    ) {
        this.cmd = cmd;
        this.analyzer = analyzer;
        this.stepBucketAlias = stepBucketAlias;
        this.required = required;
        this.time = time;
    }

    // ---------- context ----------

    public PromqlCommand cmd() {
        return cmd;
    }

    public Configuration configuration() {
        return analyzer.configuration();
    }

    /** The evaluation timestamp of the branch being translated. */
    public Expression time() {
        return time;
    }

    public Attribute stepAttr() {
        return stepBucketAlias != null ? stepBucketAlias.toAttribute() : cmd.stepAttribute();
    }

    /** What the enclosing node requires the node being translated to deliver. */
    public TranslationConstraint required() {
        return required;
    }

    public PromqlContext promqlContext(TranslationResult child) {
        return promqlContext(child, AggregateFunction.NO_WINDOW);
    }

    public PromqlContext promqlContext(TranslationResult child, Expression window) {
        return new PromqlContext(time, window, child.step(), configuration());
    }

    // ---------- services ----------

    /** Translates a child node under what the current node requires of it. */
    public TranslationResult translate(LogicalPlan child, TranslationConstraint required) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, required, time).translate(child);
    }

    private TranslationResult translate(LogicalPlan node) {
        if (node instanceof PromqlPlan promql) {
            return promql.translate(this);
        }
        throw new QlIllegalArgumentException("unsupported PromQL plan node: {}", node);
    }

    /** Translates a join operand into a finished table under {@code required}, with its own step and value identities. */
    public TranslationResult translateOperand(LogicalPlan operand, TranslationConstraint required) {
        return new TranslationContext(cmd, analyzer, stepBucketAlias, required, time).translateIntermediate(
            operand,
            new NameId(),
            new NameId()
        );
    }

    /**
     * The value primitive: {@code value} computed over the child; the labels pass through. Expressions compose lazily up
     * the tree until they cross an aggregation boundary: once the plan below is aggregated, the expression must
     * materialize as the value column (an Eval) so parents reference it by attribute.
     */
    public TranslationResult eval(TranslationResult child, Expression value) {
        if (child.kind().afterInitialAggregation == false) {
            return child.with(child.plan(), value);
        }
        Alias alias = new Alias(value.source(), cmd.valueColumnName(), value);
        return child.with(new Eval(cmd.source(), child.plan(), List.of(alias)), alias.toAttribute());
    }

    /**
     * The aggregation primitive at the table's own grain: every carried column stays a key. A raw table collapses once
     * through the innermost {@link TimeSeriesAggregate}; an already-collapsed table regroups through {@link Aggregate}.
     */
    public TranslationResult aggregate(TranslationResult input, Expression function) {
        return aggregate(input, input.shape(), function, false);
    }

    /** Aggregates at {@code keys}: a key label the input lacks is grouped under null, like Prometheus. */
    public TranslationResult aggregate(TranslationResult input, TranslationConstraint keys, Expression function) {
        return aggregate(input, keys, function, false);
    }

    /**
     * Aggregates at {@code keys}. A packed regroup ({@code packed}, or any open key set) additionally packs dimensions
     * before aggregation to prevent multi-valued dimensions from splitting rows and double-counting, then unpacks them
     * afterwards.
     */
    public TranslationResult aggregate(TranslationResult input, TranslationConstraint keys, Expression function, boolean packed) {
        Alias value = new Alias(function.source(), cmd.valueColumnName(), function);
        if (input.kind().afterInitialAggregation == false) {
            assert input.kind() == Kind.BEFORE_INITIAL_AGGREGATE : "[INVARIANT]: aggregates take raw or collapsed tables";
            return emitCollapse(input, keys, value);
        }
        return emitRegroup(input, keys, value, keys.isOpen() || packed);
    }

    /**
     * The table with exactly {@code keys} as its label columns - where a requirement meets a table that is already
     * translated. A complement is the table's packing excluding exactly those labels (the requirement that produced it
     * was pushed down to the scan) or, for a table with known labels and no packing, all of them but the excluded ones:
     * everything atop a {@code by} is static. A name is the table's column, or a null fill when the table lacks the label,
     * so every key is a column.
     */
    public TranslationResult bind(TranslationResult input, TranslationConstraint keys, Source source) {
        var bound = new LinkedHashMap<TranslationColumn, Attribute>();
        var nullFills = new ArrayList<Alias>();
        for (Set<String> except : keys.allBut()) {
            var column = new DynamicColumnList(except);
            Attribute packed = input.labels().get(column);
            if (packed != null) {
                bound.put(column, packed);
                continue;
            }
            assert input.grain() == null : "[INVARIANT]: packing " + column.name() + " must be delivered by " + input.labels();
            input.labels().forEach((c, attribute) -> {
                if (c instanceof Static s && except.contains(s.name()) == false) {
                    bound.put(c, attribute);
                }
            });
        }
        for (String name : keys.names()) {
            Attribute attribute = input.label(name);
            if (attribute == null) {
                Alias fill = nullAlias(ref(name));
                nullFills.add(fill);
                attribute = fill.toAttribute();
            }
            bound.put(new Static(name), attribute);
        }
        LogicalPlan plan = nullFills.isEmpty() ? input.plan() : new Eval(source, input.plan(), nullFills);
        return input.with(plan, bound, input.value());
    }

    /** A fresh reference to a label the plan does not carry yet; {@link #nullAlias} defines it. */
    public static Attribute ref(String label) {
        return new ReferenceAttribute(Source.EMPTY, null, label, DataType.KEYWORD);
    }

    /** The definition of {@code attribute} as null, under its own id. */
    public static Alias nullAlias(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    /**
     * Translates one operand or merge branch into a finished table with its own step bucket, value column and evaluation
     * time: the source-time and matcher filters pushed down, the initial per-series aggregate applied, the value cast to
     * double under {@code valueId}.
     */
    public TranslationResult translateIntermediate(LogicalPlan branch, NameId stepId, NameId valueId) {
        Expression branchTime = cmd.collectEvaluationTimestampForBranch(branch);
        Alias step = canCreateStepBucket() ? emitStepBucketExpression(stepId, branchTime) : null;
        var run = new TranslationContext(cmd, analyzer, step, required, branchTime);
        return run.translateIntermediate(branch, valueId);
    }

    // ---------- the command ----------

    public LogicalPlan translateFinal() {
        if (cmd.promqlPlan() instanceof VectorBinaryOperator op) {
            VectorMatch match = op.match();
            if (match.filter() != VectorMatch.Filter.NONE || match.grouping() != Joining.NONE) {
                // Explicit matching translates to a join whose operands the verifier requires to have concrete label
                // sets, so its result names every label and carries no packed identity to expose. (A default match
                // like `a / b` has an open identity and takes the single-branch path below.)
                // TODO: relax this requirement once expression like `foo on(a, b) / bar` is supprted
                return doTranslateFinal(op.translate(this).plan(), null, false);
            }
        }

        // `or` is the only set operator that adds rows (more series), requiring a top-level multi-branch `UnionAll` that
        // cannot compose as a single-value sub-expression.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        var branches = new ArrayList<LogicalPlan>();
        flattenUnion(cmd.promqlPlan(), branches);

        if (branches.size() == 1) {
            TranslationResult ir = translateIntermediate(cmd.promqlPlan(), cmd.stepId(), cmd.valueId());
            return doTranslateFinal(ir.plan(), ir.grain(), ir.kind().constant);
        }

        // Compile every branch as its own module (own step/value ids, own shifted evaluation timestamp), then link.
        var intermediateResultPlan = doTranslateUnion(
            branches.stream().map(b -> translateIntermediate(b, new NameId(), new NameId())).toList()
        );
        return doTranslateFinal(intermediateResultPlan, null, false);
    }

    /**
     * Shared by every `final` translation root. {@code identity} is the column carrying the result's series identity
     * (its grain), or null when the label set is closed.
     */
    private LogicalPlan doTranslateFinal(LogicalPlan plan, Attribute identity, boolean localRelation) {
        plan = emitNullsFilter(cmd.source(), emitFinalProjection(plan, identity), cmd.valueAttribute());
        return localRelation ? plan : emitByStepFilter(plan);
    }

    /**
     * Union combinator over independently translated tabular results.
     * {@link UnionAll} aligns columns by name and null-fills missing labels, then
     * {@link TopNBy} keeps single row per {@code (step, labelset)} group ordered by incoming IR order.
     */
    private LogicalPlan doTranslateUnion(List<TranslationResult> intermediateResults) {
        // Already validated against MergePlan.MAX_BRANCHES by PromqlCommand.verify
        assert MergePlan.exceedsMaxBranches(intermediateResults.size()) == false
            : "[INVARIANT]: merge branch count ["
                + intermediateResults.size()
                + "] must be less of equal MergePlan.MAX_BRANCHES ["
                + MergePlan.MAX_BRANCHES
                + "]";

        var source = cmd.source();
        var branchPlans = new ArrayList<LogicalPlan>(intermediateResults.size());
        for (int i = 0; i < intermediateResults.size(); i++) {
            var ir = intermediateResults.get(i);
            LogicalPlan branchPlan = ir.plan();
            // Each branch is projected to its public shape: value, step, its labels, its series identity
            // (renamed to `_timeseries` for name-based union alignment) and the branch tag. The explicit
            // projection also pins the page layout to the branch output: pages cross an exchange and an
            // Eval below (the value double-cast) can name-shadow a column, leaving its channel in the
            // page but not in output() (see #158164).
            var branchOutput = new ArrayList<Attribute>();
            branchOutput.add(ir.valueColumn());
            branchOutput.add(ir.step());
            for (String label : ir.statics()) {
                branchOutput.add(ir.label(label));
            }
            Attribute identity = ir.grain();
            if (identity != null) {
                if (identity.name().equals(MetadataAttribute.TIMESERIES) == false) {
                    var alias = new Alias(source, MetadataAttribute.TIMESERIES, identity, new NameId());
                    branchPlan = new Eval(source, branchPlan, List.of(alias));
                    identity = alias.toAttribute();
                }
                branchOutput.add(identity);
            }
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            branchPlan = emitNullsFilter(source, branchPlan, ir.valueColumn());
            var branchTag = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            branchOutput.add(branchTag.toAttribute());
            branchPlans.add(new Project(source, new Eval(source, branchPlan, List.of(branchTag)), branchOutput));
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
    private TranslationResult translateIntermediate(LogicalPlan branch, NameId valueId) {
        TranslationResult ir = doTranslateTryInline(translate(branch));

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
                ir = aggregate(ir.with(plan, value), value);
                plan = ir.plan();
                value = ir.value();
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

        // The value column definition: the branch's value expression cast to double under the caller's id.
        Alias valueAlias = emitValueDoubleCastExpression(value, valueId);
        plan = new Eval(cmd.source(), plan, List.of(valueAlias));
        if (ir.kind().constant == false) {
            plan = pushDownEvaluationTimestampFilter(plan, branch);
        }

        Kind kind = ir.kind().constant ? Kind.CONSTANT : Kind.AFTER_INITIAL_AGGREGATE;
        return new TranslationResult(plan, ir.labels(), valueAlias.toAttribute(), ir.step(), null, kind);
    }

    /** Folds a branch whose value depends on nothing but the step column into a compile-time step/value relation. */
    private TranslationResult doTranslateTryInline(TranslationResult result) {
        Attribute stepAttr = cmd.stepAttribute();
        if (result.kind().constant
            || result.labels().isEmpty() == false
            || cmd.start().value() == null
            || result.value().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr)) == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.value().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new TranslationResult(plan, Map.of(), value, step, result.pendingFilter(), Kind.CONSTANT);
    }

    // ---------- aggregation ----------

    private static TranslationResult table(
        LogicalPlan plan,
        TranslationResult input,
        Alias value,
        Map<TranslationColumn, Attribute> labels
    ) {
        return new TranslationResult(plan, labels, value.toAttribute(), input.step(), input.pendingFilter(), Kind.AFTER_INITIAL_AGGREGATE);
    }

    /**
     * The innermost aggregate groups on the child's already-defined columns. Packed groupings receive distinct
     * output names without exposing their source representation to the aggregate.
     */
    private TranslationResult emitCollapse(TranslationResult input, TranslationConstraint keys, Alias value) {
        Source source = cmd.promqlPlan().source();
        boolean groupsBySeries = keys.isEmpty() == false;
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

        // Dynamic columns are grouped by increasing exclusions, followed by the static labels. Every column is
        // functionally dependent on the finest dynamic column, so grouping by all of them preserves per-series
        // granularity while making the full label set available to the surrounding query. A packing is aliased to its
        // column name so the aggregate output does not expose its source representation; the alias is a grouping
        // expression the aggregate itself defines, so the null-fills are taken before it.
        TranslationResult bound = bind(input, keys, source);
        var groupKeys = new ArrayList<NamedExpression>();
        var out = new LinkedHashMap<TranslationColumn, Attribute>();
        bound.labels().forEach((column, attribute) -> {
            if (column instanceof DynamicColumnList dynamic) {
                Alias packed = new Alias(source, dynamic.name(), attribute, attribute.id());
                groupKeys.add(packed);
                out.put(column, packed.toAttribute());
            } else {
                groupKeys.add(attribute);
                out.put(column, attribute);
            }
        });
        LogicalPlan plan = new TimeSeriesAggregate(
            source,
            bound.plan(),
            groupings(stepBucketAlias, groupKeys),
            aggregates(value, input.step(), List.copyOf(out.values())),
            null,
            time,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
        return table(plan, input, value, out);
    }

    /**
     * Regroups an already-aggregated table. Every regroup consumes its child's columns and null-fills missing
     * grouping columns. A packed regroup additionally packs dimensions before aggregation to prevent multi-valued
     * dimensions from splitting rows and double-counting, then unpacks them afterwards.
     */
    private TranslationResult emitRegroup(TranslationResult input, TranslationConstraint keys, Alias value, boolean requiresPacking) {
        Source source = cmd.source();
        Attribute step = input.step();
        if (value.child() instanceof AggregateFunction == false) {
            value = value.replaceChild(new Values(value.child().source(), value.child()));
        }
        // a declared label the child lacks is absent from every series: grouped under null, like Prometheus
        TranslationResult bound = bind(input, keys, source);
        LogicalPlan plan = bound.plan();
        List<Attribute> keyAttributes = bound.attributes();

        // TranslateTimeSeriesAggregate unpacks the inner TSA's dimensions and this regroup re-packs them.
        if (requiresPacking == false || keyAttributes.isEmpty()) {
            plan = new Aggregate(source, plan, groupings(step, keyAttributes), aggregates(value, step, keyAttributes));
            return table(plan, input, value, bound.labels());
        }
        Attribute packedAttribute = PackDims.newPackedAttribute(source);
        PackDims packDims = new PackDims(source, plan, keyAttributes, packedAttribute);
        Alias packedGrouping = PackDims.newPackedGrouping(source, packedAttribute);
        Aggregate agg = new Aggregate(
            source,
            packDims,
            groupings(step, List.of(packedGrouping)),
            aggregates(value, step, List.of(packedGrouping.toAttribute()))
        );
        List<Attribute> unpackedDims = keyAttributes.stream()
            .<Attribute>map(
                dim -> new ReferenceAttribute(dim.source(), null, dim.name(), dim.dataType().noText(), Nullability.TRUE, dim.id(), false)
            )
            .toList();
        UnpackDims unpackDims = new UnpackDims(source, agg, packedGrouping.toAttribute(), unpackedDims);
        List<NamedExpression> projections = new ArrayList<>(List.of(value.toAttribute(), step));
        projections.addAll(unpackedDims);
        Map<NameId, Attribute> unpacked = new HashMap<>();
        unpackedDims.forEach(attribute -> unpacked.put(attribute.id(), attribute));
        var rebound = new LinkedHashMap<TranslationColumn, Attribute>();
        bound.labels().forEach((column, attribute) -> rebound.put(column, unpacked.get(attribute.id())));
        return table(new Project(source, unpackDims, projections), input, value, rebound);
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

    // ---------- the command coda ----------

    /** Projects the plan to the command's declared output, re-aliasing columns that match by name but not by id. */
    private LogicalPlan emitFinalProjection(LogicalPlan plan, Attribute packing) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }
        // Under a passthrough mapping the plan carries the concrete field (`labels.job`) while the command declares
        // the label alone, so fall back to the canonical name.
        for (var attr : plan.output()) {
            lookupMap.putIfAbsent(PromqlLabels.labelName(attr), attr);
        }
        // Dynamic columns travel under names derived from their exclusions,
        // e.g.: * \ {a,b} is encoded as `_timeseries$a$b`;
        // Final output drops `$a$b` suffix.
        if (packing != null) {
            lookupMap.put(MetadataAttribute.TIMESERIES, packing);
        }
        var projected = new ArrayList<Attribute>();
        var evals = new ArrayList<Alias>();
        for (var attr : cmd.output()) {
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                var alias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                evals.add(alias);
                projected.add(alias.toAttribute());
            } else {
                projected.add(attr);
            }
        }
        if (evals.isEmpty() == false) {
            plan = new Eval(cmd.source(), plan, evals);
        }
        return new Project(cmd.source(), plan, projected);
    }

    /** Keeps only steps within the query range; step buckets are anchored at {@code start} and offset-independent. */
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
            var timestamp = new Alias(cmd.source(), cmd.timestampColumnName(), shifted, ref.id());
            return addEvaluationTimestamp(plan, timestamp);
        }
        return plan;
    }

    /** Joined operands already own their evaluation times; only visit this branch's source. */
    private LogicalPlan addEvaluationTimestamp(LogicalPlan plan, Alias timestamp) {
        if (plan instanceof InnerJoin) {
            return plan;
        }
        if (plan instanceof EsRelation) {
            return new Eval(cmd.source(), plan, List.of(timestamp));
        }
        return plan.replaceChildren(plan.children().stream().map(child -> addEvaluationTimestamp(child, timestamp)).toList());
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

    /** The value column definition: the branch's value expression, cast to double unless it provably is one. */
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
}
