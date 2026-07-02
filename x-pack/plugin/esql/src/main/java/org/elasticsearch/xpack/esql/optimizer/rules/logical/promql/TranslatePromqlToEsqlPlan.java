/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TStep;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.InheritedAttributes;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.SynthesizedAttributes;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.HistogramQuantile;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.MetadataAttribute.isTimeSeriesAttributeName;
import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.withFilter;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAndNullable;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.PromqlAttributesTranslationContext.canonicalName;

/**
 * Translates PromQL logical plan into ESQL plan.
 * <p>
 * This rule runs before {@link TranslateTimeSeriesAggregate} to convert PromQL-specific plan
 * into standard ESQL nodes (TimeSeriesAggregate, Aggregate, Eval, etc.) that can then be further optimized.
 * <p>
 * Translation examples:
 * <pre>
 * PromQL: rate(http_requests[5m])
 * Result: TimeSeriesAggregate[rate(value), groupBy=[step]]
 *
 * PromQL: sum by (cluster) (rate(http_requests[5m]))
 * Result: TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: avg(sum by (cluster) (rate(http_requests[5m])))
 * Result: Aggregate[avg(sum_result), groupBy=[step]]
 *           \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: avg(ceil(sum by (cluster) (rate(http_requests[5m]))))
 * Result: Aggregate[avg(ceil_result), groupBy=[step]]
 *           \_ Eval[ceil(sum_result)]
 *                 \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: time() - avg(sum by (cluster) (rate(http_requests[5m])))
 * Result: Eval[time() - avg_result]
 *           \_ Aggregate[avg(sum_result), groupBy=[step]]
 *                 \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 * </pre>
 *
 * Translation mechanism:
 * <p>
 * Recursive descent via {@code translateNode()}. Each node returns a {@code TranslationResult}:
 * <ul>
 *   <li>{@code plan} the LogicalPlan built so far</li>
 *   <li>{@code expression} reference to this node's output, composed into parent expressions</li>
 * </ul>
 *
 * Example translations:
 * <ul>
 *   <li>{@link Selector}: plan unchanged, expression = LastOverTime(field) or field reference</li>
 *   <li>{@link AcrossSeriesAggregate}: plan = new Aggregate, expression = reference to aggregate output</li>
 *   <li>{@link PromqlFunctionCall}: plan = Eval if child aggregated, expression = function(child expr)</li>
 *   <li>{@link VectorBinaryOperator}: plan = merged from both sides, expression = left op right</li>
 * </ul>
 */
public final class TranslatePromqlToEsqlPlan extends AnalyzerRules.ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {
    // Sentinel bounds for open-ended range queries (PROMQL step=X without explicit start/end).
    // TStep requires explicit lower and upper bounds, so we pass the widest representable range.
    // Use Instant.EPOCH / MAX_MILLIS_BEFORE_9999 instead of Long.MIN/MAX to avoid time boundary handling in the engine.
    public static final Instant EPOCH_MIN = Instant.EPOCH;
    public static final Instant EPOCH_MAX = Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999);

    /** Result flows upward */
    private record TranslationResult(
        /* ESQL plan built so far. */
        LogicalPlan plan,
        /* Reference to this node's numeric value, composed into parent expressions. */
        Expression expression,
        /* Label matcher flows up until an aggregate that folds it / push to relation */
        Expression pendingFilter,
        /* The attributes this subtree exposes. */
        SynthesizedAttributes synthesizedAttributes,
        /* true when the subtree resolved to a constant/empty plan (no aggregation needed) */
        boolean constFolded
    ) {
        TranslationResult(LogicalPlan plan, Expression expression) {
            this(plan, expression, null, SynthesizedAttributes.none(), false);
        }

        TranslationResult(LogicalPlan plan, Expression expression, Expression selectorFilter) {
            this(plan, expression, selectorFilter, SynthesizedAttributes.none(), false);
        }

        TranslationResult(LogicalPlan plan, Expression expression, Expression selectorFilter, SynthesizedAttributes synthesizedAttributes) {
            this(plan, expression, selectorFilter, synthesizedAttributes, false);
        }
    }

    /** Context flows downward */
    private record TranslationContext(
        /* The root PromQL command. */
        PromqlCommand promqlCommand,
        /* Analyzer context (configuration, transport version, etc.). */
        AnalyzerContext analyzerContext,
        /* Alias for the step bucket expression used in all aggregation groupings. May be null for empty indices. */
        Alias stepBucketAlias,
        /* The labels the child subtree MUST produce. */
        InheritedAttributes inheritedAttributes,
        /* The current branch evaluation time (default: @timestamp). */
        Expression time
    ) {
        Attribute stepAttr() {
            return stepBucketAlias != null ? stepBucketAlias.toAttribute() : promqlCommand.stepAttribute();
        }
    }

    @Override
    protected boolean skipResolved() {
        return false;
    }

    /**
     * Create a predicate expression used for time based filter pushdown.
     */
    private static Expression createTimeFilterPredicate(PromqlCommand p, LogicalPlan plan, Configuration configuration) {
        if (p.start().value() == null || p.end().value() == null) {
            return null;
        }
        var offset = p.offset(plan);
        var timestamp = p.timestamp();

        var window = p.sourceFilterWindow();
        var lo = new GreaterThanOrEqual(
            p.source(),
            timestamp,
            new Sub(p.source(), p.start(), Literal.timeDuration(p.source(), window.plus(offset)), configuration)
        );
        var hi = new LessThanOrEqual(
            p.source(),
            timestamp,
            new Sub(p.source(), p.end(), Literal.timeDuration(p.source(), offset), configuration)
        );
        return new And(p.source(), lo, hi);
    }

    /**
     * Add `Eval` materializing synthetic timestamp (default: @timestamp).
     */
    private static LogicalPlan applyIntermediateTimestamp(
        PromqlCommand p,
        LogicalPlan plan,
        Expression evalTime,
        LogicalPlan branch,
        Configuration configuration
    ) {
        if (evalTime instanceof ReferenceAttribute ref && p.timestampColumnName().equals(ref.name())) {
            var expr = new Add(p.source(), p.timestamp(), Literal.timeDuration(p.source(), p.offset(branch)), configuration);
            var time = new Alias(p.source(), p.timestampColumnName(), expr, ref.id());
            return plan.transformUp(node -> node == p.child(), node -> new Eval(p.source(), node, List.of(time)));
        }
        return plan;
    }

    @Override
    protected LogicalPlan rule(PromqlCommand cmd, AnalyzerContext context) {
        // `or` is special-cased here because it is the only set operator that adds rows (more series), which
        // requires a top-level multi-branch UnionAll that cannot compose as a single-value sub-expression. The
        // row-filtering operators `and`/`unless` will instead translate as joins inside translateNode.
        // PromQL `or` is left-associative, so flatten the top-level chain into independent branches.
        List<LogicalPlan> branches = flattenTopLevelUnion(cmd.promqlPlan());

        if (branches.size() == 1) {
            Expression timestampExpression = cmd.timestamp(cmd.promqlPlan());
            Alias stepBucketAlias = canCreateStepBucket(cmd)
                ? createStepBucketAlias(cmd, context.configuration(), cmd.stepId(), timestampExpression)
                : null;
            BranchPlan branch = translateBody(cmd, cmd.promqlPlan(), context, stepBucketAlias, cmd.valueId(), timestampExpression);
            LogicalPlan plan = applyProjection(cmd, branch.plan());
            plan = applyNullOutputFilter(cmd, plan);
            if (branch.constFolded() == false) {
                plan = applyIntermediateTimestamp(cmd, plan, timestampExpression, cmd.promqlPlan(), context.configuration());
                plan = withTimestampFilter(cmd, plan);
            }
            return plan;
        }

        LogicalPlan plan = translateUnion(cmd, context, branches);
        return withTimestampFilter(cmd, plan);
    }

    /**
     * Translates a single PromQL expression (a union branch, or the whole expression when there is no top-level
     * union) into an ESQL plan whose output is {@code [value, step, labels...]}. The final projection to the
     * command output and null-row filtering are applied by the caller.
     */
    private BranchPlan translateBody(
        PromqlCommand cmd,
        LogicalPlan promqlBranch,
        AnalyzerContext context,
        Alias stepBucketAlias,
        NameId valueId,
        Expression timestampExpression
    ) {
        TranslationContext ctx = new TranslationContext(
            cmd,
            context,
            stepBucketAlias,
            InheritedAttributes.unconstrained(),
            timestampExpression
        );

        TranslationResult result = translateNode(promqlBranch, cmd.child(), ctx);

        result = tryConstFoldPureScalar(result, cmd, ctx);

        var plan = result.plan();
        var valueExpr = result.expression();
        var filter = combineAndNullable(
            Arrays.asList(result.pendingFilter(), createTimeFilterPredicate(cmd, promqlBranch, context.configuration()))
        );

        // TODO: Fix selector-free PromQL evaluation to produce values even when no data
        // See https://github.com/elastic/elasticsearch/issues/149791

        if (filter != null) {
            plan = applyLabelFilter(plan, filter, cmd);
        }

        if (result.constFolded() == false) {
            // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
            // TODO: If we ever support metric references without last_over_time, we could
            // skip TimeSeriesAggregate and use plain Aggregate instead (see #141501 discussion).
            if (findAggregate(plan, Aggregate.class) == null) {
                plan = createInnermostAggregatePlan(ctx, plan, SynthesizedAttributes.of(promqlBranch.output()), List.of(), valueExpr);
                valueExpr = getValueOutput(plan);
            }

            if (promqlBranch instanceof VectorBinaryComparison binaryComparison && binaryComparison.filterMode()) {
                // for comparison with the filtering mode, return the left operand and apply filter later
                plan = addComparisonFilter(plan, binaryComparison, context, valueExpr);
            }
        }

        plan = applyValueToDoubleConversion(cmd, plan, valueExpr, valueId);

        return new BranchPlan(plan, result.constFolded());
    }

    /** Result of translating a single PromQL branch, before projection/null/timestamp filtering. */
    private record BranchPlan(LogicalPlan plan, boolean constFolded) {}

    /**
     * Translates a top-level {@code or} chain into a left-preferring union.
     * <p>
     * Each branch is translated independently, tagged with its position via a synthetic
     * {@link PromqlCommand#branchColumnName() branch column}, and combined with {@link UnionAll}
     * (which aligns columns by name and null-fills missing label columns).
     * A {@link TopNBy} then keeps a single row per {@code (step, labelset)} group, ordered by branch ascending,
     * so the leftmost branch's value wins - exactly matching PromQL's {@code a or b} semantics where {@code b}
     * only contributes series whose labelset is absent from {@code a}.
     */
    private LogicalPlan translateUnion(PromqlCommand cmd, AnalyzerContext context, List<LogicalPlan> branches) {
        // Already validated against Fork.MAX_BRANCHES by PromqlCommand.verify; asserted here against regressions.
        assert Fork.exceedsMaxBranches(branches.size()) == false
            : "union branch count [" + branches.size() + "] exceeds Fork.MAX_BRANCHES [" + Fork.MAX_BRANCHES + "]";

        Source source = cmd.source();
        List<LogicalPlan> branchPlans = new ArrayList<>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
            // Each branch gets its own step/value attributes; the final projection maps them to the command output.
            // Branches may carry independent offsets, so each derives its own shifted evaluation timestamp.
            LogicalPlan branch = branches.get(i);
            Expression timestampExpression = cmd.timestamp(branch);
            Alias stepBucketAlias = createStepBucketAlias(cmd, context.configuration(), new NameId(), timestampExpression);
            BranchPlan body = translateBody(cmd, branch, context, stepBucketAlias, new NameId(), timestampExpression);
            LogicalPlan branchPlan = body.plan();
            // Drop null-valued rows per branch so an absent left side does not shadow a present right side.
            branchPlan = applyNullOutputFilter(cmd, branchPlan);
            if (body.constFolded() == false) {
                branchPlan = applyIntermediateTimestamp(cmd, branchPlan, timestampExpression, branch, context.configuration());
            }
            Alias branchTag = new Alias(source, cmd.branchColumnName(), new Literal(source, i, DataType.INTEGER));
            branchPlan = new Eval(source, branchPlan, List.of(branchTag));
            branchPlans.add(branchPlan);
        }

        // The attribute ids chosen here are preserved by name when the analyzer later recomputes the UnionAll
        // output, so the groupings and projection built below remain valid.
        List<Attribute> unionOutput = VectorBinarySet.unionOutputByName(branchPlans);
        UnionAll union = new UnionAll(source, branchPlans, unionOutput);

        // Group by step + all label columns (everything except the value and the synthetic branch tag).
        List<Expression> groupings = new ArrayList<>();
        Attribute branchAttr = null;
        for (Attribute attr : unionOutput) {
            if (attr.name().equals(cmd.branchColumnName())) {
                branchAttr = attr;
            } else if (attr.name().equals(cmd.valueColumnName()) == false) {
                groupings.add(attr);
            }
        }

        Order branchOrder = new Order(source, branchAttr, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        LogicalPlan dedup = new TopNBy(source, union, List.of(branchOrder), new Literal(source, 1, DataType.INTEGER), groupings);

        // Project to the command output; this also drops the synthetic branch tag.
        return applyProjection(cmd, dedup);
    }

    /**
     * Flattens a left-associative top-level {@code or} chain into an ordered list of branch expressions.
     * Non-union nodes (and nested unions reachable only through non-union operators) are returned as single
     * branches. The branch order is left-to-right, so branch 0 has the highest precedence.
     */
    private static List<LogicalPlan> flattenTopLevelUnion(LogicalPlan promqlPlan) {
        List<LogicalPlan> branches = new ArrayList<>();
        flattenTopLevelUnion(promqlPlan, branches);
        return branches;
    }

    private static void flattenTopLevelUnion(LogicalPlan node, List<LogicalPlan> branches) {
        if (node instanceof VectorBinarySet setOp && setOp.op() == VectorBinarySet.SetOp.UNION) {
            flattenTopLevelUnion(setOp.left(), branches);
            flattenTopLevelUnion(setOp.right(), branches);
        } else {
            branches.add(node);
        }
    }

    private static TranslationResult tryConstFoldPureScalar(TranslationResult result, PromqlCommand cmd, TranslationContext ctx) {
        if (result.constFolded() || cmd.start().value() == null) {
            return result;
        }
        Attribute stepAttr = cmd.stepAttribute();
        boolean constOnly = result.expression().references().stream().allMatch(ref -> ref.semanticEquals(stepAttr));
        if (constOnly == false) {
            return result;
        }
        var plan = PromqlLogicalPlanBuilder.buildLocalRelation(cmd);
        var step = plan.output().getFirst();
        var value = result.expression().transformUp(Attribute.class, attr -> attr.semanticEquals(stepAttr) ? step : attr);
        return new TranslationResult(plan, value, result.pendingFilter(), result.synthesizedAttributes(), true);
    }

    private static LogicalPlan applyProjection(PromqlCommand command, LogicalPlan plan) {
        var lookupMap = new HashMap<String, Attribute>();
        for (var attr : plan.output()) {
            lookupMap.put(attr.name(), attr);
        }

        var projected = new ArrayList<>(command.output());
        for (int i = 0; i < projected.size(); i++) {
            var attr = projected.get(i);
            var lookupAttr = lookupMap.get(attr.name());
            if (lookupAttr != null && lookupAttr.semanticEquals(attr) == false) {
                final var tsFieldAttrAlias = new Alias(lookupAttr.source(), attr.name(), lookupAttr, attr.id());
                plan = new Eval(command.source(), plan, List.of(tsFieldAttrAlias));
                projected.set(i, tsFieldAttrAlias.toAttribute());
            }
        }

        return new Project(command.source(), plan, projected);
    }

    /**
     * Recursively translates a PromQL plan node into an ESQL plan node.
     */
    private TranslationResult translateNode(LogicalPlan node, LogicalPlan currentPlan, TranslationContext ctx) {
        return switch (node) {
            case AcrossSeriesAggregate agg -> translateAcrossSeriesAggregate(agg, currentPlan, ctx);
            case HistogramQuantile histogramQuantile -> translateHistogramQuantile(histogramQuantile, currentPlan, ctx);
            case ScalarConversionFunction scalar -> translateScalarConversion(scalar, currentPlan, ctx);
            case WithinSeriesAggregate withinAgg -> translateFunctionCall(withinAgg, currentPlan, ctx);
            case PromqlFunctionCall functionCall -> translateFunctionCall(functionCall, currentPlan, ctx);
            case ScalarFunction scalarFunction -> translateScalarFunction(scalarFunction, currentPlan, ctx);
            case VectorBinaryOperator binaryOp -> translateBinaryOperator(binaryOp, currentPlan, ctx);
            case Selector selector -> translateSelector(selector, currentPlan, ctx);
            default -> throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", node);
        };
    }

    /**
     * Translates {@code AcrossSeriesAggregate} to an ESQL {@code Aggregate}.
     * <p>
     * PromQL aggregation shape is dynamic and can't be expressed in ESQL directly without enumerating the full label set.
     * We avoid that at plan time (for performance): the translator walks the aggregate chain twice, carrying the grouping
     * algebra of {@link PromqlAttributesTranslationContext} (see that class for the transition rules and worked examples). The
     * inherited {@link InheritedAttributes} pushes scope down to the leaf, then the synthesized {@link SynthesizedAttributes}
     * folds each grouping back up.
     *
     * <p>Only {@code AcrossSeriesAggregate} creates plan-level aggregation nodes.
     * {@code WithinSeriesAggregate} and other {@code PromqlFunctionCall} nodes are
     * lowered to expressions and folded into the aggregate.
     */
    private TranslationResult translateAcrossSeriesAggregate(AcrossSeriesAggregate agg, LogicalPlan currentPlan, TranslationContext ctx) {
        // Descend: narrow the inherited attributes and hand them to the child.
        var inheritedAttributes = getInheritedAttributes(agg, ctx);

        TranslationContext childCtx = new TranslationContext(
            ctx.promqlCommand,
            ctx.analyzerContext,
            ctx.stepBucketAlias,
            inheritedAttributes,
            ctx.time
        );
        TranslationResult childResult = translateNode(agg.child(), currentPlan, childCtx);

        if (childResult.constFolded()) {
            return childResult;
        }

        // Ascend: fold this aggregate's grouping over the synthesizedAttributes the child synthesized.
        var synthesizedAttributes = getSynthesizedAttributes(agg, childResult);

        var aggExpression = createAggregateExpression(agg, childResult.expression(), ctx);

        var resultPlan = findAggregate(childResult.plan(), Aggregate.class) != null
            /* child already aggregated, no additional `_tsid` grouping needed */
            ? createOuterAggregatePlan(ctx, childResult.plan(), synthesizedAttributes, aggExpression)
            /* group by _tsid, timestamp and compute optional aggregates, e.g., avg_over_time() */
            : createInnermostAggregatePlan(
                ctx,
                childResult.plan(),
                synthesizedAttributes,
                inheritedAttributes.pathExclusions(),
                aggExpression
            );

        return new TranslationResult(resultPlan, getValueOutput(resultPlan), childResult.pendingFilter(), synthesizedAttributes);
    }

    private static SynthesizedAttributes getSynthesizedAttributes(AcrossSeriesAggregate agg, TranslationResult childResult) {
        return switch (agg.grouping()) {
            case BY -> SynthesizedAttributes.foldIncluding(agg.output(), childResult.synthesizedAttributes());
            case WITHOUT -> SynthesizedAttributes.foldExcluding(agg.groupings(), childResult.synthesizedAttributes());
            case NONE -> SynthesizedAttributes.none();
        };
    }

    private static InheritedAttributes getInheritedAttributes(AcrossSeriesAggregate agg, TranslationContext ctx) {
        return switch (agg.grouping()) {
            case BY -> ctx.inheritedAttributes().limitedTo(agg.groupings());
            case WITHOUT -> ctx.inheritedAttributes().excluding(agg.groupings());
            case NONE -> InheritedAttributes.unconstrained();
        };
    }

    private TranslationResult translateHistogramQuantile(
        HistogramQuantile histogramQuantile,
        LogicalPlan currentPlan,
        TranslationContext ctx
    ) {
        TranslationContext childCtx = new TranslationContext(
            ctx.promqlCommand,
            ctx.analyzerContext,
            ctx.stepBucketAlias,
            histogramQuantileChildLabels(histogramQuantile, currentPlan, ctx),
            ctx.time
        );
        TranslationResult childResult = translateNode(histogramQuantile.child(), currentPlan, childCtx);

        if (childResult.constFolded()) {
            return childResult;
        }

        LogicalPlan childPlan = childResult.plan();
        boolean childAlreadyAggregated = findAggregate(childResult.plan(), Aggregate.class) != null;
        Attribute upperBound = findAttributeByLabelName(childResult.synthesizedAttributes().declared(), HistogramQuantile.LE_LABEL);
        if (upperBound == null && childAlreadyAggregated) {
            upperBound = findAttributeByLabelName(childResult.plan().output(), HistogramQuantile.LE_LABEL);
        }
        SynthesizedAttributes exportLabels;
        if (upperBound == null) {
            // Mirrors Prometheus, which warns and drops series whose `le` bucket label is missing.
            HeaderWarning.addWarning("histogram_quantile: input vector has no le label; no buckets to evaluate");
            exportLabels = preserveTimeseries(childResult.synthesizedAttributes(), histogramQuantile.child().output());
            LogicalPlan filteredChild = new Filter(histogramQuantile.source(), childPlan, Literal.FALSE);
            Expression emptyResult = new Values(histogramQuantile.source(), new Literal(histogramQuantile.source(), null, DataType.DOUBLE));
            LogicalPlan resultPlan = childAlreadyAggregated
                ? createOuterAggregatePlan(ctx, filteredChild, exportLabels, emptyResult)
                : createInnermostAggregatePlan(ctx, filteredChild, exportLabels, List.of(), emptyResult);
            return new TranslationResult(resultPlan, getValueOutput(resultPlan), childResult.pendingFilter(), exportLabels);
        }

        if (childAlreadyAggregated == false) {
            childPlan = createInnermostAggregatePlan(
                ctx,
                childPlan,
                childResult.synthesizedAttributes(),
                List.of(upperBound),
                childResult.expression()
            );
            childResult = new TranslationResult(
                childPlan,
                getValueOutput(childPlan),
                childResult.pendingFilter(),
                synthesizedLabels(childPlan)
            );
            upperBound = findAttributeByLabelName(childPlan.output(), HistogramQuantile.LE_LABEL);
            assert upperBound != null : "histogram_quantile child materialization must expose le";
        }

        // histogram_quantile groups by every label except the `le` bucket label, so the `le` attribute is the
        // single excluded dimension - the synthesized shape drops it and the innermost `_timeseries` excludes it.
        List<Attribute> excludedDimensions = List.of(upperBound);
        exportLabels = SynthesizedAttributes.foldExcluding(excludedDimensions, childResult.synthesizedAttributes());

        // The aggregator consumes bucket counts as doubles; counter buckets are frequently integer/long typed, so cast explicitly.
        Expression count = new ToDouble(histogramQuantile.source(), childResult.expression());
        Expression aggregateExpression = new PromqlHistogramQuantile(
            histogramQuantile.source(),
            count,
            histogramQuantileUpperBound(histogramQuantile.source(), upperBound),
            histogramQuantile.quantile()
        );

        LogicalPlan resultPlan = createOuterAggregatePlan(ctx, childPlan, exportLabels, aggregateExpression);

        return new TranslationResult(resultPlan, getValueOutput(resultPlan), childResult.pendingFilter(), exportLabels);
    }

    /**
     * Ensure {@code _timeseries} survives in the exported labels.
     * Without `le`, no {@link TimeSeriesWithout} is inserted and concrete-dimension grouping drops
     * {@code _timeseries} from the output, yet the command wrapper still projects it. Re-add it here
     * (when the child exposes it) like the le-present path does via excluded dimensions.
     */
    private static SynthesizedAttributes preserveTimeseries(SynthesizedAttributes labels, List<Attribute> childOutput) {
        Attribute ts = PromqlAttributesTranslationContext.findByFieldName(childOutput, MetadataAttribute.TIMESERIES);
        if (ts != null && PromqlAttributesTranslationContext.findByFieldName(labels.declared(), MetadataAttribute.TIMESERIES) == null) {
            return SynthesizedAttributes.of(PromqlAttributesTranslationContext.union(labels.declared(), List.of(ts)));
        }
        return labels;
    }

    private static SynthesizedAttributes synthesizedLabels(LogicalPlan plan) {
        List<Attribute> labels = concreteDimensionAttributes(plan.output());
        Attribute ts = PromqlAttributesTranslationContext.findByFieldName(plan.output(), MetadataAttribute.TIMESERIES);
        if (ts != null) {
            labels = PromqlAttributesTranslationContext.union(labels, List.of(ts));
        }
        return SynthesizedAttributes.of(labels);
    }

    /**
     * histogram_quantile groups by every label except the {@code le} bucket label, so the child must expose the labels
     * the surrounding query still needs (the inherited scope) together with {@code le}. {@link InheritedAttributes#including}
     * widens the inherited scope with the child's concrete dimensions and {@code le}: when the scope is finite (e.g. an
     * outer {@code BY(job)}) those labels are preserved as concrete keys; when it is the full universe the enumerated
     * dimensions stand in for it. When the child has no concrete dimensions and there is no {@code le}, fall back to the
     * raw child output so its {@code _timeseries} identity survives.
     */
    private static InheritedAttributes histogramQuantileChildLabels(
        HistogramQuantile histogramQuantile,
        LogicalPlan currentPlan,
        TranslationContext ctx
    ) {
        List<Attribute> identity = concreteDimensionAttributes(histogramQuantile.child().output());
        Attribute upperBound = findAttributeByLabelName(currentPlan.output(), HistogramQuantile.LE_LABEL);
        if (upperBound != null) {
            return ctx.inheritedAttributes().including(identity).including(List.of(upperBound));
        }
        if (identity.isEmpty()) {
            return ctx.inheritedAttributes().limitedTo(histogramQuantile.child().output());
        }
        return ctx.inheritedAttributes().including(identity);
    }

    private static List<Attribute> concreteDimensionAttributes(List<Attribute> attributes) {
        return PromqlAttributesTranslationContext.filterDimensionAttributes(attributes)
            .stream()
            // FieldAttribute.timeSeriesAttribute(...) also reports as a dimension; keep it out of concrete label demand.
            .filter(attribute -> isTimeSeriesAttributeName(attribute.name()) == false)
            .toList();
    }

    private static Attribute findAttributeByLabelName(List<Attribute> attributes, String labelName) {
        // Prometheus passthrough dimensions surface under two names: the concrete field (e.g. `labels.pod`) and a short
        // alias (`pod`). `canonicalName` strips the passthrough prefix, so both match `labelName`. The `_timeseries`
        // block loader excludes by the concrete dimension field name, so we must resolve to the concrete (prefixed)
        // attribute and never to the bare alias - otherwise the exclusion name never matches and the label leaks. The
        // bare attribute is the right answer only when no prefixed variant exists (a top-level dimension), so keep it
        // as a fallback. Without this preference the result depended on the alphabetical order of `attributes`: labels
        // sorting after `labels.` (e.g. `pod`) happened to hit the concrete field first, while earlier ones (`cluster`,
        // `job`, `instance`) hit the alias and leaked.
        Attribute bareMatch = null;
        for (var attr : attributes) {
            if (canonicalName(attr).equals(labelName)) {
                if (attr.name().equals(labelName) == false) {
                    return attr;
                }
                bareMatch = attr;
            }
        }
        return bareMatch;
    }

    private static Expression histogramQuantileUpperBound(Source source, Attribute upperBound) {
        if (upperBound == null) {
            return Literal.keyword(source, null);
        }
        return upperBound;
    }

    /** scalar() collapse to one value per step. */
    private TranslationResult translateScalarConversion(
        ScalarConversionFunction scalarFunc,
        LogicalPlan currentPlan,
        TranslationContext ctx
    ) {
        TranslationResult childResult = translateNode(scalarFunc.child(), currentPlan, ctx);

        // Constant folds directly
        if (childResult.expression().foldable()) {
            return new TranslationResult(
                childResult.plan(),
                new ToDouble(scalarFunc.source(), childResult.expression()),
                childResult.pendingFilter()
            );
        }

        // Child aggregated collapsed to step-only grouping
        // E.g., scalar(sum by (cluster) (metric)))
        Expression scalarExpr = new Scalar(scalarFunc.source(), childResult.expression());
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            // plain Aggregate grouped by step only, collapsing all series into one value per step.
            Alias aggAlias = new Alias(scalarExpr.source(), ctx.promqlCommand().valueColumnName(), scalarExpr);
            Attribute stepAttr = ctx.stepAttr();
            LogicalPlan aggregate = new Aggregate(
                ctx.promqlCommand().source(),
                childResult.plan(),
                List.of(stepAttr),
                List.of(aggAlias, stepAttr)
            );
            return new TranslationResult(aggregate, getValueOutput(aggregate), childResult.pendingFilter());
        }

        LogicalPlan timeSeriesAgg = createInnermostAggregatePlan(
            ctx,
            childResult.plan(),
            SynthesizedAttributes.none(),
            List.of(),
            scalarExpr
        );
        return new TranslationResult(timeSeriesAgg, getValueOutput(timeSeriesAgg), childResult.pendingFilter());
    }

    /** The first output attribute is always the value column. */
    private static Expression getValueOutput(LogicalPlan plan) {
        return plan.output().getFirst().toAttribute();
    }

    /**
     * Translates a generic PromQL function call (ceil, abs, floor, etc.).
     */
    private TranslationResult translateFunctionCall(PromqlFunctionCall functionCall, LogicalPlan currentPlan, TranslationContext ctx) {
        TranslationResult childResult = translateNode(functionCall.child(), currentPlan, ctx);

        if (childResult.constFolded()) {
            return childResult;
        }

        Expression window = AggregateFunction.NO_WINDOW;
        if (functionCall.child() instanceof RangeSelector rangeSelector) {
            window = rangeSelector.range();
            if (isImplicitRangePlaceholder(window)) {
                window = ctx.promqlCommand().resolveImplicitRangeWindow();
            }
        }

        PromqlFunctionRegistry.PromqlContext promqlCtx = new PromqlFunctionRegistry.PromqlContext(
            ctx.time(),
            window,
            ctx.stepAttr(),
            ctx.analyzerContext().configuration()
        );

        Expression function = functionCall.buildEsqlFunction(childResult.expression(), promqlCtx);

        // Wrap already aggregated child in Eval
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            Alias evalAlias = new Alias(function.source(), ctx.promqlCommand().valueColumnName(), function);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), childResult.plan(), List.of(evalAlias));
            return new TranslationResult(
                evalPlan,
                evalAlias.toAttribute(),
                childResult.pendingFilter(),
                childResult.synthesizedAttributes()
            );
        }

        return new TranslationResult(childResult.plan(), function, childResult.pendingFilter(), childResult.synthesizedAttributes());
    }

    private static boolean isImplicitRangePlaceholder(Expression range) {
        return range.foldable()
            && range.fold(FoldContext.small()) instanceof Duration duration
            && duration.equals(PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER);
    }

    /**
     * Translates a scalar function (time(), etc.).
     * These produce expressions without modifying the plan.
     */
    private TranslationResult translateScalarFunction(ScalarFunction scalarFunction, LogicalPlan currentPlan, TranslationContext ctx) {
        PromqlCommand cmd = ctx.promqlCommand();
        var function = scalarFunction.buildEsqlFunction(
            new PromqlFunctionRegistry.PromqlContext(cmd.timestamp(), null, cmd.stepAttribute(), ctx.analyzerContext().configuration())
        );

        return new TranslationResult(currentPlan, function);
    }

    /**
     * Translates a binary operator (+, -, *, /, comparisons).
     * If either side contains aggregation, adds Eval on top to compute the binary operation.
     */
    private TranslationResult translateBinaryOperator(VectorBinaryOperator binaryOp, LogicalPlan currentPlan, TranslationContext ctx) {
        TranslationResult leftResult = translateNode(binaryOp.left(), currentPlan, ctx);
        Expression leftExpr = new ToDouble(leftResult.expression().source(), leftResult.expression());

        if (binaryOp instanceof VectorBinaryComparison comp && comp.filterMode()) {
            return new TranslationResult(leftResult.plan(), leftExpr, leftResult.pendingFilter(), leftResult.synthesizedAttributes());
        }

        TranslationResult rightResult = translateNode(binaryOp.right(), currentPlan, ctx);
        Expression rightExpr = new ToDouble(rightResult.expression().source(), rightResult.expression());

        Expression binaryExpr = binaryOp.binaryOp()
            .asFunction()
            .create(binaryOp.source(), leftExpr, rightExpr, ctx.analyzerContext().configuration());

        boolean leftAgg = findAggregate(leftResult.plan(), Aggregate.class) != null;
        boolean rightAgg = findAggregate(rightResult.plan(), Aggregate.class) != null;

        // Both aggregated -> fold into one; otherwise take the aggregated side's plan.
        LogicalPlan resultPlan;
        if (leftAgg && rightAgg) {
            resultPlan = foldBinaryOperatorAggregate(leftResult, rightResult);
        } else if (leftAgg) {
            resultPlan = leftResult.plan();
        } else {
            resultPlan = rightResult.plan();
        }

        Expression pendingFilter = null;
        if (!leftAgg || !rightAgg) {
            pendingFilter = combineAndNullable(Arrays.asList(leftResult.pendingFilter(), rightResult.pendingFilter()));
        }

        // TODO: vector matching (on/ignoring) https://github.com/elastic/elasticsearch/issues/142596
        SynthesizedAttributes shape = leftResult.synthesizedAttributes().hasDeclared()
            ? leftResult.synthesizedAttributes()
            : rightResult.synthesizedAttributes();

        if (findAggregate(resultPlan, Aggregate.class) != null) {
            Alias evalAlias = new Alias(binaryExpr.source(), ctx.promqlCommand().valueColumnName(), binaryExpr);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), resultPlan, List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute(), pendingFilter, shape);
        }

        return new TranslationResult(resultPlan, binaryExpr, pendingFilter, shape);
    }

    /**
     * Fold left and right aggregates into a single plan.
     */
    private static LogicalPlan foldBinaryOperatorAggregate(TranslationResult left, TranslationResult right) {
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
            // TODO: different groupings need vector matching https://github.com/elastic/elasticsearch/issues/142596
            boolean areGroupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                && leftGroupingNames.equals(rightGroupingNames);

            if (areGroupingsCompatible == false) {
                // Vector matching (on/ignoring modifiers) is not yet supported; surface as a 400 rather than a 500.
                // See https://github.com/elastic/elasticsearch/issues/142596
                throw new VerificationException(
                    "Binary expressions between vectors with different grouping keys are not supported yet. "
                        + "Left groupings: {}, right groupings: {}",
                    leftGroupingNames,
                    rightGroupingNames
                );
            }

            // Merge aggregates; each side keeps its own filter condition.
            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.pendingFilter()));
            uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.pendingFilter()));

            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                Expression inner = e;
                if (e instanceof Alias a) {
                    inner = a.child();
                }
                // Rename it to avoid conflicting output names
                return new Alias(e.source(), names.next(e.name()), inner, e.id());
            }).toList();

            return leftAgg.with(leftAgg.child(), leftAgg.groupings(), newAggregates);
        });

        // Replay right side's Evals on top of merged aggregate.
        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

    /**
     * Translates a selector (instant, range, or literal).
     * Adds label filter conditions to the context.
     */
    private TranslationResult translateSelector(Selector selector, LogicalPlan currentPlan, TranslationContext ctx) {
        PromqlCommand cmd = ctx.promqlCommand();

        LogicalPlan foldedPlan = PromqlLogicalPlanBuilder.tryFoldRelation(cmd, currentPlan);

        Expression matcherCondition = translateLabelMatchers(
            selector.source(),
            selector.labels(),
            selector.labelMatchers(),
            ctx.analyzerContext().configuration()
        );

        if (selector instanceof LiteralSelector literalSelector) {
            if (foldedPlan != null) {
                return new TranslationResult(foldedPlan, literalSelector.literal(), matcherCondition, SynthesizedAttributes.none(), true);
            }
            return new TranslationResult(currentPlan, literalSelector.literal(), matcherCondition, SynthesizedAttributes.none());
        }

        if (foldedPlan != null) {
            var empty = new LocalRelation(cmd.source(), List.of(cmd.valueAttribute(), cmd.stepAttribute()), EmptyLocalSupplier.EMPTY);
            return new TranslationResult(empty, Literal.NULL, null, SynthesizedAttributes.none(), true);
        }

        Expression expr;
        if (selector instanceof InstantSelector) {
            // InstantSelector maps to LastOverTime to get latest sample per time series
            expr = new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, ctx.time());
        } else {
            expr = selector.series();
        }

        // Non-literal selectors reflect the inheritedAttributes back up; EsRelation must have the required dimensions.
        SynthesizedAttributes shape = ctx.inheritedAttributes().reflect();

        return new TranslationResult(currentPlan, expr, matcherCondition, shape);
    }

    /**
     * Walks through UnaryPlan wrappers (Eval, Project, Filter, etc.) to find the nearest
     * aggregate of the given type. Returns null if none is found.
     */
    private static <T extends Aggregate> T findAggregate(LogicalPlan plan, Class<T> type) {
        if (type.isInstance(plan)) {
            return type.cast(plan);
        }
        if (plan instanceof UnaryPlan unary) {
            return findAggregate(unary.child(), type);
        }
        return null;
    }

    /** Build the aggregate function (sum, max, etc.) from the PromQL registry. */
    private static Expression createAggregateExpression(AcrossSeriesAggregate agg, Expression inputValue, TranslationContext ctx) {
        PromqlFunctionRegistry.PromqlContext promqlCtx = new PromqlFunctionRegistry.PromqlContext(
            ctx.time(),
            AggregateFunction.NO_WINDOW,
            ctx.stepAttr(),
            ctx.analyzerContext().configuration()
        );
        return agg.buildEsqlFunction(inputValue, promqlCtx);
    }

    private static LogicalPlan createInnermostAggregatePlan(
        TranslationContext ctx,
        LogicalPlan plan,
        SynthesizedAttributes synthesizedAttributes,
        List<Attribute> pathExclusions,
        Expression agg
    ) {
        PromqlCommand command = ctx.promqlCommand();
        Source source = command.promqlPlan().source();

        // Innermost aggregate: its child is the leaf selector, so it owns the physical `_timeseries` grouping and
        // translates against its own scope, excluding every dimension dropped on the way down (the inheritedAttributes's path
        // exclusions).
        var translation = synthesizedAttributes.translateLeaf(pathExclusions);

        // Empty `without ()` retains the full label set T: translateLeaf surfaces a `_timeseries` grouping key for it
        // (with an empty exclusion set), so hasTSGrouping already covers it - no separate full-label-set signal needed.
        boolean needsTimeSeriesGrouping = hasTSGrouping(translation.groupings()) || translation.excludedDimensions().isEmpty() == false;
        // TranslateTimeSeriesAggregate splits this node into two phases, replacing inner
        // TimeSeriesAggregateFunctions (e.g. LastOverTime) with references to phase-1 results.
        // The phase-2 expression must remain a valid AggregateFunction inside the Aggregate node.
        // Sum(LastOverTime(m)) -> Sum(ref) -- Sum survives, no wrap needed
        // LastOverTime(m) -> ref -- bare ref, needs Values(ref)
        // Mul(LastOverTime(m), 8) -> Mul(ref, 8) -- not an agg, needs Values(Mul(ref,8))
        // Guarded by needsTimeSeriesGrouping because without dimension grouping (e.g. constants
        // like vector(5)) TranslateTimeSeriesAggregate passes Literals straight to phase 1.
        boolean wrapWithValues = (agg instanceof AggregateFunction == false) || (agg instanceof TimeSeriesAggregateFunction);
        if (needsTimeSeriesGrouping && wrapWithValues) {
            agg = new Values(agg.source(), agg);
        }

        var groupings = new ArrayList<Expression>();
        groupings.add(ctx.stepBucketAlias());

        var aggregates = new ArrayList<NamedExpression>();
        aggregates.add(new Alias(agg.source(), command.valueColumnName(), agg));
        aggregates.add(ctx.stepAttr());

        if (needsTimeSeriesGrouping) {
            // Resolve each excluded label to the concrete dimension column the child exposes, so the exclusion names
            // match the backing dimension fields the `_timeseries` block loader enumerates: PromQL `pod` must become the
            // stored dimension `labels.pod` for Prometheus passthrough data, otherwise the bare key never matches and
            // the label leaks into the output. A label absent from the child stays unresolved and is simply a no-op.
            List<Expression> excluded = new ArrayList<>(translation.excludedDimensions().size());
            for (Attribute label : translation.excludedDimensions()) {
                Attribute resolved = findAttributeByLabelName(plan.output(), canonicalName(label));
                excluded.add(resolved != null ? resolved : label);
            }
            var function = new TimeSeriesWithout(source, excluded);
            var expression = function.createNamedExpression();
            groupings.add(expression);
            aggregates.add(expression.toAttribute());
        }

        // Add non `_timeseries` keys as passthrough groupings and aggregates
        for (Attribute key : translation.groupings()) {
            if (isTimeSeriesAttributeName(key.name()) == false) {
                groupings.add(key);
                aggregates.add(key);
            }
        }

        return new TimeSeriesAggregate(source, plan, groupings, aggregates, null, ctx.time(), TimeSeriesAggregate.Origin.PROMQL_COMMAND);
    }

    private static boolean hasTSGrouping(List<Attribute> groupings) {
        return groupings.stream().anyMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
    }

    /** Outer aggregation over an already-aggregated child. */
    private static LogicalPlan createOuterAggregatePlan(
        TranslationContext ctx,
        LogicalPlan plan,
        SynthesizedAttributes labels,
        Expression aggExpr
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        NamedExpression value = new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr);

        var translation = labels.translate(plan.output());

        if (labels.hasExclusions()) {
            // A WITHOUT regroup (whether the child exposes `_timeseries` or only concrete labels) must pack its carried
            // dimensions before aggregating: a multi-valued dimension would otherwise split the row and double-count.
            // Pack all carried dimensions before the aggregate to prevent multi-valued splitting,
            // then Unpack after. This covers keys (_timeseries or concrete), attributes (labels
            // to pass through), and missing labels (null-synthesized for BY over WITHOUT).
            // TODO: TranslateTimeSeriesAggregate independently unpacks the inner TSA's dimensions,
            // then we re-pack them here. A TSA flag to skip its unpack would eliminate this redundant cycle.

            Source source = ctx.promqlCommand().source();
            Attribute step = ctx.stepAttr();

            // Null-synthesize missing labels before packing
            List<Alias> nullAliases = new ArrayList<>();
            if (translation.absent().isEmpty() == false) {
                for (var missing : translation.absent()) {
                    nullAliases.add(nullAlias(missing));
                }
                plan = new Eval(source, plan, nullAliases);
            }

            // Build the full list of attributes to pack: keys + attributes + missing
            List<Attribute> allToPack = new ArrayList<>();
            allToPack.addAll(translation.groupings());
            allToPack.addAll(translation.passthrough());
            for (Alias na : nullAliases) {
                allToPack.add(na.toAttribute());
            }

            List<Alias> packAliases = new ArrayList<>();
            List<Expression> groupings = new ArrayList<>();
            groupings.add(step);
            List<NamedExpression> aggKeys = new ArrayList<>();
            List<Alias> unpackAliases = new ArrayList<>();
            var names = new TemporaryNameGenerator.Monotonic();

            for (Attribute attr : allToPack) {
                Alias pack = new Alias(source, names.next(attr.name()), new PackDimension(source, attr));
                packAliases.add(pack);
                groupings.add(pack.toAttribute());
                aggKeys.add(pack.toAttribute());

                unpackAliases.add(
                    new Alias(source, attr.name(), new UnpackDimension(source, pack.toAttribute(), attr.dataType()), attr.id())
                );
            }

            Eval packEval = new Eval(source, plan, packAliases);

            var aggregates = new ArrayList<NamedExpression>(aggKeys.size() + 2);
            aggregates.add(value);
            aggregates.add(step);
            aggregates.addAll(aggKeys);

            Aggregate agg = new Aggregate(source, packEval, groupings, aggregates);
            Eval unpackEval = new Eval(source, agg, unpackAliases);

            List<NamedExpression> projections = new ArrayList<>();
            projections.add(value.toAttribute());
            projections.add(step);
            for (Alias unpack : unpackAliases) {
                projections.add(unpack.toAttribute());
            }
            return new Project(source, unpackEval, projections);
        }

        // BY/NONE over already-aggregated child: group by concrete labels only.
        // _timeseries is NOT a grouping key; concrete labels from attributes become keys.
        var keys = new ArrayList<NamedExpression>();
        for (var key : translation.groupings()) {
            if (isTimeSeriesAttributeName(key.name()) == false) {
                keys.add(key);
            }
        }
        keys.addAll(translation.passthrough());
        if (translation.absent().isEmpty() == false) {
            List<Alias> missingAliases = new ArrayList<>();
            for (var missing : translation.absent()) {
                // Surface a passthrough label that exists under its backing field name (labels.X -> X); only truly
                // absent labels are null-filled. Mirrors Prometheus, where a BY label not present is dropped/empty.
                var alias = aliasExistingPromqlLabel(promqlCommand.source(), plan, missing);
                missingAliases.add(alias);
                keys.add(alias.toAttribute());
            }
            plan = new Eval(promqlCommand.source(), plan, missingAliases);
        }

        var step = ctx.stepAttr();
        var groupings = new ArrayList<Expression>(keys.size() + 1);
        groupings.add(step);
        groupings.addAll(keys);

        var aggregates = new ArrayList<NamedExpression>(keys.size() + 2);
        aggregates.add(value);
        aggregates.add(step);
        aggregates.addAll(keys);

        return new Aggregate(ctx.promqlCommand().source(), plan, groupings, aggregates);
    }

    private static Alias nullAlias(Attribute attribute) {
        var nullLiteral = new Literal(attribute.source(), null, attribute.resolved() ? attribute.dataType() : DataType.KEYWORD);
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
    }

    private static Alias aliasExistingPromqlLabel(Source source, LogicalPlan plan, Attribute attribute) {
        Attribute existing = findAttributeByLabelName(plan.output(), canonicalName(attribute));
        return existing == null ? nullAlias(attribute) : new Alias(source, attribute.name(), existing, attribute.id());
    }

    /** Push label filter down to EsRelation level */
    private static LogicalPlan applyLabelFilter(LogicalPlan plan, Expression filterCondition, PromqlCommand promqlCommand) {
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof Filter f && f.child() instanceof EsRelation) {
                // Already has a filter on EsRelation, combine conditions
                return new Filter(f.source(), f.child(), new And(f.source(), f.condition(), filterCondition));
            } else if (p instanceof EsRelation) {
                return new Filter(promqlCommand.source(), p, filterCondition);
            }
            return p;
        });
    }

    /**
     * Filter out rows with null values in the aggregation result.
     * This is necessary to match PromQL semantics where series with missing data are dropped.
     * Note that this doesn't filter out label columns that are null, as those are valid in PromQL.
     * Example: {@code sum by (unknown_label) (metric)} is equivalent to {@code sum(metric)} in PromQL.
     * <p>
     * The filter targets the value column by name rather than by position: the value column is first in the
     * projected command output but last in a freshly translated branch body (a value-shadowing {@code Eval}
     * moves it to the end), and the union path filters before projecting.
     */
    private static LogicalPlan applyNullOutputFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        Attribute valueAttr = null;
        for (Attribute attr : plan.output()) {
            if (attr.name().equals(promqlCommand.valueColumnName())) {
                valueAttr = attr;
                break;
            }
        }
        assert valueAttr != null : "value column [" + promqlCommand.valueColumnName() + "] missing from plan output " + plan.output();
        return new Filter(promqlCommand.source(), plan, new IsNotNull(valueAttr.source(), valueAttr));
    }

    /**
     * Applies the projection (output) filter on the {@code step} column. Step labels are anchored at {@code start} and are
     * offset-independent, so this is shared across all branches and applied once at the command level.
     */
    private static LogicalPlan withTimestampFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        Literal start = promqlCommand.start();
        Literal end = promqlCommand.end();
        var source = promqlCommand.source();
        var step = promqlCommand.stepAttribute();

        var lo = new GreaterThanOrEqual(source, step, start.value() != null ? start : Literal.dateTime(source, EPOCH_MIN));
        var hi = new LessThanOrEqual(source, step, end.value() != null ? end : Literal.dateTime(source, EPOCH_MAX));
        return new Filter(source, plan, new And(source, lo, hi));
    }

    /** Comparison filter (e.g., metric > x) */
    private LogicalPlan addComparisonFilter(
        LogicalPlan plan,
        VectorBinaryComparison binaryComparison,
        AnalyzerContext context,
        Expression left
    ) {
        ToDouble right = new ToDouble(binaryComparison.right().source(), ((LiteralSelector) binaryComparison.right()).literal());
        Function condition = binaryComparison.op().asFunction().create(binaryComparison.source(), left, right, context.configuration());
        return new Filter(binaryComparison.source(), plan, condition);
    }

    /** Ensure value column is double */
    private static LogicalPlan applyValueToDoubleConversion(
        PromqlCommand promqlCommand,
        LogicalPlan plan,
        Expression valueExpr,
        NameId valueId
    ) {
        Source source = promqlCommand.source();

        if ((valueExpr instanceof Attribute == false && valueExpr.resolved() && valueExpr.dataType() == DataType.DOUBLE) == false) {
            valueExpr = new ToDouble(source, valueExpr);
        }

        var value = new Alias(source, promqlCommand.valueColumnName(), valueExpr, valueId);
        return new Eval(source, plan, List.of(value));
    }

    /**
     * Builds the {@code step} bucket alias for a branch: the {@link TStep} grouping key shared across all aggregation
     * groupings, derived from the (possibly offset-shifted) {@code timestamp}. {@code stepId} names the synthetic column.
     * <p>
     * Because the bucket reads the evaluation timestamp, an {@code offset} shifts which samples fall into each fixed
     * output bucket without moving the buckets themselves.
     */
    private static Alias createStepBucketAlias(
        PromqlCommand p,
        Configuration configuration,
        NameId stepId,
        Expression timestampExpression
    ) {
        Expression size;
        Expression start;
        Expression end;
        if (p.isInstantQuery()) {
            size = Literal.timeDuration(p.source(), p.resolveInstantQueryWindow());
            start = new Sub(p.source(), p.start(), size, configuration);
            end = p.end();
        } else {
            size = p.resolveTimeBucketSize();
            start = p.start().value() != null ? p.start() : Literal.dateTime(p.source(), EPOCH_MIN);
            end = p.end().value() != null ? p.end() : Literal.dateTime(p.source(), EPOCH_MAX);
        }

        var tstep = new TStep(size.source(), size, start, end, timestampExpression, configuration);
        return new Alias(tstep.source(), p.stepColumnName(), tstep, stepId);
    }

    private static boolean canCreateStepBucket(PromqlCommand cmd) {
        if (cmd.timestamp() == null || cmd.timestamp().resolved() == false) {
            if (cmd.isRangeQuery() && cmd.buckets() != null && cmd.buckets().value() != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Translates PromQL label matchers into ESQL filter expressions.
     * <p>
     * Uses AutomatonUtils to detect optimizable patterns:
     * - Exact match -> field == "value"
     * - Prefix pattern (prefix.*) -> field STARTS_WITH "prefix"
     * - Suffix pattern (.*suffix) -> field ENDS_WITH "suffix"
     * - Simple alternation (a|b|c) -> field IN ("a", "b", "c")
     * - Disjoint prefixes -> field STARTS_WITH "p1" OR field STARTS_WITH "p2"
     * - Disjoint suffixes -> field ENDS_WITH "s1" OR field ENDS_WITH "s2"
     * - Complex patterns -> field RLIKE "pattern"
     *
     * @param source        the source location for error reporting
     * @param labelMatchers the PromQL label matchers to translate
     * @return an ESQL Expression combining all label matcher conditions with AND
     */
    private static Expression translateLabelMatchers(
        Source source,
        List<Expression> fields,
        LabelMatchers labelMatchers,
        Configuration config
    ) {
        var matchers = labelMatchers.matchers();
        // optimization for literal selectors that don't have label matchers
        if (matchers.isEmpty()) {
            return null;
        }
        List<Expression> conditions = new ArrayList<>();
        boolean hasNameMatcher = false;
        for (int i = 0, s = matchers.size(); i < s; i++) {
            LabelMatcher matcher = matchers.get(i);
            // special handling for name label
            if (LabelMatcher.NAME.equals(matcher.name())) {
                hasNameMatcher = true;
            } else {
                Expression field = fields.get(hasNameMatcher ? i - 1 : i); // adjust index if name matcher was seen
                if (field.resolved() && DataType.isString(field.dataType()) == false) {
                    field = new ToString(field.source(), field, config);
                }
                Expression condition = translateLabelMatcher(source, field, matcher);
                if (condition != null) {
                    conditions.add(condition);
                }
            }
        }

        // could happen in case of an optimization that removes all matchers
        if (conditions.isEmpty()) {
            return null;
        }

        return combineAnd(conditions);
    }

    /**
     * Translates a single PromQL label matcher into an ESQL filter expression.
     *
     * @param source the source location
     * @param matcher the label matcher to translate
     * @return the ESQL Expression, or null if the matcher matches all or none
     */
    public static Expression translateLabelMatcher(Source source, Expression field, LabelMatcher matcher) {
        // Check for universal matchers
        if (matcher.matchesAll()) {
            return Literal.fromBoolean(source, true); // No filter needed (matches everything)
        }

        if (matcher.matchesNone()) {
            // This is effectively FALSE - could use a constant false expression
            return Literal.fromBoolean(source, false);
        }

        Expression condition;

        if (matcher.isMultiValue()) {
            var expressions = new ArrayList<Expression>(matcher.values().size());
            if (matcher.matcher().isRegex()) {
                // Each value is a regex, combine with OR
                for (var v : matcher.values()) {
                    expressions.add(new RLike(source, field, new RLikePattern(v)));
                }
                condition = Predicates.combineOr(expressions);
            } else {
                // Each value is a plain literal, match exact with the IN clause
                for (var v : matcher.values()) {
                    expressions.add(Literal.keyword(source, v));
                }
                condition = new In(source, field, expressions);
            }
            if (matcher.isNegation()) {
                condition = new Not(source, condition);
            }
        } else {
            // Single value exact match
            var m = AutomatonUtils.matchesExact(matcher.automaton());
            if (m != null) {
                condition = new Equals(source, field, Literal.keyword(source, m));
            } else {
                // Try to extract disjoint patterns (handles mixed prefix/suffix/exact)
                var fragments = AutomatonUtils.extractFragments(matcher.getFirstValue());
                if (fragments != null && fragments.isEmpty() == false) {
                    condition = translateDisjointPatterns(source, field, fragments);
                } else {
                    // Fallback to RLIKE with the full automaton pattern
                    // Note: We need to ensure the pattern is properly anchored for PromQL semantics
                    condition = new RLike(source, field, new RLikePattern(matcher.getFirstValue()));
                }
                if (matcher.isNegation()) {
                    condition = new Not(source, condition);
                }
            }
        }

        // PromQL spec: absent labels are treated as having value "". If the matcher's automaton
        // accepts the empty string (e.g. {label=""} or {label!="foo"}), series where the label
        // field is NULL (absent) must also match.
        if (matcher.matchesEmpty()) {
            condition = Predicates.combineOr(List.of(new IsNull(source, field), condition));
        }
        return condition;
    }

    /**
     * Translates disjoint pattern fragments into optimized ESQL expressions.
     * <p>
     * Homogeneous patterns (all the same type):
     * - All EXACT -> field IN ("a", "b", "c")
     * - All PREFIX -> field STARTS_WITH "p1" OR field STARTS_WITH "p2" ...
     * - All SUFFIX -> field ENDS_WITH "s1" OR field ENDS_WITH "s2" ...
     * <p>
     * Heterogeneous patterns:
     * - Mixed -> (field == "exact") OR (field STARTS_WITH "prefix") OR (field ENDS_WITH "suffix") OR (field RLIKE "regex")
     * <p>
     * Fragments are sorted by type for optimal query execution order:
     * 1. EXACT (most selective, can use IN clause)
     * 2. PREFIX (index-friendly)
     * 3. SUFFIX (index-friendly)
     * 4. REGEX (least selective, fallback)
     *
     * @param source the source location
     * @param field the field attribute
     * @param fragments the list of pattern fragments
     * @return the ESQL Expression combining all fragments
     */
    private static Expression translateDisjointPatterns(Source source, Expression field, List<AutomatonUtils.PatternFragment> fragments) {
        // Sort fragments by type priority using enum ordinal: EXACT -> PREFIX -> SUFFIX -> REGEX
        List<AutomatonUtils.PatternFragment> sortedFragments = new ArrayList<>(fragments);
        sortedFragments.sort(Comparator.comparingInt(a -> a.type().ordinal()));

        // Check if all fragments are of the same type
        AutomatonUtils.PatternFragment.Type firstType = sortedFragments.getFirst().type();
        boolean homogeneous = true;
        for (AutomatonUtils.PatternFragment fragment : sortedFragments) {
            if (fragment.type() != firstType) {
                homogeneous = false;
                break;
            }
        }

        if (homogeneous && firstType == AutomatonUtils.PatternFragment.Type.EXACT) {
            // Optimize to IN clause
            List<Expression> values = new ArrayList<>(sortedFragments.size());
            for (AutomatonUtils.PatternFragment fragment : sortedFragments) {
                values.add(Literal.keyword(source, fragment.value()));
            }
            return new In(source, field, values);
        }

        // For non-exact homogeneous or heterogeneous patterns, create OR of conditions
        List<Expression> conditions = new ArrayList<>(sortedFragments.size());
        for (AutomatonUtils.PatternFragment fragment : sortedFragments) {
            Expression condition = translatePatternFragment(source, field, fragment);
            conditions.add(condition);
        }

        // Combine with OR
        return Predicates.combineOr(conditions);
    }

    /**
     * Translates a single pattern fragment into an ESQL expression.
     */
    private static Expression translatePatternFragment(Source source, Expression field, AutomatonUtils.PatternFragment fragment) {
        Literal value = Literal.keyword(source, fragment.value());
        return switch (fragment.type()) {
            case EXACT -> new Equals(source, field, value);
            case PREFIX -> new StartsWith(source, field, value);
            case PROPER_PREFIX -> new And(source, new NotEquals(source, field, value), new StartsWith(source, field, value));
            case SUFFIX -> new EndsWith(source, field, value);
            case PROPER_SUFFIX -> new And(source, new NotEquals(source, field, value), new EndsWith(source, field, value));
            case REGEX -> new RLike(source, field, new RLikePattern(fragment.value()));
        };
    }
}
