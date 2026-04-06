/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TemporaryNameGenerator;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.parser.promql.PromqlLogicalPlanBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.time.Duration;
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
public final class TranslatePromqlToEsqlPlan extends OptimizerRules.ParameterizedOptimizerRule<PromqlCommand, LogicalOptimizerContext> {

    // TODO make configurable via lookback_delta parameter and (cluster?) setting
    public static final Duration DEFAULT_LOOKBACK = Duration.ofMinutes(5);

    public TranslatePromqlToEsqlPlan() {
        super(OptimizerRules.TransformDirection.UP);
    }

    /** Result flows upward */
    private record TranslationResult(
        /* ESQL plan built so far. */
        LogicalPlan plan,
        /* Reference to this node's numeric value, composed into parent expressions. */
        Expression expression,
        /* Label matcher flows up until an aggregate that folds it / push to relation */
        Expression pendingFilter,
        /* What aggregate labels the child subtree exposes */
        LabelSetSpec labelSetSpec
    ) {
        TranslationResult(LogicalPlan plan, Expression expression) {
            this(plan, expression, null, LabelSetSpec.none());
        }

        TranslationResult(LogicalPlan plan, Expression expression, Expression selectorFilter) {
            this(plan, expression, selectorFilter, LabelSetSpec.none());
        }

    }

    /** Context flows downward */
    private record TranslationContext(
        /* The root PromQL command. */
        PromqlCommand promqlCommand,
        /* Optimizer context (configuration, transport version, etc.). */
        LogicalOptimizerContext optimizerContext,
        /* Alias for the step bucket expression used in all aggregation groupings. */
        Alias stepBucketAlias,
        /*  What aggregate labels the child subtree MUST expose */
        LabelSetSpec labelSetSpec
    ) {
        Attribute stepAttr() {
            return stepBucketAlias.toAttribute();
        }

    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand, LogicalOptimizerContext context) {
        Alias stepBucketAlias = createStepBucketAlias(promqlCommand);

        // Base plan EsRelation with timestamp filter
        LogicalPlan basePlan = withTimestampFilter(promqlCommand, promqlCommand.child());

        TranslationContext ctx = new TranslationContext(promqlCommand, context, stepBucketAlias, LabelSetSpec.none());

        TranslationResult result = translateNode(promqlCommand.promqlPlan(), basePlan, ctx);

        var plan = result.plan();
        var valueExpr = result.expression();
        var filter = result.pendingFilter();

        if (filter != null) {
            plan = applyLabelFilter(plan, filter, promqlCommand);
        }

        // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
        // TODO: If we ever support metric references without last_over_time, we could
        // skip TimeSeriesAggregate and use plain Aggregate instead (see #141501 discussion).
        if (findAggregate(plan, Aggregate.class) == null) {
            plan = createInnermostAggregatePlan(ctx, plan, LabelSetSpec.of(promqlCommand.promqlPlan().output()), valueExpr);
            valueExpr = getValueOutput(plan);
        }

        if (promqlCommand.promqlPlan() instanceof VectorBinaryComparison binaryComparison && binaryComparison.filterMode()) {
            // for comparison with the filtering mode, return the left operand and apply filter later
            plan = addComparisonFilter(plan, binaryComparison, context);
        }

        plan = applyValueToDoubleConversion(promqlCommand, plan, valueExpr);

        plan = applyProjection(promqlCommand, plan);

        plan = applyNullOutputFilter(promqlCommand, plan);

        return plan;
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
     * We avoid that at plan time (for performance), so the translator carries aggregation shape as a triplet
     * {@code (G, O, X)} where G = grouping labels, O = output labels, X = excluded labels.
     * G is either a concrete set of label names or an opaque runtime representation {@code T...} (backed by _timeseries).
     * Labels in O but not in G are null-filled in the output.
     * <ul>
     *   <li>WITHOUT(E): (G, O, X) -> (G\E, G\E, X u E)</li>
     *   <li>BY(W): (G, O, X) -> (W\X, W, X)</li>
     * </ul>
     * <p>
     * Leaf state is {@code [G=T..., O=T..., X={}]} (all labels present but unknown by name).
     * <p>
     * Examples:
     *
     * <pre>
     * sum without(pod) (
     *   avg without(region) (
     *     cpu_util
     *   ) [G=T...\{region}, O=T...\{region}, X={region}]
     * ) [G=T...\{region,pod}, O=T...\{region,pod}, X={region,pod}]
     * </pre>
     *
     * <pre>
     * sum by(cluster,region) (
     *   avg without(region) (
     *     cpu_util
     *   ) [G=T...\{region}, O=T...\{region}, X={region}]
     * ) [G={cluster}, O={cluster,region}, X={region}]   // region is null-filled
     * </pre>
     *
     * <pre>
     * max without(cluster) (
     *   sum by(cluster,region) (
     *     avg without(region) (
     *       cpu_util
     *     ) [G=T...\{region}, O=T...\{region}, X={region}]
     *   ) [G={cluster}, O={cluster,region}, X={region}] // region is null-filled
     * ) [G={}, O={}, X={region,cluster}]
     * </pre>
     *
     * <pre>
     * sum without(pod) (
     *   avg by(cluster,pod) (
     *     cpu_util
     *   ) [G={cluster,pod}, O={cluster,pod}, X={}]
     * ) [G={cluster}, O={cluster}, X={pod}]
     * </pre>
     * <pre>
     * sum by(cluster) (
     *   avg by(cluster,pod) (
     *     cpu_util
     *   ) [G={cluster,pod}, O={cluster,pod}, X={}]
     * ) [G={cluster}, O={cluster}, X={}]
     * </pre>
     *
     * <p>Only {@code AcrossSeriesAggregate} creates plan-level aggregation nodes.
     * {@code WithinSeriesAggregate} and other {@code PromqlFunctionCall} nodes are
     * lowered to expressions and folded into the aggregate.
     */
    private TranslationResult translateAcrossSeriesAggregate(AcrossSeriesAggregate agg, LogicalPlan currentPlan, TranslationContext ctx) {
        LabelSetSpec importAggregateLabels = switch (agg.grouping()) {
            case BY -> LabelSetSpec.of(agg.groupings(), ctx.labelSetSpec.excluded());
            case WITHOUT -> LabelSetSpec.without(ctx.labelSetSpec, agg.groupings());
            case NONE -> LabelSetSpec.none();
        };

        TranslationContext childCtx = new TranslationContext(
            ctx.promqlCommand,
            ctx.optimizerContext,
            ctx.stepBucketAlias,
            importAggregateLabels
        );
        TranslationResult childResult = translateNode(agg.child(), currentPlan, childCtx);

        LabelSetSpec exportAggregateLabels = switch (agg.grouping()) {
            case BY -> LabelSetSpec.by(childResult.labelSetSpec, agg.output());
            case WITHOUT -> LabelSetSpec.without(childResult.labelSetSpec, agg.groupings());
            case NONE -> LabelSetSpec.none();
        };

        var aggExpression = createAggregateExpression(agg, childResult.expression(), ctx);

        var resultPlan = findAggregate(childResult.plan(), Aggregate.class) != null
            /* child already aggregated, no additional `_tsid` grouping needed */
            ? createOuterAggregatePlan(ctx, childResult.plan(), exportAggregateLabels, aggExpression)
            /* group by _tsid, timestamp and compute optional aggregates, e.g., avg_over_time() */
            : createInnermostAggregatePlan(ctx, childResult.plan(), exportAggregateLabels, aggExpression);

        return new TranslationResult(resultPlan, getValueOutput(resultPlan), childResult.pendingFilter(), exportAggregateLabels);
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

        LogicalPlan timeSeriesAgg = createInnermostAggregatePlan(ctx, childResult.plan(), LabelSetSpec.none(), scalarExpr);
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

        Expression window = AggregateFunction.NO_WINDOW;
        if (functionCall.child() instanceof RangeSelector rangeSelector) {
            window = rangeSelector.range();
            if (isImplicitRangePlaceholder(window)) {
                window = resolveImplicitRangeWindow(ctx.promqlCommand());
            }
        }

        PromqlFunctionRegistry.PromqlContext promqlCtx = new PromqlFunctionRegistry.PromqlContext(
            ctx.promqlCommand().timestamp(),
            window,
            ctx.stepAttr()
        );

        Expression function = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
            functionCall.functionName(),
            functionCall.source(),
            childResult.expression(),
            promqlCtx,
            functionCall.parameters()
        );

        // This can happen when trying to provide a counter to a function that doesn't support it e.g. avg_over_time on a counter
        // This is essentially a bug since this limitation doesn't exist in PromQL itself.
        // Throwing an error here to avoid generating invalid plans with obscure errors downstream.
        Expression.TypeResolution typeResolution = function.typeResolved();
        if (typeResolution.unresolved()) {
            throw new QlIllegalArgumentException("Could not resolve type for function [{}]: {}", function, typeResolution.message());
        }

        // Wrap already aggregated child in Eval
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            Alias evalAlias = new Alias(function.source(), ctx.promqlCommand().valueColumnName(), function);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), childResult.plan(), List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute(), childResult.pendingFilter(), childResult.labelSetSpec());
        }

        return new TranslationResult(childResult.plan(), function, childResult.pendingFilter(), childResult.labelSetSpec());
    }

    private static boolean isImplicitRangePlaceholder(Expression range) {
        return range.foldable()
            && range.fold(FoldContext.small()) instanceof Duration duration
            && duration.equals(PromqlLogicalPlanBuilder.IMPLICIT_RANGE_PLACEHOLDER);
    }

    /**
     * Resolves the implicit range placeholder to a concrete duration based on step and scrape interval.
     * The implicit window is calculated as {@code max(step, scrape_interval)}.
     */
    private static Literal resolveImplicitRangeWindow(PromqlCommand promqlCommand) {
        Duration step = foldDuration(resolveTimeBucketSize(promqlCommand), "step");
        Duration scrapeInterval = foldDuration(promqlCommand.scrapeInterval(), "scrape_interval");
        return Literal.timeDuration(promqlCommand.source(), step.compareTo(scrapeInterval) >= 0 ? step : scrapeInterval);
    }

    private static Duration foldDuration(Expression expression, String paramName) {
        if (expression != null && expression.foldable() && expression.fold(FoldContext.small()) instanceof Duration duration) {
            return duration;
        }
        throw new QlIllegalArgumentException("Expected [{}] to be a duration literal, got [{}]", paramName, expression);
    }

    /**
     * Translates a scalar function (time(), etc.).
     * These produce expressions without modifying the plan.
     */
    private TranslationResult translateScalarFunction(ScalarFunction scalarFunction, LogicalPlan currentPlan, TranslationContext ctx) {
        PromqlFunctionRegistry.PromqlContext promqlCtx = new PromqlFunctionRegistry.PromqlContext(
            ctx.promqlCommand().timestamp(),
            null,
            ctx.stepAttr()
        );
        Expression function = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
            scalarFunction.functionName(),
            scalarFunction.source(),
            null,
            promqlCtx,
            List.of()
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
            return new TranslationResult(leftResult.plan(), leftExpr, leftResult.pendingFilter(), leftResult.labelSetSpec());
        }

        TranslationResult rightResult = translateNode(binaryOp.right(), currentPlan, ctx);
        Expression rightExpr = new ToDouble(rightResult.expression().source(), rightResult.expression());

        Expression binaryExpr = binaryOp.binaryOp()
            .asFunction()
            .create(binaryOp.source(), leftExpr, rightExpr, ctx.optimizerContext().configuration());

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
        LabelSetSpec grouping = leftResult.labelSetSpec().declared().isEmpty() == false
            ? leftResult.labelSetSpec()
            : rightResult.labelSetSpec();

        if (findAggregate(resultPlan, Aggregate.class) != null) {
            Alias evalAlias = new Alias(binaryExpr.source(), ctx.promqlCommand().valueColumnName(), binaryExpr);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), resultPlan, List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute(), pendingFilter, grouping);
        }

        return new TranslationResult(resultPlan, binaryExpr, pendingFilter, grouping);
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
                throw new QlIllegalArgumentException("binary expressions with different grouping keys not supported yet");
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
        Expression matcherCondition = translateLabelMatchers(selector.source(), selector.labels(), selector.labelMatchers());
        Expression expr;
        if (selector instanceof LiteralSelector literalSelector) {
            expr = literalSelector.literal();
        } else if (selector instanceof InstantSelector) {
            // InstantSelector maps to LastOverTime to get latest sample per time series
            expr = new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, ctx.promqlCommand().timestamp());
        } else {
            expr = selector.series();
        }

        // Non-literal selectors pass through parent demand; EsRelation must have required dimensions.
        LabelSetSpec grouping = (selector instanceof LiteralSelector) ? LabelSetSpec.none() : ctx.labelSetSpec();

        return new TranslationResult(currentPlan, expr, matcherCondition, grouping);
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
            ctx.promqlCommand().timestamp(),
            AggregateFunction.NO_WINDOW,
            ctx.stepAttr()
        );
        return PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(agg.functionName(), agg.source(), inputValue, promqlCtx, agg.parameters());
    }

    private static LogicalPlan createInnermostAggregatePlan(TranslationContext ctx, LogicalPlan plan, LabelSetSpec labels, Expression agg) {
        PromqlCommand command = ctx.promqlCommand();
        Source source = command.promqlPlan().source();

        // Self-resolution: at the innermost level there is no child aggregation to resolve against.
        // This just splits declared labels into keys (_timeseries) vs attributes (concrete labels).
        // var spec = labels.apply(labels);
        // List<Attribute> all = new ArrayList<>(labels.excluded());
        // all.addAll(ctx.labelSetSpec.excluded());
        // List<Attribute> resolvedExcluded = all.stream().filter(a -> a instanceof FieldAttribute fa &&
        // fa.isDimension()).distinct().toList();

        var spec = labels.withExcluded(ctx.labelSetSpec().excluded()).apply();

        boolean needsTimeSeriesGrouping = hasTSGrouping(spec.includedGroupings()) || spec.excludedGroupings().isEmpty() == false;
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
            var tw = new TimeSeriesWithout(source, new ArrayList<>(spec.excludedGroupings())).toAttribute();
            groupings.add(tw);
            aggregates.add(tw.toAttribute());
        }

        // Add non `_timeseries` keys as passthrough groupings and aggregates
        for (Attribute key : spec.includedGroupings()) {
            if (isTimeSeriesAttributeName(key.name()) == false) {
                groupings.add(key);
                aggregates.add(key);
            }
        }

        return new TimeSeriesAggregate(source, plan, groupings, aggregates, null, command.timestamp());
    }

    private static boolean hasTSGrouping(List<Attribute> groupings) {
        return groupings.stream().anyMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
    }

    /** Outer aggregation over an already-aggregated child. */
    private static LogicalPlan createOuterAggregatePlan(TranslationContext ctx, LogicalPlan plan, LabelSetSpec labels, Expression aggExpr) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        NamedExpression value = new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr);

        var spec = labels.withIncluded(plan.output()).apply();

        if (labels.excluded().isEmpty() == false && hasTSGrouping(spec.includedGroupings())) {
            // Pack all carried dimensions before the aggregate to prevent multi-valued splitting,
            // then Unpack after. This covers keys (_timeseries or concrete), attributes (labels
            // to pass through), and missing labels (null-synthesized for BY over WITHOUT).
            // TODO: TranslateTimeSeriesAggregate independently unpacks the inner TSA's dimensions,
            // then we re-pack them here. A TSA flag to skip its unpack would eliminate this redundant cycle.

            Source source = ctx.promqlCommand().source();
            Attribute step = ctx.stepAttr();

            // Null-synthesize missing labels before packing
            List<Alias> nullAliases = new ArrayList<>();
            if (spec.missingAttributes().isEmpty() == false) {
                for (var missing : spec.missingAttributes()) {
                    nullAliases.add(nullAlias(missing));
                }
                plan = new Eval(source, plan, nullAliases);
            }

            // Build the full list of attributes to pack: keys + attributes + missing
            List<Attribute> allToPack = new ArrayList<>();
            allToPack.addAll(spec.includedGroupings());
            allToPack.addAll(spec.matchedAttributes());
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
        for (var key : spec.includedGroupings()) {
            if (isTimeSeriesAttributeName(key.name()) == false) {
                keys.add(key);
            }
        }
        keys.addAll(spec.matchedAttributes());
        if (spec.missingAttributes().isEmpty() == false) {
            List<Alias> nullAliases = new ArrayList<>();
            for (var missing : spec.missingAttributes()) {
                var alias = nullAlias(missing);
                nullAliases.add(alias);
                keys.add(alias.toAttribute());
            }
            plan = new Eval(promqlCommand.source(), plan, nullAliases);
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
        var nullLiteral = new Literal(attribute.source(), null, attribute.dataType());
        return new Alias(attribute.source(), attribute.name(), nullLiteral, attribute.id());
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
     */
    private static LogicalPlan applyNullOutputFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        return new Filter(promqlCommand.source(), plan, new IsNotNull(plan.output().getFirst().source(), plan.output().getFirst()));
    }

    /** Filter to [start, end] time range */
    private static LogicalPlan withTimestampFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        Literal start = promqlCommand.start();
        Literal end = promqlCommand.end();
        if (start.value() != null && end.value() != null) {
            var source = promqlCommand.source();
            var timestamp = promqlCommand.timestamp();
            var lower = new GreaterThanOrEqual(source, timestamp, start);
            var upper = new LessThanOrEqual(source, timestamp, end);
            plan = new Filter(source, plan, new And(source, lower, upper));
        }
        return plan;
    }

    /** Comparison filter (e.g., metric > x) */
    private LogicalPlan addComparisonFilter(LogicalPlan plan, VectorBinaryComparison binaryComparison, LogicalOptimizerContext context) {
        Attribute left = plan.output().getFirst().toAttribute();
        ToDouble right = new ToDouble(binaryComparison.right().source(), ((LiteralSelector) binaryComparison.right()).literal());
        Function condition = binaryComparison.op().asFunction().create(binaryComparison.source(), left, right, context.configuration());
        return new Filter(binaryComparison.source(), plan, condition);
    }

    /** Ensure value column is double */
    private static LogicalPlan applyValueToDoubleConversion(PromqlCommand promqlCommand, LogicalPlan plan, Expression valueExpr) {
        Alias convertedValue = new Alias(
            promqlCommand.source(),
            promqlCommand.valueColumnName(),
            new ToDouble(promqlCommand.source(), valueExpr),
            promqlCommand.valueId()
        );
        return new Eval(promqlCommand.source(), plan, List.of(convertedValue));
    }

    private static Alias createStepBucketAlias(PromqlCommand promqlCommand) {
        Expression timeBucketSize = resolveTimeBucketSize(promqlCommand);
        Bucket b = new Bucket(
            timeBucketSize.source(),
            promqlCommand.timestamp(),
            timeBucketSize,
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER
        );
        return new Alias(b.source(), promqlCommand.stepColumnName(), b, promqlCommand.stepId());
    }

    private static Expression resolveTimeBucketSize(PromqlCommand promqlCommand) {
        if (promqlCommand.isRangeQuery()) {
            if (promqlCommand.step().value() != null) {
                return promqlCommand.step();
            }
            return resolveAutoStepFromBuckets(promqlCommand);
        }
        // use default lookback for instant queries
        return Literal.timeDuration(promqlCommand.source(), DEFAULT_LOOKBACK);
    }

    private static Literal resolveAutoStepFromBuckets(PromqlCommand promqlCommand) {
        Bucket autoBucket = new Bucket(
            promqlCommand.buckets().source(),
            promqlCommand.timestamp(),
            promqlCommand.buckets(),
            promqlCommand.start(),
            promqlCommand.end(),
            ConfigurationAware.CONFIGURATION_MARKER
        );
        long rangeStart = ((Number) promqlCommand.start().value()).longValue();
        long rangeEnd = ((Number) promqlCommand.end().value()).longValue();
        var rounding = autoBucket.getDateRounding(FoldContext.small(), rangeStart, rangeEnd);
        long roundedStart = rounding.round(rangeStart);
        long nextRoundedValue = rounding.nextRoundingValue(roundedStart);
        return Literal.timeDuration(promqlCommand.source(), Duration.ofMillis(Math.max(1L, nextRoundedValue - roundedStart)));
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
    private static Expression translateLabelMatchers(Source source, List<Expression> fields, LabelMatchers labelMatchers) {
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

        // Try to extract the exact match
        String exactMatch = AutomatonUtils.matchesExact(matcher.automaton());
        if (exactMatch != null) {
            return new Equals(source, field, Literal.keyword(source, exactMatch));
        }

        Expression condition;
        // Try to extract disjoint patterns (handles mixed prefix/suffix/exact)
        List<AutomatonUtils.PatternFragment> fragments = AutomatonUtils.extractFragments(matcher.value());
        if (fragments != null && fragments.isEmpty() == false) {
            condition = translateDisjointPatterns(source, field, fragments);
        } else {
            // Fallback to RLIKE with the full automaton pattern
            // Note: We need to ensure the pattern is properly anchored for PromQL semantics
            condition = new RLike(source, field, new RLikePattern(matcher.toString()));
        }
        if (matcher.isNegation()) {
            condition = new Not(source, condition);
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
