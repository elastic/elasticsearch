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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
    public static final String STEP_COLUMN_NAME = "step";

    public TranslatePromqlToEsqlPlan() {
        super(OptimizerRules.TransformDirection.UP);
    }

    /**
     * Upward-flowing result of translating a single PromQL node.
     */
    private record TranslationResult(
        /* ESQL plan built so far. */
        LogicalPlan plan,
        /* Reference to this node's numeric value, composed into parent expressions. */
        Expression expression,
        /* Selector filter not yet consumed by an aggregate; null when already applied. */
        Expression selectorFilter,
        /* Labels this node exports upward for grouping. */
        List<Attribute> exposedLabels,
        /* Labels excluded by a WITHOUT modifier; empty for BY/NONE. */
        List<Attribute> excludedLabels
    ) {
        TranslationResult(LogicalPlan plan, Expression expression) {
            this(plan, expression, null, List.of(), List.of());
        }

        TranslationResult(LogicalPlan plan, Expression expression, Expression selectorFilter) {
            this(plan, expression, selectorFilter, List.of(), List.of());
        }

        boolean hasExposedLabels() {
            return exposedLabels.isEmpty() == false;
        }

        boolean hasExcludedLabels() {
            return excludedLabels.isEmpty() == false;
        }
    }

    /**
     * Downward-flowing context passed through recursive translation.
     */
    private record TranslationContext(
        /* The root PromQL command being translated. */
        PromqlCommand promqlCommand,
        /* Optimizer context (configuration, transport version, etc.). */
        LogicalOptimizerContext optimizerContext,
        /* Alias for the step bucket expression used in all aggregation groupings. */
        Alias stepBucketAlias,
        /* Concrete labels the parent demands from this subtree. */
        List<Attribute> requiredLabels
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

        // Only `BY` specifies concrete labels the root needs from its child.
        // `WITHOUT` and `NONE` don't require specific labels upfront.
        List<Attribute> requiredLabels = List.of();
        if (promqlCommand.promqlPlan() instanceof AcrossSeriesAggregate agg && agg.grouping() == AcrossSeriesAggregate.Grouping.BY) {
            requiredLabels = normalizeLabels(agg.groupings());
        }

        TranslationContext ctx = new TranslationContext(promqlCommand, context, stepBucketAlias, requiredLabels);

        TranslationResult result = translateNode(promqlCommand.promqlPlan(), basePlan, ctx);

        var plan = result.plan();
        var valueExpr = result.expression();
        var filter = result.selectorFilter();

        if (filter != null) {
            plan = applyLabelFilter(plan, filter, promqlCommand);
        }

        // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
        // TODO: If we ever support metric references without last_over_time, we could
        // skip TimeSeriesAggregate and use plain Aggregate instead (see #141501 discussion).
        if (findAggregate(plan, Aggregate.class) == null) {
            plan = createInnerAggregate(ctx, plan, promqlCommand.promqlPlan().output(), List.of(), valueExpr);
            valueExpr = getValueOutput(plan);
        }

        if (promqlCommand.promqlPlan() instanceof VectorBinaryComparison binaryComparison && binaryComparison.filterMode()) {
            // for comparison with filtering mode, return left operand and apply filter later
            plan = addComparisonFilter(plan, binaryComparison, context);
        }

        plan = convertValueToDouble(promqlCommand, plan, valueExpr);
        // For BY/NONE the output columns are statically known from the AST and can be projected directly.
        // For WITHOUT the concrete labels are only determined during translation (they depend on which labels
        // the data actually has minus the excluded set), so we must resolve them against the translated plan.
        if (result.hasExcludedLabels()) {
            plan = new Project(promqlCommand.source(), plan, resolveOutput(promqlCommand, plan, result));
        } else {
            plan = new Project(promqlCommand.source(), plan, promqlCommand.output());
        }
        plan = filterNulls(promqlCommand, plan);
        return plan;
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
     * Translates an AcrossSeriesAggregate (sum, avg, max, min, count, etc.).
     * Creates TimeSeriesAggregate if child has no aggregation or Aggregate if child already aggregated.
     * NOTE: Only AcrossSeriesAggregate nodes create plan-level aggregation nodes.
     * WithinSeriesAggregate and other PromqlFunctionCall nodes produce
     * expressions embedded inside the aggregation, but do not create Aggregate plan nodes themselves.
     */
    private TranslationResult translateAcrossSeriesAggregate(AcrossSeriesAggregate agg, LogicalPlan currentPlan, TranslationContext ctx) {
        List<Attribute> groupingLabels = normalizeLabels(agg.groupings());
        // Downward demand: what this aggregate requires from its child.
        List<Attribute> requiredByChild = switch (agg.grouping()) {
            /* BY demands its own explicit labels. */
            case BY -> groupingLabels;
            /* WITHOUT passes through what the parent demanded minus the excluded labels (e.g. parent wants {cluster,pod}, WITHOUT(pod) passes {cluster}). */
            case WITHOUT -> difference(ctx.requiredLabels(), groupingLabels);
            /* NONE collapses everything, so no labels are required. */
            case NONE -> List.of();
        };

        TranslationResult childResult = translateNode(
            agg.child(),
            currentPlan,
            new TranslationContext(ctx.promqlCommand, ctx.optimizerContext, ctx.stepBucketAlias, requiredByChild)
        );
        Expression functionExpression = buildAggregateExpression(agg, childResult.expression(), ctx);

        /* Carries the WITHOUT exclusion list for lowering to TimeSeriesWithout; empty for BY/NONE. */
        List<Attribute> excludedLabels = agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT ? groupingLabels : List.of();
        // Upward export: what this aggregate exposes after aggregation.
        List<Attribute> exposedLabels = switch (agg.grouping()) {
            /* BY exports its declared labels. */
            case BY -> normalizeLabels(agg.output());
            /* WITHOUT exports whatever the child exposed minus the excluded labels (e.g. child exposes {cluster,region,pod}, WITHOUT(pod) exposes {cluster,region}). */
            case WITHOUT -> difference(childResult.exposedLabels(), groupingLabels);
            /* NONE collapses to a single series with no labels. */
            case NONE -> List.of();
        };

        LogicalPlan aggregatePlan;
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            // Child already has an aggregate: create outer Aggregate
            aggregatePlan = createOuterAggregate(ctx, childResult.plan(), exposedLabels, excludedLabels, functionExpression);
        } else {
            // No aggregate yet: create the innermost TimeSeriesAggregate, folding within-series
            // function expressions into the aggregation. WITHOUT groups by packed _timeseries and
            // carries only the labels that remain visible after the exclusion; BY/NONE use the
            // AST-declared output labels like upstream.
            List<Attribute> innerLabels = excludedLabels.isEmpty() ? normalizeLabels(agg.output()) : exposedLabels;
            aggregatePlan = createInnerAggregate(ctx, childResult.plan(), innerLabels, excludedLabels, functionExpression);
        }

        return new TranslationResult(
            aggregatePlan,
            getValueOutput(aggregatePlan),
            childResult.selectorFilter(),
            exposedLabels,
            excludedLabels
        );
    }

    /**
     * Translates a ScalarConversionFunction.
     */
    private TranslationResult translateScalarConversion(
        ScalarConversionFunction scalarFunc,
        LogicalPlan currentPlan,
        TranslationContext ctx
    ) {
        TranslationResult childResult = translateNode(scalarFunc.child(), currentPlan, ctx);

        // Foldable: convert to double directly
        if (childResult.expression().foldable()) {
            return new TranslationResult(
                childResult.plan(),
                new ToDouble(scalarFunc.source(), childResult.expression()),
                childResult.selectorFilter()
            );
        }

        // Child aggregated: grouping by step
        // E.g. scalar(sum by (cluster) (metric)))
        Expression scalarExpr = new Scalar(scalarFunc.source(), childResult.expression());
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            // plain Aggregate grouped by step only, collapsing all series into one value per step.
            Alias aggAlias = new Alias(scalarExpr.source(), ctx.promqlCommand().valueColumnName(), scalarExpr);
            LogicalPlan aggregate = createAggregate(ctx, childResult.plan(), aggAlias, List.of(), List.of());
            return new TranslationResult(aggregate, getValueOutput(aggregate), childResult.selectorFilter(), List.of(), List.of());
        }

        LogicalPlan timeSeriesAgg = createInnerAggregate(ctx, childResult.plan(), List.of(), List.of(), scalarExpr);
        return new TranslationResult(timeSeriesAgg, getValueOutput(timeSeriesAgg), childResult.selectorFilter(), List.of(), List.of());
    }

    /**
     * Gets the value output from an aggregate plan.
     */
    private static Expression getValueOutput(LogicalPlan plan) {
        // The value column is always the first output attribute
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

        // If child is aggregate, add Eval to compute function.
        //
        // Example: ceil(sum by (cluster) (foo)) becomes:
        // Eval[ceil(sum_result)]
        // \_ TimeSeriesAggregate[sum_result = sum(x), groupBy=[step, cluster]]
        //
        // This handles both cases when aggregate is TimeSeriesAggregate or more generic Aggregate.
        if (findAggregate(childResult.plan(), Aggregate.class) != null) {
            Alias evalAlias = new Alias(function.source(), ctx.promqlCommand().valueColumnName(), function);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), childResult.plan(), List.of(evalAlias));
            return new TranslationResult(
                evalPlan,
                evalAlias.toAttribute(),
                childResult.selectorFilter,
                childResult.exposedLabels,
                childResult.excludedLabels
            );
        }

        return new TranslationResult(
            childResult.plan(),
            function,
            childResult.selectorFilter,
            childResult.exposedLabels,
            childResult.excludedLabels
        );
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
            return new TranslationResult(
                leftResult.plan(),
                leftExpr,
                leftResult.selectorFilter,
                leftResult.exposedLabels,
                leftResult.excludedLabels
            );
        }

        TranslationResult rightResult = translateNode(binaryOp.right(), currentPlan, ctx);
        Expression rightExpr = new ToDouble(rightResult.expression().source(), rightResult.expression());

        Expression binaryExpr = binaryOp.binaryOp()
            .asFunction()
            .create(binaryOp.source(), leftExpr, rightExpr, ctx.optimizerContext().configuration());

        boolean leftAgg = findAggregate(leftResult.plan(), Aggregate.class) != null;
        boolean rightAgg = findAggregate(rightResult.plan(), Aggregate.class) != null;

        // If both sides have Aggregate, use a new joint Aggregate plan;
        // otherwise, use the plan from the side that has Aggregate.
        // In both cases aggregate expressions participate in the Eval node that wraps the result.
        LogicalPlan resultPlan;
        if (leftAgg && rightAgg) {
            // filters already attached to each side's aggregate functions in foldBinaryOperatorAggregate
            resultPlan = foldBinaryOperatorAggregate(leftResult, rightResult);
        } else if (leftAgg) {
            resultPlan = leftResult.plan();
        } else {
            resultPlan = rightResult.plan();
        }

        Expression pendingFilter = null;
        if (!leftAgg || !rightAgg) {
            pendingFilter = combineAndNullable(Arrays.asList(leftResult.selectorFilter(), rightResult.selectorFilter()));
        }

        // Inherit grouping state from whichever operand carries labels (e.g. vector side of vector+scalar).
        // When both sides expose labels, left wins; full vector matching (on/ignoring) is not yet supported.
        // TODO: https://github.com/elastic/elasticsearch/issues/142596
        TranslationResult groupingSource = leftResult.hasExposedLabels() ? leftResult : rightResult;

        if (findAggregate(resultPlan, Aggregate.class) != null) {
            Alias evalAlias = new Alias(binaryExpr.source(), ctx.promqlCommand().valueColumnName(), binaryExpr);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), resultPlan, List.of(evalAlias));
            return new TranslationResult(
                evalPlan,
                evalAlias.toAttribute(),
                pendingFilter,
                groupingSource.exposedLabels(),
                groupingSource.excludedLabels()
            );
        }

        return new TranslationResult(
            resultPlan,
            binaryExpr,
            pendingFilter,
            groupingSource.exposedLabels(),
            groupingSource.excludedLabels()
        );
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
            // Different groupings require vector matching semantics (on/ignoring/group_left/group_right)
            // which is unsupported yet.
            // TODO: https://github.com/elastic/elasticsearch/issues/142596
            boolean areGroupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
                && leftGroupingNames.equals(rightGroupingNames);

            if (areGroupingsCompatible == false) {
                throw new QlIllegalArgumentException("binary expressions with different grouping keys not supported yet");
            }

            // Unique aggregates from both sides
            // Each side's selector uses its own selector condition
            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(leftAgg.aggregates(), left.selectorFilter()));
            uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.selectorFilter()));

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

        // If right had Eval nodes wrapping its Aggregate layer them on top of the merged plan
        // E.g. sum(a) / ceil(max(b)) becomes Eval[ceil(max(b))] -> Aggregate[sum(a), max(b)]
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

        List<Attribute> exposedLabels = List.of();
        // Only expose labels that were both demanded by the parent and declared by the selector.
        if ((selector instanceof LiteralSelector) == false) {
            List<Attribute> selectorLabels = new ArrayList<>();
            for (Expression label : selector.labels()) {
                if (label instanceof Attribute attribute) {
                    selectorLabels.add(attribute);
                }
            }

            // TODO: Should we fail in case of missmatch?
            exposedLabels = intersection(ctx.requiredLabels(), selectorLabels);
        }

        return new TranslationResult(currentPlan, expr, matcherCondition, exposedLabels, List.of());
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

    /**
     * Builds the aggregate function expression for an AcrossSeriesAggregate.
     */
    private static Expression buildAggregateExpression(AcrossSeriesAggregate agg, Expression inputValue, TranslationContext ctx) {
        PromqlFunctionRegistry.PromqlContext promqlCtx = new PromqlFunctionRegistry.PromqlContext(
            ctx.promqlCommand().timestamp(),
            AggregateFunction.NO_WINDOW,
            ctx.stepAttr()
        );
        return PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(agg.functionName(), agg.source(), inputValue, promqlCtx, agg.parameters());
    }

    /**
     * Creates a TimeSeriesAggregate node for the innermost aggregation.
     */
    private static LogicalPlan createInnerAggregate(
        TranslationContext ctx,
        LogicalPlan plan,
        List<Attribute> exposedLabels,
        List<Attribute> excludedLabels,
        Expression aggregateExpression
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();

        // Check for time series grouping (special handling for _tsid)
        FieldAttribute timeSeriesGrouping = getTimeSeriesGrouping(exposedLabels);

        if (timeSeriesGrouping != null) {
            aggregateExpression = new Values(aggregateExpression.source(), aggregateExpression);
            plan = plan.transformDown(EsRelation.class, r -> r.withAdditionalAttribute(timeSeriesGrouping));
        }

        Alias packedTimeSeries = null;
        List<Expression> groupings = new ArrayList<>();
        groupings.add(ctx.stepBucketAlias());
        if (excludedLabels.isEmpty() == false) {
            packedTimeSeries = newTimeSeriesGroupingAlias(promqlCommand.promqlPlan().source(), excludedLabels);
            groupings.add(packedTimeSeries);
        }
        groupings.addAll(exposedLabels);

        List<NamedExpression> aggregates = new ArrayList<>(exposedLabels.size() + 3);
        aggregates.add(new Alias(aggregateExpression.source(), promqlCommand.valueColumnName(), aggregateExpression));
        aggregates.add(ctx.stepAttr());
        if (packedTimeSeries != null) {
            aggregates.add(packedTimeSeries.toAttribute());
        }
        for (Attribute attr : exposedLabels) {
            if (attr != timeSeriesGrouping) {
                aggregates.add(attr);
            }
        }

        return new TimeSeriesAggregate(promqlCommand.promqlPlan().source(), plan, groupings, aggregates, null, promqlCommand.timestamp());
    }

    /**
     * Creates an outer Aggregate on top of an already-aggregated child.
     * For WITHOUT, packs excluded dimensions into a {@code _timeseries} grouping.
     * For BY over WITHOUT, patches the child TSA to carry labels it doesn't yet expose.
     */
    private static LogicalPlan createOuterAggregate(
        TranslationContext ctx,
        LogicalPlan plan,
        List<Attribute> exposedLabels,
        List<Attribute> excludedLabels,
        Expression aggExpr
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        NamedExpression value = new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr);
        var tsa = findAggregate(plan, TimeSeriesAggregate.class);

        if (excludedLabels.isEmpty() == false) {
            // WITHOUT: group by packed _timeseries, carry visible labels via VALUES()
            PackedGrouping packed = applyTimeSeriesMetadataAttribute(ctx, plan, tsa, excludedLabels);
            List<Attribute> resolvedLabels = resolveLabels(exposedLabels, packed.plan().output());
            return createAggregate(ctx, packed.plan(), value, List.of(packed.packedAttribute()), resolvedLabels);
        }

        // BY: patch child TSA if needed, then resolve groupings (with null-fill for absent labels)
        if (tsa != null) {
            plan = adjustAggregateOutputWithLabels(plan, tsa, exposedLabels);
        }
        var resolved = resolveGroupingsOrNull(plan, exposedLabels);
        if (resolved.synthesizedNulls.isEmpty() == false) {
            plan = new Eval(promqlCommand.source(), plan, resolved.synthesizedNulls);
        }
        return createAggregate(ctx, plan, value, resolved.groupings, List.of());
    }

    /**
     * When BY wraps a WITHOUT child, the BY's labels may not be in the child's output.
     * This pushes attribute loading to TimeSeriesAggregate to carry them, skipping labels
     * that were excluded by the child's WITHOUT (those are legitimately absent).
     */
    private static LogicalPlan adjustAggregateOutputWithLabels(LogicalPlan plan, TimeSeriesAggregate tsa, List<Attribute> exposedLabels) {
        Set<String> excludedByChild = findExcludedFieldsInAggregate(plan);
        List<Attribute> needed = difference(exposedLabels, excludedByChild);
        List<Attribute> missing = difference(needed, resolveLabels(needed, plan.output()));
        if (missing.isEmpty()) {
            return plan;
        }
        var newAggregates = new ArrayList<NamedExpression>(tsa.aggregates());
        newAggregates.addAll(missing);
        return transformTimeSeriesAggregate(plan, tsa, new ArrayList<>(tsa.groupings()), newAggregates);
    }

    /**
     * Replaces the target TimeSeriesAggregate in the plan with one using the given groupings and aggregates.
     */
    private static LogicalPlan transformTimeSeriesAggregate(
        LogicalPlan plan,
        TimeSeriesAggregate target,
        List<Expression> groupings,
        List<NamedExpression> aggregates
    ) {
        return plan.transformDown(
            TimeSeriesAggregate.class,
            agg -> agg == target
                ? new TimeSeriesAggregate(agg.source(), agg.child(), groupings, aggregates, agg.timeBucket(), agg.timestamp())
                : agg
        );
    }

    private record ResolvedGroupings(List<Attribute> groupings, List<Alias> synthesizedNulls) {}

    /**
     * Resolves each exposed label against the child output via {@link #resolveLabels}.
     * Labels not found are synthesized as typed NULLs (e.g. BY(cluster) over a child
     * that only outputs pod).
     */
    private static ResolvedGroupings resolveGroupingsOrNull(LogicalPlan plan, List<Attribute> exposedLabels) {
        List<Attribute> resolved = resolveLabels(exposedLabels, plan.output());
        List<Attribute> missing = difference(exposedLabels, resolved);
        List<Alias> nulls = new ArrayList<>();
        List<Attribute> result = new ArrayList<>(resolved);
        for (Attribute label : missing) {
            Alias nullAlias = new Alias(label.source(), label.name(), new Literal(label.source(), null, label.dataType()), label.id());
            nulls.add(nullAlias);
            result.add(nullAlias.toAttribute());
        }
        return new ResolvedGroupings(result, nulls);
    }

    private record PackedGrouping(LogicalPlan plan, Attribute packedAttribute) {}

    /**
     * Ensures the child plan exposes a {@code _timeseries = TimeSeriesWithout(excluded)} grouping.
     * If the child already has one, reuses it. Otherwise, adds it to the given
     * TimeSeriesAggregate's groupings and projects the result to include it.
     */
    private static PackedGrouping applyTimeSeriesMetadataAttribute(
        TranslationContext ctx,
        LogicalPlan inputPlan,
        TimeSeriesAggregate tsa,
        List<Attribute> excluded
    ) {
        // If the child already exposes _timeseries, just build a new alias with the current exclusions
        Attribute existingPacked = findTimeSeriesOutput(inputPlan.output());
        if (existingPacked != null) {
            return new PackedGrouping(inputPlan, newTimeSeriesGroupingAlias(existingPacked.source(), excluded).toAttribute());
        }

        if (tsa == null) {
            throw new QlIllegalArgumentException(
                "PromQL WITHOUT over an aggregated input requires a time-series aggregate child; found [{}]",
                inputPlan.nodeString()
            );
        }

        // Add _timeseries grouping to the inner TSA
        Alias tsAlias = newTimeSeriesGroupingAlias(ctx.promqlCommand().source(), excluded);
        Attribute packedAttr = tsAlias.toAttribute();
        List<Expression> newGroupings = new ArrayList<>(tsa.groupings());
        if (tsa.groupings().stream().noneMatch(g -> g instanceof NamedExpression ne && ne.toAttribute().id().equals(packedAttr.id()))) {
            newGroupings.add(tsAlias);
        }
        LogicalPlan rewritten = transformTimeSeriesAggregate(inputPlan, tsa, newGroupings, new ArrayList<>(tsa.aggregates()));

        // Project to ensure _timeseries is visible alongside existing outputs
        List<NamedExpression> projections = new ArrayList<>(rewritten.output());
        if (projections.stream().noneMatch(p -> p instanceof Attribute a && a.id().equals(packedAttr.id()))) {
            projections.add(packedAttr);
        }
        return new PackedGrouping(new Project(ctx.promqlCommand().source(), rewritten, projections), packedAttr);
    }

    private static List<NamedExpression> resolveOutput(PromqlCommand promqlCommand, LogicalPlan plan, TranslationResult result) {
        List<NamedExpression> projections = new ArrayList<>();
        Attribute valueAttr = findAttributeById(plan.output(), promqlCommand.valueId());
        if (valueAttr == null) {
            valueAttr = findAttributeByFieldName(plan.output(), promqlCommand.valueColumnName());
        }
        if (valueAttr == null) {
            throw new IllegalStateException("PromQL root projection requires a value column");
        }
        projections.add(valueAttr);

        Attribute stepAttr = findAttributeById(plan.output(), promqlCommand.stepId());
        if (stepAttr == null) {
            stepAttr = findAttributeByFieldName(plan.output(), STEP_COLUMN_NAME);
        }
        if (stepAttr == null) {
            throw new IllegalStateException("PromQL root projection requires a step column");
        }
        projections.add(stepAttr);

        List<Attribute> expectedExtra = promqlCommand.output().subList(2, promqlCommand.output().size());
        for (Attribute expected : expectedExtra) {
            Attribute actual = findAttributeById(plan.output(), expected.id());
            if (actual == null && result.hasExcludedLabels()) {
                actual = findTimeSeriesOutput(plan.output());
            }
            if (actual == null) {
                actual = findAttributeByFieldName(plan.output(), fieldName(expected));
            }
            if (actual == null) {
                throw new IllegalStateException("PromQL root projection requires output [" + expected.name() + "]");
            }
            if (actual.id().equals(expected.id()) && actual.name().equals(expected.name())) {
                projections.add(actual);
            } else {
                projections.add(new Alias(expected.source(), expected.name(), actual, expected.id()));
            }
        }
        return projections;
    }

    /**
     * Builds an outer Aggregate grouped by step + groupingAttributes.
     * Any outputAttributes are carried through via VALUES() (used by WITHOUT to preserve visible labels).
     */
    private static Aggregate createAggregate(
        TranslationContext ctx,
        LogicalPlan child,
        NamedExpression value,
        List<Attribute> groupingAttributes,
        List<Attribute> outputAttributes
    ) {
        Attribute stepAttr = ctx.stepAttr();
        List<Expression> groupings = new ArrayList<>(groupingAttributes.size() + 1);
        groupings.add(stepAttr);
        groupings.addAll(groupingAttributes);

        List<NamedExpression> aggregates = new ArrayList<>(groupingAttributes.size() + outputAttributes.size() + 2);
        aggregates.add(value);
        aggregates.add(stepAttr);
        aggregates.addAll(groupingAttributes);
        for (Attribute outputAttribute : outputAttributes) {
            aggregates.add(
                new Alias(
                    outputAttribute.source(),
                    outputAttribute.name(),
                    valuesAggregateForGroupingAttribute(ctx, outputAttribute),
                    outputAttribute.id()
                )
            );
        }
        return new Aggregate(ctx.promqlCommand().source(), child, groupings, aggregates);
    }

    private static AggregateFunction valuesAggregateForGroupingAttribute(TranslationContext ctx, Attribute outputAttribute) {
        if (outputAttribute.isDimension() && ctx.optimizerContext().minimumVersion().supports(DimensionValues.DIMENSION_VALUES_VERSION)) {
            return new DimensionValues(outputAttribute.source(), outputAttribute);
        }
        return new Values(outputAttribute.source(), outputAttribute);
    }

    /**
     * Applies label filters to the plan at the appropriate level.
     * Finds the base EsRelation and adds a Filter on top of it.
     */
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
    private static LogicalPlan filterNulls(PromqlCommand promqlCommand, LogicalPlan plan) {
        return new Filter(promqlCommand.source(), plan, new IsNotNull(plan.output().getFirst().source(), plan.output().getFirst()));
    }

    private static FieldAttribute getTimeSeriesGrouping(List<Attribute> groupings) {
        for (Attribute attr : groupings) {
            if (attr instanceof FieldAttribute fieldAttr
                && fieldAttr.field().getTimeSeriesFieldType() == EsField.TimeSeriesFieldType.DIMENSION
                && fieldAttr.name().equals(MetadataAttribute.TIMESERIES)) {
                return fieldAttr;
            }
        }
        return null;
    }

    /**
     * Adds a Filter node for specified time range.
     */
    private static LogicalPlan withTimestampFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        if (promqlCommand.start().value() != null && promqlCommand.end().value() != null) {
            Source promqlSource = promqlCommand.source();
            Expression timestamp = promqlCommand.timestamp();
            plan = new Filter(
                promqlSource,
                plan,
                new And(
                    promqlSource,
                    new GreaterThanOrEqual(promqlSource, timestamp, promqlCommand.start()),
                    new LessThanOrEqual(promqlSource, timestamp, promqlCommand.end())
                )
            );
        }
        return plan;
    }

    /**
     * Adds a Filter node for comparison filtering.
     */
    private LogicalPlan addComparisonFilter(LogicalPlan plan, VectorBinaryComparison binaryComparison, LogicalOptimizerContext context) {
        Attribute left = plan.output().getFirst().toAttribute();
        ToDouble right = new ToDouble(binaryComparison.right().source(), ((LiteralSelector) binaryComparison.right()).literal());
        Function condition = binaryComparison.op().asFunction().create(binaryComparison.source(), left, right, context.configuration());
        return new Filter(binaryComparison.source(), plan, condition);
    }

    /**
     * Ensures the value column is of type double.
     */
    private static LogicalPlan convertValueToDouble(PromqlCommand promqlCommand, LogicalPlan plan, Expression valueExpr) {
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
        return new Alias(b.source(), STEP_COLUMN_NAME, b, promqlCommand.stepId());
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
     * Strips non-grouping attributes (metadata, unresolved, metrics, NULLs) and deduplicates by field name.
     */
    private static List<Attribute> normalizeLabels(List<Attribute> attributes) {
        List<Attribute> normalized = new ArrayList<>();
        for (Attribute attribute : attributes) {
            if (MetadataAttribute.isTimeSeriesAttributeName(attribute.name())) {
                continue;
            }
            if (attribute.resolved() == false) {
                continue;
            }
            if (attribute instanceof FieldAttribute fieldAttribute && fieldAttribute.isMetric()) {
                continue;
            }
            if (attribute.dataType() == DataType.NULL) {
                continue;
            }
            normalized.add(attribute);
        }
        LinkedHashMap<String, Attribute> byName = new LinkedHashMap<>();
        for (Attribute attr : normalized) {
            byName.put(fieldName(attr), attr);
        }
        return List.copyOf(byName.values());
    }

    /**
     * Set difference by field name.
     */
    private static List<Attribute> difference(List<Attribute> from, List<Attribute> toRemove) {
        Set<String> removeNames = new HashSet<>();
        for (Attribute attr : toRemove) {
            removeNames.add(fieldName(attr));
        }
        return difference(from, removeNames);
    }

    /**
     * Set difference by pre-computed field name set.
     */
    private static List<Attribute> difference(List<Attribute> from, Set<String> toRemove) {
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : from) {
            if (toRemove.contains(fieldName(attr)) == false) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Set intersection by field name.
     */
    private static List<Attribute> intersection(List<Attribute> requested, List<Attribute> available) {
        Set<String> availableNames = new HashSet<>();
        for (Attribute attr : available) {
            availableNames.add(fieldName(attr));
        }
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : requested) {
            if (availableNames.contains(fieldName(attr))) {
                result.add(attr);
            }
        }
        return result;
    }

    /**
     * Resolves requested labels against a plan output, matching first by identity then by field name.
     */
    private static List<Attribute> resolveLabels(List<Attribute> requested, List<Attribute> visibleOutput) {
        List<Attribute> resolved = new ArrayList<>();
        for (Attribute attribute : requested) {
            if (visibleOutput.contains(attribute)) {
                resolved.add(attribute);
                continue;
            }
            Attribute byName = findAttributeByFieldName(visibleOutput, fieldName(attribute));
            if (byName != null) {
                resolved.add(byName);
            }
        }
        return normalizeLabels(resolved);
    }

    private static Alias newTimeSeriesGroupingAlias(Source source, List<Attribute> excludedDimensions) {
        return new TimeSeriesWithout(source, List.copyOf(excludedDimensions)).toAttribute();
    }

    private static Attribute findTimeSeriesOutput(List<Attribute> outputs) {
        for (Attribute output : outputs) {
            if (MetadataAttribute.isTimeSeriesAttributeName(output.name())) {
                return output;
            }
        }
        return null;
    }

    private static Attribute findAttributeByFieldName(List<Attribute> attributes, String fieldNameToFind) {
        for (Attribute attribute : attributes) {
            if (fieldName(attribute).equals(fieldNameToFind)) {
                return attribute;
            }
        }
        return null;
    }

    private static Attribute findAttributeById(List<Attribute> attributes, NameId id) {
        for (Attribute attribute : attributes) {
            if (attribute.id().equals(id)) {
                return attribute;
            }
        }
        return null;
    }

    private static String fieldName(Attribute attr) {
        if (attr instanceof FieldAttribute fieldAttr) {
            return fieldAttr.fieldName().string();
        }
        return attr.name();
    }

    private static Set<String> findExcludedFieldsInAggregate(LogicalPlan plan) {
        TimeSeriesAggregate tsa = findAggregate(plan, TimeSeriesAggregate.class);
        if (tsa != null) {
            for (Expression grouping : tsa.groupings()) {
                if (Alias.unwrap(grouping) instanceof TimeSeriesWithout tw) {
                    return tw.excludedFieldNames();
                }
            }
        }
        return Set.of();
    }

    /**
     * Translates PromQL label matchers into ESQL filter expressions.
     * <p>
     * Uses AutomatonUtils to detect optimizable patterns:
     * - Exact match → field == "value"
     * - Prefix pattern (prefix.*) → field STARTS_WITH "prefix"
     * - Suffix pattern (.*suffix) → field ENDS_WITH "suffix"
     * - Simple alternation (a|b|c) → field IN ("a", "b", "c")
     * - Disjoint prefixes → field STARTS_WITH "p1" OR field STARTS_WITH "p2"
     * - Disjoint suffixes → field ENDS_WITH "s1" OR field ENDS_WITH "s2"
     * - Complex patterns → field RLIKE "pattern"
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
    private static Expression translateLabelMatcher(Source source, Expression field, LabelMatcher matcher) {
        // Check for universal matchers
        if (matcher.matchesAll()) {
            return Literal.fromBoolean(source, true); // No filter needed (matches everything)
        }

        if (matcher.matchesNone()) {
            // This is effectively FALSE - could use a constant false expression
            return Literal.fromBoolean(source, false);
        }

        // Try to extract exact match
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
     *
     * Homogeneous patterns (all same type):
     * - All EXACT → field IN ("a", "b", "c")
     * - All PREFIX → field STARTS_WITH "p1" OR field STARTS_WITH "p2" ...
     * - All SUFFIX → field ENDS_WITH "s1" OR field ENDS_WITH "s2" ...
     *
     * Heterogeneous patterns:
     * - Mixed → (field == "exact") OR (field STARTS_WITH "prefix") OR (field ENDS_WITH "suffix") OR (field RLIKE "regex")
     *
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
