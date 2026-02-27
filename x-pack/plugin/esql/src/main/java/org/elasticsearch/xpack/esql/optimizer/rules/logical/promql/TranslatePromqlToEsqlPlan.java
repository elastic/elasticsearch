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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TsdimWithout;
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
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarConversionFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ScalarFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
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
import java.util.Map;
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
     * Holds ESQL plan built so far, an expression representing the node value,
     * and an optional selector filter that has not yet been consumed by an aggregate.
     */
    private record TranslationResult(LogicalPlan plan, Expression expression, Expression selectorFilter) {
        TranslationResult(LogicalPlan plan, Expression expression) {
            this(plan, expression, null);
        }
    }

    /**
     * Holds context passed through the recursive translation.
     */
    private record TranslationContext(PromqlCommand promqlCommand, LogicalOptimizerContext optimizerContext, Alias stepBucketAlias) {
        Attribute stepAttr() {
            return stepBucketAlias.toAttribute();
        }
    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand, LogicalOptimizerContext context) {
        Alias stepBucketAlias = createStepBucketAlias(promqlCommand);

        // Base plan EsRelation with timestamp filter
        LogicalPlan basePlan = withTimestampFilter(promqlCommand, promqlCommand.child());

        // Create translation context
        TranslationContext ctx = new TranslationContext(promqlCommand, context, stepBucketAlias);

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
        if (containsAggregation(plan) == false) {
            plan = createInnerAggregate(ctx, plan, promqlCommand.promqlPlan().output(), valueExpr, false, null);
            valueExpr = plan.output().getFirst().toAttribute();
        }

        if (promqlCommand.promqlPlan() instanceof VectorBinaryComparison binaryComparison && binaryComparison.filterMode()) {
            // for comparison with filtering mode, return left operand and apply filter later
            plan = addComparisonFilter(plan, binaryComparison, context);
        }

        plan = convertValueToDouble(promqlCommand, plan, valueExpr);
        // ensure we're returning exactly the same columns (including ids) and in the same order before and after optimization
        plan = new Project(promqlCommand.source(), plan, promqlCommand.output());
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
        TranslationResult childResult = translateNode(agg.child(), currentPlan, ctx);
        Expression aggExpr = buildAggregateExpression(agg, childResult.expression(), ctx);
        boolean isWithout = agg.grouping() == AcrossSeriesAggregate.Grouping.WITHOUT;
        if (containsAggregation(childResult.plan())) {
            // Child already has an aggregate: create outer Aggregate
            LogicalPlan aggregatePlan = createOuterAggregate(ctx, childResult.plan(), agg, aggExpr);
            Expression outputRef = getValueOutput(aggregatePlan);
            return new TranslationResult(aggregatePlan, outputRef, childResult.selectorFilter());
        } else {
            // No aggregate yet: create the innermost TimeSeriesAggregate, folding within-series
            // function expressions into the aggregation.
            TsdimWithout tsdimWithout = isWithout ? new TsdimWithout(agg.source(), new ArrayList<>(agg.groupings())) : null;
            LogicalPlan timeSeriesAgg = createInnerAggregate(ctx, childResult.plan(), agg.output(), aggExpr, isWithout, tsdimWithout);
            Expression outputRef = getValueOutput(timeSeriesAgg);
            return new TranslationResult(timeSeriesAgg, outputRef, childResult.selectorFilter());
        }
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
        if (containsAggregation(childResult.plan())) {
            // plain Aggregate grouped by step only, collapsing all series into one value per step.
            Attribute stepAttr = ctx.stepAttr();
            Alias aggAlias = new Alias(scalarExpr.source(), ctx.promqlCommand().valueColumnName(), scalarExpr);
            LogicalPlan aggregate = new Aggregate(
                ctx.promqlCommand().source(),
                childResult.plan(),
                List.of(stepAttr),
                List.of(aggAlias, stepAttr)
            );
            return new TranslationResult(aggregate, getValueOutput(aggregate), childResult.selectorFilter());
        }

        LogicalPlan timeSeriesAgg = createInnerAggregate(ctx, childResult.plan(), scalarFunc.output(), scalarExpr, false, null);
        return new TranslationResult(timeSeriesAgg, getValueOutput(timeSeriesAgg), childResult.selectorFilter());
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
        if (containsAggregation(childResult.plan())) {
            Alias evalAlias = new Alias(function.source(), ctx.promqlCommand().valueColumnName(), function);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), childResult.plan(), List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute(), childResult.selectorFilter());
        }

        return new TranslationResult(childResult.plan(), function, childResult.selectorFilter());
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
            return new TranslationResult(leftResult.plan(), leftExpr, leftResult.selectorFilter());
        }

        TranslationResult rightResult = translateNode(binaryOp.right(), currentPlan, ctx);
        Expression rightExpr = new ToDouble(rightResult.expression().source(), rightResult.expression());

        Expression binaryExpr = binaryOp.binaryOp()
            .asFunction()
            .create(binaryOp.source(), leftExpr, rightExpr, ctx.optimizerContext().configuration());

        boolean leftAgg = containsAggregation(leftResult.plan());
        boolean rightAgg = containsAggregation(rightResult.plan());

        // If both sides have Aggregate, use a new joint Aggregate plan;
        // otherwise, use the plan from the side that has Aggregate.
        // In both cases aggregate expressions participate in the Eval node that wraps the result.
        LogicalPlan resultPlan;
        if (leftAgg && rightAgg) {
            resultPlan = foldBinaryOperatorAggregate(leftResult, rightResult, binaryOp, ctx);

        } else if (leftAgg) {
            resultPlan = leftResult.plan();
        } else {
            resultPlan = rightResult.plan();
        }

        Expression pendingFilter = null;
        if (!leftAgg || !rightAgg) {
            pendingFilter = combineAndNullable(Arrays.asList(leftResult.selectorFilter(), rightResult.selectorFilter()));
        }

        if (containsAggregation(resultPlan)) {
            Alias evalAlias = new Alias(binaryExpr.source(), ctx.promqlCommand().valueColumnName(), binaryExpr);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), resultPlan, List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute(), pendingFilter);
        }

        return new TranslationResult(resultPlan, binaryExpr, pendingFilter);
    }

    /**
     * Fold left and right aggregates into a single plan, or build an InlineJoin for incompatible groupings.
     */
    private LogicalPlan foldBinaryOperatorAggregate(
        TranslationResult left,
        TranslationResult right,
        VectorBinaryOperator binaryOp,
        TranslationContext ctx
    ) {
        var rightAgg = right.plan().collect(Aggregate.class).getFirst();
        var leftAgg = left.plan().collect(Aggregate.class).getFirst();

        // Compare by name rather than identity to handle _timeseries attributes with different NameIds
        // (each AcrossSeriesAggregate creates its own _timeseries attribute instance).
        // When both sides use _timeseries grouping, also verify the TsdimWithout exclusion sets match
        // to prevent merging aggregates that group by different dimension sets.
        boolean areGroupingsCompatible = leftAgg.groupings().size() == rightAgg.groupings().size()
            && groupingNames(leftAgg.groupings()).equals(groupingNames(rightAgg.groupings()))
            && tsdimWithoutSetsMatch(leftAgg, rightAgg);

        if (areGroupingsCompatible) {
            return foldCompatibleAggregates(left, right, leftAgg, rightAgg);
        }

        VectorMatch match = binaryOp.match();
        if (match.grouping() == VectorMatch.Joining.NONE) {
            throw new QlIllegalArgumentException("binary expressions with different grouping keys not supported yet");
        }

        boolean isGroupLeft = match.grouping() == VectorMatch.Joining.LEFT;
        TranslationResult drivingSide = isGroupLeft ? left : right;
        TranslationResult inlineSide = isGroupLeft ? right : left;
        Aggregate drivingAgg = isGroupLeft ? leftAgg : rightAgg;
        Aggregate inlineAgg = isGroupLeft ? rightAgg : leftAgg;

        Source source = ctx.promqlCommand().source();

        Set<String> matchingKeyNames = computeMatchingKeyNames(match, leftAgg, rightAgg);
        if (matchingKeyNames.isEmpty()) {
            throw new QlIllegalArgumentException("group modifier join requires at least one matching key");
        }

        LogicalPlan drivingPlan = replaceAggregate(
            drivingSide.plan(),
            drivingAgg,
            withFilteredAggregates(drivingAgg, drivingSide.selectorFilter())
        );
        LogicalPlan inlinePlan = replaceAggregate(
            inlineSide.plan(),
            inlineAgg,
            withFilteredAggregates(inlineAgg, inlineSide.selectorFilter())
        );

        // Give the inline side a fresh EsRelation copy so its FieldAttributes have distinct
        // NameIds and don't collide with the driving side when TranslateTimeSeriesAggregate
        // adds _tsid to both independently.
        inlinePlan = copyEsRelation(inlinePlan);

        // PromQL vector matching InlineJoin should be self-contained; do not depend on
        // eval propagation from a stubbed right side.
        if (inlinePlan.anyMatch(p -> p instanceof StubRelation)) {
            throw new QlIllegalArgumentException(
                "promql vector matching inline side must not contain [{}]",
                StubRelation.class.getSimpleName()
            );
        }

        Attribute inlineValue = findOutputAttribute(inlinePlan, inlineSide.expression());

        // Rename the inline value column to avoid shadowing the driving value in
        // mergeOutputExpressions (which drops left-side attributes with same name as right-side).
        if (containsOutputName(drivingPlan, inlineValue.name())) {
            String uniqueName = uniqueOutputName(drivingPlan, "inline_" + inlineValue.name());
            inlinePlan = renameOutputAttribute(inlinePlan, inlineValue, uniqueName);
            inlineValue = findOutputAttribute(inlinePlan, inlineValue);
        }

        // Build JoinConfig: match keys by name between left and right outputs
        List<Attribute> leftJoinFields = new ArrayList<>();
        List<Attribute> rightJoinFields = new ArrayList<>();
        Map<String, Attribute> leftKeysByName = keyAttributesByName(drivingPlan.output(), matchingKeyNames, "driving");
        Map<String, Attribute> rightKeysByName = keyAttributesByName(Join.makeReference(inlinePlan.output()), matchingKeyNames, "inline");
        List<String> missingKeys = new ArrayList<>();
        for (String key : matchingKeyNames) {
            Attribute leftKey = leftKeysByName.get(key);
            Attribute rightKey = rightKeysByName.get(key);
            if (leftKey == null || rightKey == null) {
                missingKeys.add(key);
                continue;
            }
            leftJoinFields.add(leftKey);
            rightJoinFields.add(rightKey);
        }

        if (missingKeys.isEmpty() == false) {
            throw new QlIllegalArgumentException(
                "could not resolve matching keys [{}] in both sides of group modifier join",
                missingKeys
            );
        }

        InlineJoin inlineJoin = new InlineJoin(source, drivingPlan, inlinePlan, JoinTypes.LEFT, leftJoinFields, rightJoinFields);

        // Find inline value attribute in InlineJoin output
        Attribute inlineValueAttr = findOutputAttribute(inlineJoin, inlineValue);

        // NOT NULL filter enforces INNER JOIN semantics (Prometheus drops unmatched rows)
        return new Filter(source, inlineJoin, new IsNotNull(source, inlineValueAttr));
    }

    /**
     * Fold compatible groupings into a single aggregate (both sides group by the same keys).
     */
    private static LogicalPlan foldCompatibleAggregates(
        TranslationResult left,
        TranslationResult right,
        Aggregate leftAgg,
        Aggregate rightAgg
    ) {
        var names = new TemporaryNameGenerator.Monotonic();

        var result = left.plan().transformDown(Aggregate.class, agg -> {
            var uniqueAggregates = new LinkedHashSet<Expression>();
            uniqueAggregates.addAll(withFilter(agg.aggregates(), left.selectorFilter()));
            uniqueAggregates.addAll(withFilter(rightAgg.aggregates(), right.selectorFilter()));

            var newAggregates = uniqueAggregates.stream().map(e -> (NamedExpression) e).map(e -> {
                Expression inner = e;
                if (e instanceof Alias a) {
                    inner = a.child();
                }
                return new Alias(e.source(), names.next(e.name()), inner, e.id());
            }).toList();

            return agg.with(agg.child(), agg.groupings(), newAggregates);
        });

        var rightEvals = right.plan().collect(Eval.class);
        for (Eval eval : rightEvals.reversed()) {
            result = new Eval(eval.source(), result, eval.fields());
        }
        return result;
    }

    private static HashSet<String> groupingNames(List<Expression> groupings) {
        var names = new HashSet<String>();
        for (Expression g : groupings) {
            if (g instanceof NamedExpression ne) {
                names.add(ne.name());
            }
        }
        return names;
    }

    /**
     * Returns true if both aggregates have matching TsdimWithout exclusion sets
     * (or neither uses TsdimWithout). Prevents merging aggregates like
     * {@code sum without(pod)(m) / sum without(region)(m)} which group by different dimension sets
     * despite both having {@code _timeseries} as a grouping name.
     */
    private static boolean tsdimWithoutSetsMatch(Aggregate leftAgg, Aggregate rightAgg) {
        TsdimWithout leftWithout = leftAgg instanceof TimeSeriesAggregate tsa ? tsa.tsdimWithout() : null;
        TsdimWithout rightWithout = rightAgg instanceof TimeSeriesAggregate tsa ? tsa.tsdimWithout() : null;
        if (leftWithout == null && rightWithout == null) {
            return true;
        }
        if (leftWithout == null || rightWithout == null) {
            return false;
        }
        return leftWithout.excludedFieldNames().equals(rightWithout.excludedFieldNames());
    }

    /**
     * Creates a copy of the plan where the EsRelation's field attributes have fresh NameIds.
     * Updates all attribute references throughout the plan tree to use the new NameIds.
     * This prevents NameId collisions when both sides of an InlineJoin share the same source.
     */
    private static LogicalPlan copyEsRelation(LogicalPlan plan) {
        Map<NameId, Attribute> mapping = new LinkedHashMap<>();
        plan.forEachDown(EsRelation.class, esRelation -> {
            for (Attribute attr : esRelation.output()) {
                mapping.putIfAbsent(attr.id(), attr.withId(new NameId()));
            }
        });
        if (mapping.isEmpty()) {
            return plan;
        }
        return plan.transformUp(LogicalPlan.class, p -> {
            if (p instanceof EsRelation esRelation) {
                List<Attribute> newAttrs = new ArrayList<>(esRelation.output().size());
                for (Attribute attr : esRelation.output()) {
                    Attribute newAttr = mapping.get(attr.id());
                    newAttrs.add(newAttr != null ? newAttr : attr);
                }
                return esRelation.withAttributes(newAttrs);
            }
            return p.transformExpressionsOnly(Attribute.class, attr -> {
                Attribute replacement = mapping.get(attr.id());
                return replacement != null ? replacement : attr;
            });
        });
    }

    private static Aggregate withFilteredAggregates(Aggregate aggregate, Expression selectorFilter) {
        var filteredAggs = withFilter(aggregate.aggregates(), selectorFilter);
        return aggregate.with(aggregate.child(), aggregate.groupings(), filteredAggs.stream().map(e -> (NamedExpression) e).toList());
    }

    private static LogicalPlan replaceAggregate(LogicalPlan plan, Aggregate target, Aggregate replacement) {
        return plan.transformDown(Aggregate.class, agg -> agg == target ? replacement : agg);
    }

    private static Map<String, Attribute> keyAttributesByName(List<Attribute> output, Set<String> keyNames, String sideName) {
        Map<String, Attribute> byName = new LinkedHashMap<>();
        for (Attribute attr : output) {
            if (keyNames.contains(attr.name()) == false) {
                continue;
            }
            Attribute existing = byName.putIfAbsent(attr.name(), attr);
            if (existing != null && existing.id().equals(attr.id()) == false) {
                throw new QlIllegalArgumentException("ambiguous join key [{}] in [{}] plan output", attr.name(), sideName);
            }
        }
        return byName;
    }

    private static boolean containsOutputName(LogicalPlan plan, String name) {
        for (Attribute attr : plan.output()) {
            if (attr.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static String uniqueOutputName(LogicalPlan plan, String baseName) {
        String candidate = baseName;
        int suffix = 1;
        while (containsOutputName(plan, candidate)) {
            candidate = baseName + "_" + suffix++;
        }
        return candidate;
    }

    private static LogicalPlan renameOutputAttribute(LogicalPlan plan, Attribute targetAttr, String newName) {
        List<NamedExpression> projections = new ArrayList<>(plan.output().size());
        boolean found = false;
        for (Attribute output : plan.output()) {
            if (output.id().equals(targetAttr.id())) {
                projections.add(new Alias(output.source(), newName, output, output.id()));
                found = true;
            } else {
                projections.add(output);
            }
        }
        if (found == false) {
            throw new QlIllegalArgumentException("could not rename output attribute [{}] in plan [{}]", targetAttr, plan.nodeName());
        }
        return new Project(plan.source(), plan, projections);
    }

    /**
     * Finds an attribute in the plan's output matching the given expression by NameId, falling back to name.
     */
    private static Attribute findOutputAttribute(LogicalPlan plan, Expression expr) {
        if (expr instanceof NamedExpression ne) {
            for (Attribute attr : plan.output()) {
                if (attr.id().equals(ne.id())) {
                    return attr;
                }
            }
            List<Attribute> matchesByName = new ArrayList<>();
            for (Attribute attr : plan.output()) {
                if (attr.name().equals(ne.name())) {
                    matchesByName.add(attr);
                }
            }
            if (matchesByName.size() == 1) {
                return matchesByName.getFirst();
            }
            if (matchesByName.size() > 1) {
                throw new QlIllegalArgumentException("ambiguous inline value in join output for [{}]", ne.name());
            }
        }
        throw new QlIllegalArgumentException("could not resolve [{}] in plan output [{}]", expr, plan.output());
    }

    /**
     * Computes the set of matching key names from on/ignoring clause and both sides' groupings.
     */
    private static Set<String> computeMatchingKeyNames(VectorMatch match, Aggregate leftAgg, Aggregate rightAgg) {
        Set<String> leftNames = groupingNames(leftAgg);
        Set<String> rightNames = groupingNames(rightAgg);

        Set<String> keys;
        if (match.filter() == VectorMatch.Filter.ON) {
            keys = new LinkedHashSet<>(match.filterLabels());
            keys.add(STEP_COLUMN_NAME);
            for (String label : match.filterLabels()) {
                if (leftNames.contains(label) == false || rightNames.contains(label) == false) {
                    throw new QlIllegalArgumentException("label [{}] in on() clause must be present in both sides' groupings", label);
                }
            }
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            keys = new LinkedHashSet<>(leftNames);
            keys.retainAll(rightNames);
            keys.removeAll(match.filterLabels());
        } else {
            keys = new LinkedHashSet<>(leftNames);
            keys.retainAll(rightNames);
        }

        return keys;
    }

    private static Set<String> groupingNames(Aggregate agg) {
        Set<String> names = new LinkedHashSet<>();
        for (Expression g : agg.groupings()) {
            Attribute attr = Expressions.attribute(g);
            if (attr != null) {
                names.add(attr.name());
            }
        }
        return names;
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

        return new TranslationResult(currentPlan, expr, matcherCondition);
    }

    /**
     * Checks if the plan already contains an aggregation stage.
     */
    private static boolean containsAggregation(LogicalPlan plan) {
        return plan.anyMatch(p -> p instanceof Aggregate);
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
        LogicalPlan basePlan,
        List<Attribute> labelGroupings,
        Expression aggExpr,
        boolean isWithout,
        TsdimWithout tsdimWithout
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        List<NamedExpression> aggs = new ArrayList<>();
        List<Expression> groupings = new ArrayList<>();

        FieldAttribute timeSeriesGrouping = getTimeSeriesGrouping(labelGroupings);
        if (timeSeriesGrouping != null) {
            if (isWithout == false) {
                aggExpr = new Values(aggExpr.source(), aggExpr);
            }
            basePlan = basePlan.transformDown(EsRelation.class, r -> r.withAdditionalAttribute(timeSeriesGrouping));
        }

        // Value aggregation
        aggs.add(new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr));
        Attribute stepAttr = ctx.stepAttr();
        aggs.add(stepAttr);
        for (Attribute grouping : labelGroupings) {
            if (grouping != timeSeriesGrouping) {
                aggs.add(grouping);
            }
        }

        // Groupings: step + label groupings
        groupings.add(ctx.stepBucketAlias());
        groupings.addAll(labelGroupings);

        return new TimeSeriesAggregate(
            promqlCommand.promqlPlan().source(),
            basePlan,
            groupings,
            aggs,
            null,
            promqlCommand.timestamp(),
            tsdimWithout
        );
    }

    /**
     * Builds the outer (parent) {@link Aggregate} for nested PromQL aggregations and ensures
     * its groupings are compatible with the child plan output.
     * <p>
     * For each requested parent grouping:
     * <ol>
     *     <li>PASS THROUGH: if the child already outputs the grouping, reuse it directly.
     *     </li>
     *     <li>EXTRACT FROM PACKED: if the child has an inner {@link TimeSeriesAggregate} with
     *     {@code _timeseries} (e.g., from {@code without}), add the requested label attribute to inner agg.
     *     </li>
     *     <li>NULL FILL: if the grouping is unavailable (for example excluded by {@code without}), synthesize it as {@code null}.
     *     </li>
     * </ol>
     */
    private static LogicalPlan createOuterAggregate(
        TranslationContext ctx,
        LogicalPlan childPlan,
        AcrossSeriesAggregate agg,
        Expression aggExpr
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        Attribute stepAttr = ctx.stepAttr();

        // Find the inner that has "_timeseries" dim (if any).
        final var dimensionValuesAgg = childPlan.collect(TimeSeriesAggregate.class)
            .stream()
            .filter(tsa -> tsa.tsdimWithout() != null)
            .filter(
                tsa -> tsa.groupings()
                    .stream()
                    .anyMatch(g -> g instanceof NamedExpression ne && MetadataAttribute.TIMESERIES.equals(ne.name()))
            )
            .findFirst();

        Set<String> excludedByWithout = dimensionValuesAgg.map(TimeSeriesAggregate::tsdimWithout)
            .map(TsdimWithout::excludedFieldNames)
            .orElseGet(Set::of);

        Alias aggAlias = new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr);
        List<Alias> evalStepGroupings = new ArrayList<>();
        List<Attribute> resolvedGroupings = new ArrayList<>();
        List<NamedExpression> outputAggs = new ArrayList<>();
        outputAggs.add(aggAlias);
        resolvedGroupings.add(stepAttr);
        outputAggs.add(stepAttr);

        // Start from the existing inner groupings/aggs if we have a TSA; otherwise start empty and never use them.
        List<Expression> innerGroupings = dimensionValuesAgg.map(tsa -> new ArrayList<>(tsa.groupings())).orElseGet(ArrayList::new);

        List<NamedExpression> innerAggs = dimensionValuesAgg.map(tsa -> new ArrayList<NamedExpression>(tsa.aggregates()))
            .orElseGet(ArrayList::new);

        int originalInnerGroupingCount = dimensionValuesAgg.map(tsa -> tsa.groupings().size()).orElse(0);

        for (var grouping : agg.output()) {
            // Skip synthetic groupings
            if (MetadataAttribute.TIMESERIES.equals(grouping.name()) || grouping.dataType() == DataType.NULL) {
                continue;
            }

            if (childPlan.output().contains(grouping)) {
                // PASS_THROUGH
                resolvedGroupings.add(grouping);
                outputAggs.add(grouping);
                continue;
            }

            boolean canExtractFromPacked = dimensionValuesAgg.isPresent() && excludedByWithout.contains(grouping.name()) == false;
            if (canExtractFromPacked) {
                // EXTRACT_FROM_PACKED
                innerGroupings.add(grouping);
                innerAggs.add(grouping);
                resolvedGroupings.add(grouping);
                outputAggs.add(grouping);
                continue;
            }

            // NULL_FILL
            Alias nullAlias = new Alias(
                grouping.source(),
                grouping.name(),
                new Literal(grouping.source(), null, grouping.dataType()),
                grouping.id()
            );
            evalStepGroupings.add(nullAlias);
            Attribute nullAttr = nullAlias.toAttribute();
            resolvedGroupings.add(nullAttr);
            outputAggs.add(nullAttr);
        }

        LogicalPlan plan = childPlan;

        // Rewrite the inner agg only if we actually extended it.
        if (dimensionValuesAgg.isPresent() && innerGroupings.size() > originalInnerGroupingCount) {
            var target = dimensionValuesAgg.get();
            var newGroupings = List.copyOf(innerGroupings);
            var newAggs = List.copyOf(innerAggs);

            plan = plan.transformDown(TimeSeriesAggregate.class, tsa -> {
                if (tsa != target) {
                    return tsa;
                }
                return new TimeSeriesAggregate(
                    tsa.source(),
                    tsa.child(),
                    newGroupings,
                    newAggs,
                    tsa.timeBucket(),
                    tsa.timestamp(),
                    tsa.tsdimWithout()
                );
            });
        }

        if (evalStepGroupings.isEmpty() == false) {
            plan = new Eval(promqlCommand.source(), plan, evalStepGroupings);
        }

        return new Aggregate(promqlCommand.source(), plan, List.copyOf(resolvedGroupings), outputAggs);
    }

    /**
     * Applies label filters to the plan at the appropriate level; finds the base EsRelation and adds a Filter on top of it.
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
