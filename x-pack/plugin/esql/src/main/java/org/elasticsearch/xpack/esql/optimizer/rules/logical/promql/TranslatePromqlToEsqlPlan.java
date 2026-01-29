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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
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
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
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
import java.util.Comparator;
import java.util.List;

/**
 * Translates PromQL logical plan into ESQL plan.
 *
 * This rule runs before {@link TranslateTimeSeriesAggregate} to convert PromQL-specific plan
 * into standard ESQL nodes (TimeSeriesAggregate, Aggregate, Eval, etc.) that can then be further optimized.
 *
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
 */
public final class TranslatePromqlToEsqlPlan extends OptimizerRules.ParameterizedOptimizerRule<PromqlCommand, LogicalOptimizerContext> {

    // TODO make configurable via lookback_delta parameter and (cluster?) setting
    public static final Duration DEFAULT_LOOKBACK = Duration.ofMinutes(5);
    public static final String STEP_COLUMN_NAME = "step";

    public TranslatePromqlToEsqlPlan() {
        super(OptimizerRules.TransformDirection.UP);
    }

    /**
     * Holds ESQL plan built so far and an expression representing the node value.
     */
    private record TranslationResult(LogicalPlan plan, Expression expression) {}

    /**
     * Holds context passed through the recursive translation.
     */
    private record TranslationContext(
        PromqlCommand promqlCommand,
        LogicalOptimizerContext optimizerContext,
        Alias stepBucketAlias,
        List<Expression> labelFilterConditions
    ) {
        Attribute stepAttr() {
            return stepBucketAlias.toAttribute();
        }
    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand, LogicalOptimizerContext context) {
        Alias stepBucketAlias = createStepBucketAlias(promqlCommand);
        List<Expression> labelFilterConditions = new ArrayList<>();

        // Base plan EsRelation with timestamp filter
        LogicalPlan basePlan = withTimestampFilter(promqlCommand, promqlCommand.child());

        // Create translation context
        TranslationContext ctx = new TranslationContext(promqlCommand, context, stepBucketAlias, labelFilterConditions);

        TranslationResult result = translateNode(promqlCommand.promqlPlan(), basePlan, ctx);

        LogicalPlan plan = result.plan();
        Expression valueExpr = result.expression();
        if (labelFilterConditions.isEmpty() == false) {
            plan = applyLabelFilters(plan, labelFilterConditions, promqlCommand);
        }

        // TimeSeriesAggregate always applies because InstantSelectors adds implicit last_over_time().
        // TODO: If we ever support metric references without last_over_time, we could
        // skip TimeSeriesAggregate and use plain Aggregate instead (see #141501 discussion).
        if (containsAggregation(plan) == false) {
            plan = createInnerAggregate(ctx, plan, promqlCommand.promqlPlan().output(), valueExpr);
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
        if (containsAggregation(childResult.plan())) {
            // Child already has an aggregate: create outer Aggregate
            LogicalPlan aggregatePlan = createOuterAggregate(ctx, childResult.plan(), agg, aggExpr);
            Expression outputRef = getValueOutput(aggregatePlan);
            return new TranslationResult(aggregatePlan, outputRef);
        } else {
            // No aggregate yet: create the innermost TimeSeriesAggregate, folding within-series
            // function expressions into the aggregation.
            LogicalPlan timeSeriesAgg = createInnerAggregate(ctx, childResult.plan(), agg.output(), aggExpr);
            Expression outputRef = getValueOutput(timeSeriesAgg);
            return new TranslationResult(timeSeriesAgg, outputRef);
        }
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

        Expression window = (functionCall.child() instanceof RangeSelector rangeSelector)
            ? rangeSelector.range()
            : AggregateFunction.NO_WINDOW;

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
            return new TranslationResult(evalPlan, evalAlias.toAttribute());
        }

        return new TranslationResult(childResult.plan(), function);
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
            return new TranslationResult(leftResult.plan(), leftExpr);
        }

        // The right operand is translated using leftResult.plan() as its base.
        // This assumes at most one side produces an aggregate (e.g. agg + scalar).
        // TODO: Binary ops between two independent aggregates require join operation.
        TranslationResult rightResult = translateNode(binaryOp.right(), leftResult.plan(), ctx);
        Expression rightExpr = new ToDouble(rightResult.expression().source(), rightResult.expression());

        Expression binaryExpr = binaryOp.binaryOp()
            .asFunction()
            .create(binaryOp.source(), leftExpr, rightExpr, ctx.optimizerContext().configuration());

        // If either side has aggregation, we need to add Eval on top
        LogicalPlan resultPlan = rightResult.plan();
        if (containsAggregation(resultPlan)) {
            Alias evalAlias = new Alias(binaryExpr.source(), ctx.promqlCommand().valueColumnName(), binaryExpr);
            LogicalPlan evalPlan = new Eval(ctx.promqlCommand().source(), resultPlan, List.of(evalAlias));
            return new TranslationResult(evalPlan, evalAlias.toAttribute());
        }

        return new TranslationResult(resultPlan, binaryExpr);
    }

    /**
     * Translates a selector (instant, range, or literal).
     * Adds label filter conditions to the context.
     */
    private TranslationResult translateSelector(Selector selector, LogicalPlan currentPlan, TranslationContext ctx) {
        Expression matcherCondition = translateLabelMatchers(selector.source(), selector.labels(), selector.labelMatchers());
        if (matcherCondition != null) {
            ctx.labelFilterConditions().add(matcherCondition);
        }

        Expression expr;
        if (selector instanceof LiteralSelector literalSelector) {
            expr = literalSelector.literal();
        } else if (selector instanceof InstantSelector) {
            // InstantSelector maps to LastOverTime to get latest sample per time series
            expr = new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, ctx.promqlCommand().timestamp());
        } else {
            expr = selector.series();
        }

        return new TranslationResult(currentPlan, expr);
    }

    /**
     * Checks if the plan already contains an aggregation stage.
     */
    private static boolean containsAggregation(LogicalPlan plan) {
        if (plan instanceof Aggregate) {
            return true;
        }
        if (plan instanceof Eval eval) {
            // Eval may be on top of an aggregate
            return containsAggregation(eval.child());
        }
        return false;
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
        Expression aggExpr
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        List<NamedExpression> aggs = new ArrayList<>();
        List<Expression> groupings = new ArrayList<>();

        // Check for time series grouping (special handling for _tsid)
        FieldAttribute timeSeriesGrouping = getTimeSeriesGrouping(labelGroupings);
        if (timeSeriesGrouping != null) {
            aggExpr = new Values(aggExpr.source(), aggExpr);
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

        return new TimeSeriesAggregate(promqlCommand.promqlPlan().source(), basePlan, groupings, aggs, null, promqlCommand.timestamp());
    }

    /**
     * Creates an Aggregate node for outer aggregation stages.
     * The aggExpr contains reference to the child's output.
     */
    private static LogicalPlan createOuterAggregate(
        TranslationContext ctx,
        LogicalPlan childPlan,
        AcrossSeriesAggregate agg,
        Expression aggExpr
    ) {
        PromqlCommand promqlCommand = ctx.promqlCommand();
        Attribute stepAttr = ctx.stepAttr();

        List<Alias> missingGroupingAliases = new ArrayList<>();
        List<Attribute> resolvedGroupings = new ArrayList<>();
        List<Attribute> childOutput = childPlan.output();

        for (Attribute grouping : agg.output()) {
            if (childOutput.contains(grouping)) {
                resolvedGroupings.add(grouping);
            } else {
                // If outer aggregate requests a grouping the inner doesn't produce we fill with null
                // E.g., "max by (cluster) (min by (pod) (...))" inner outputs "pod" but outer wants "cluster"
                Alias nullAlias = new Alias(grouping.source(), grouping.name(), new Literal(grouping.source(), null, grouping.dataType()));
                missingGroupingAliases.add(nullAlias);
                resolvedGroupings.add(nullAlias.toAttribute());
            }
        }

        LogicalPlan plan = missingGroupingAliases.isEmpty()
            ? childPlan
            : new Eval(promqlCommand.source(), childPlan, missingGroupingAliases);

        Alias aggAlias = new Alias(aggExpr.source(), promqlCommand.valueColumnName(), aggExpr);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(stepAttr);
        groupings.addAll(resolvedGroupings);

        List<NamedExpression> aggs = new ArrayList<>();
        aggs.add(aggAlias);
        aggs.add(stepAttr);
        aggs.addAll(resolvedGroupings);

        return new Aggregate(promqlCommand.source(), plan, groupings, aggs);
    }

    /**
     * Applies label filters to the plan at the appropriate level.
     * Finds the base EsRelation and adds a Filter on top of it.
     */
    private static LogicalPlan applyLabelFilters(LogicalPlan plan, List<Expression> labelFilterConditions, PromqlCommand promqlCommand) {
        Expression filterCondition = Predicates.combineAnd(labelFilterConditions);
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
        Expression timeBucketSize;
        if (promqlCommand.isRangeQuery()) {
            timeBucketSize = promqlCommand.step();
        } else {
            // use default lookback for instant queries
            timeBucketSize = Literal.timeDuration(promqlCommand.source(), DEFAULT_LOOKBACK);
        }
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

        return Predicates.combineAnd(conditions);
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
