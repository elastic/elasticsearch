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
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
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
 * Translates PromQL logical plans into ESQL TimeSeriesAggregate nodes.
 *
 * This rule runs before {@link TranslateTimeSeriesAggregate} to convert PromQL-specific
 * plans (WithinSeriesAggregate, AcrossSeriesAggregate) into standard ESQL TimeSeriesAggregate
 * nodes that can then be further optimized by the existing time-series translation pipeline.
 *
 * Translation examples:
 * <pre>
 * PromQL: rate(http_requests[5m])
 *
 * PromQL Plan:
 *   WithinSeriesAggregate(name="rate")
 *     └── RangeSelector(http_requests, range=5m)
 *
 * Translated to:
 *   TimeSeriesAggregate(groupBy=[_tsid], aggs=[rate(value, @timestamp)])
 *     └── Filter(__name__ == "http_requests")
 *           └── EsRelation(*, mode=TIME_SERIES)
 * </pre>
 */
public final class TranslatePromqlToTimeSeriesAggregate extends OptimizerRules.ParameterizedOptimizerRule<
    PromqlCommand,
    LogicalOptimizerContext> {

    // TODO make configurable via lookback_delta parameter and (cluster?) setting
    public static final Duration DEFAULT_LOOKBACK = Duration.ofMinutes(5);
    public static final String STEP_COLUMN_NAME = "step";

    public TranslatePromqlToTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand, LogicalOptimizerContext context) {
        Alias stepBucketAlias = createStepBucketAlias(promqlCommand);
        List<Expression> labelFilterConditions = new ArrayList<>();
        Expression value = mapNode(
            promqlCommand,
            promqlCommand.promqlPlan(),
            labelFilterConditions,
            context,
            stepBucketAlias.toAttribute()
        );
        LogicalPlan plan = withTimestampFilter(promqlCommand, promqlCommand.child());
        plan = addLabelFilters(promqlCommand, labelFilterConditions, plan);
        plan = createTimeSeriesAggregate(promqlCommand, value, plan, stepBucketAlias);
        if (promqlCommand.promqlPlan() instanceof VectorBinaryComparison binaryComparison) {
            plan = addFilter(plan, binaryComparison, context);
        }
        plan = convertValueToDouble(promqlCommand, plan);
        // ensure we're returning exactly the same columns (including ids) and in the same order before and after optimization
        plan = new Project(promqlCommand.source(), plan, promqlCommand.output());
        plan = filterNulls(promqlCommand, plan);
        return plan;
    }

    /**
     * Adds a Filter node on top of the given plan to restrict data to the specified time range.
     * The time range is defined by the start and end expressions in the PromqlCommand.
     */
    private static LogicalPlan withTimestampFilter(PromqlCommand promqlCommand, LogicalPlan plan) {
        // start and end are either both set or both null
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
     * Adds label filter conditions (such as {job="prometheus"}) as a Filter node on top of the given plan.
     * Combines multiple conditions with AND.
     * This is consistent with PromQL semantics where non-matching series are dropped.
     * Therefore, it's both easier to implement and more efficient to filter out non-matching series early.
     */
    private static LogicalPlan addLabelFilters(PromqlCommand promqlCommand, List<Expression> labelFilterConditions, LogicalPlan plan) {
        if (labelFilterConditions.isEmpty() == false) {
            plan = new Filter(promqlCommand.source(), plan, Predicates.combineAnd(labelFilterConditions));
        }
        return plan;
    }

    /**
     * Creates a TimeSeriesAggregate node wrapping the given child plan.
     * The aggregation groups by step (time bucket) and additional groupings depending on the PromQL plan root.
     */
    private static TimeSeriesAggregate createTimeSeriesAggregate(
        PromqlCommand promqlCommand,
        Expression value,
        LogicalPlan plan,
        Alias stepBucket
    ) {
        List<NamedExpression> aggs = new ArrayList<>();
        List<Expression> groupings = new ArrayList<>();

        List<Attribute> additionalGroupings = promqlCommand.promqlPlan().output();
        FieldAttribute timeSeriesGrouping = getTimeSeriesGrouping(additionalGroupings);
        if (timeSeriesGrouping != null) {
            value = new Values(value.source(), value);
            plan = plan.transformDown(EsRelation.class, r -> r.withAdditionalAttribute(timeSeriesGrouping));
        }
        // value aggregation
        aggs.add(new Alias(value.source(), promqlCommand.valueColumnName(), value));

        // timestamp/step
        aggs.add(stepBucket.toAttribute());
        groupings.add(stepBucket);

        // additional groupings (by)
        for (Attribute grouping : additionalGroupings) {
            if (grouping != timeSeriesGrouping) {
                aggs.add(grouping);
            }
            groupings.add(grouping);
        }
        return new TimeSeriesAggregate(promqlCommand.promqlPlan().source(), plan, groupings, aggs, null, promqlCommand.timestamp());
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
     * Ensures the value column is of type double by adding an Eval node with ToDouble conversion.
     */
    private static LogicalPlan convertValueToDouble(PromqlCommand promqlCommand, LogicalPlan plan) {
        Alias convertedValue = new Alias(
            promqlCommand.source(),
            promqlCommand.valueColumnName(),
            new ToDouble(promqlCommand.source(), plan.output().getFirst().toAttribute()),
            promqlCommand.valueId()
        );
        return new Eval(promqlCommand.source(), plan, List.of(convertedValue));
    }

    /**
     * Recursively maps PromQL plan nodes to ESQL expressions to compute the value for the time series aggregation.
     * Collects label filter conditions into the provided list.
     */
    private static Expression mapNode(
        PromqlCommand promqlCommand,
        LogicalPlan p,
        List<Expression> labelFilterConditions,
        LogicalOptimizerContext context,
        Attribute stepAttribute
    ) {
        return switch (p) {
            case Selector selector -> mapSelector(promqlCommand, selector, labelFilterConditions);
            case PromqlFunctionCall functionCall -> mapFunction(promqlCommand, functionCall, labelFilterConditions, context, stepAttribute);
            case ScalarFunction functionCall -> mapScalarFunction(promqlCommand, functionCall, stepAttribute);
            case VectorBinaryOperator binaryOperator -> mapBinaryOperator(
                promqlCommand,
                binaryOperator,
                labelFilterConditions,
                context,
                stepAttribute
            );
            default -> throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", p);
        };
    }

    /**
     * Maps a PromQL VectorBinaryArithmetic node to an ESQL expression.
     * Recursively maps the left and right operands and applies the binary operation.
     * Both operands are converted to double to ensure semantic consistency with PromQL.
     */
    private static Expression mapBinaryOperator(
        PromqlCommand promqlCommand,
        VectorBinaryOperator binaryOperator,
        List<Expression> labelFilterConditions,
        LogicalOptimizerContext context,
        Attribute stepAttribute
    ) {
        Expression left = mapNode(promqlCommand, binaryOperator.left(), labelFilterConditions, context, stepAttribute);
        left = new ToDouble(left.source(), left);

        if (binaryOperator instanceof VectorBinaryComparison comp && comp.filterMode()) {
            // for comparison with filtering mode, return left operand and apply filter later
            return left;
        } else {
            Expression right = mapNode(promqlCommand, binaryOperator.right(), labelFilterConditions, context, stepAttribute);
            right = new ToDouble(right.source(), right);
            return binaryOperator.binaryOp().asFunction().create(binaryOperator.source(), left, right, context.configuration());
        }
    }

    /**
     * Adds a Filter node on top of the given plan for the binary comparison condition.
     * At this time, we only support a single top-level binary comparison in filtering mode where the right operand is a literal.
     * Therefore, we can simply filter the result of the plan.
     * The left operand is assumed to be the value column of the plan's output.
     * The right operand is expected to be a LiteralSelector, which we're validating during analysis.
     * <p>
     * To support more complex expressions in the future, we would need to individually evaluate both sides as aggregations (STATS)
     * and account for nested comparisons where groupings may differ.
     * Example: sum(max by (job) (foo > bar)) > avg(baz)
     */
    private LogicalPlan addFilter(LogicalPlan plan, VectorBinaryComparison binaryComparison, LogicalOptimizerContext context) {
        Attribute left = plan.output().getFirst().toAttribute();
        ToDouble right = new ToDouble(binaryComparison.right().source(), ((LiteralSelector) binaryComparison.right()).literal());
        Function condition = binaryComparison.op().asFunction().create(binaryComparison.source(), left, right, context.configuration());
        return new Filter(binaryComparison.source(), plan, condition);
    }

    /**
     * Maps a PromQL Selector node to an ESQL expression.
     * <ul>
     *     <li>InstantSelector: maps to LastOverTime aggregation to get the latest sample per time series and step.</li>
     *     <li>
     *         RangeSelector: maps to the field expression,
     *         yielding all samples per time series and step,
     *         to be aggregated by the enclosing {@link WithinSeriesAggregate}.
     *     </li>
     *     <li>LiteralSelector: maps to its literal value.</li>
     * </ul>
     * The label matchers of the selector are translated into filter conditions and added to the provided list.
     * We're not creating a filter for the {@code __name__} label here, as that's handled by {@link IgnoreNullMetrics}.
     */
    private static Expression mapSelector(PromqlCommand promqlCommand, Selector selector, List<Expression> labelFilterConditions) {
        Expression matcherCondition = translateLabelMatchers(selector.source(), selector.labels(), selector.labelMatchers());
        if (matcherCondition != null) {
            labelFilterConditions.add(matcherCondition);
        }

        if (selector instanceof InstantSelector) {
            // TODO wire lookback delta from PromqlCommand once we support window sizes independent from the step/tbucket duration
            return new LastOverTime(selector.source(), selector.series(), AggregateFunction.NO_WINDOW, promqlCommand.timestamp());
        }
        return selector.series();
    }

    private static Expression mapFunction(
        PromqlCommand promqlCommand,
        PromqlFunctionCall functionCall,
        List<Expression> labelFilterConditions,
        LogicalOptimizerContext context,
        Attribute stepAttribute
    ) {
        Expression target = mapNode(promqlCommand, functionCall.child(), labelFilterConditions, context, stepAttribute);

        final Expression window;
        if (functionCall.child() instanceof RangeSelector rangeSelector) {
            window = rangeSelector.range();
        } else {
            window = AggregateFunction.NO_WINDOW;
        }

        PromqlFunctionRegistry.PromqlContext ctx = new PromqlFunctionRegistry.PromqlContext(
            promqlCommand.timestamp(),
            window,
            stepAttribute
        );

        List<Expression> extraParams = functionCall.parameters();
        Expression function = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
            functionCall.functionName(),
            functionCall.source(),
            target,
            ctx,
            extraParams
        );
        // This can happen when trying to provide a counter to a function that doesn't support it e.g. avg_over_time on a counter
        // This is essentially a bug since this limitation doesn't exist in PromQL itself.
        // Throwing an error here to avoid generating invalid plans with obscure errors downstream.
        Expression.TypeResolution typeResolution = function.typeResolved();
        if (typeResolution.unresolved()) {
            throw new QlIllegalArgumentException("Could not resolve type for function [{}]: {}", function, typeResolution.message());
        }
        return function;
    }

    private static Expression mapScalarFunction(PromqlCommand promqlCommand, ScalarFunction function, Attribute stepAttribute) {
        PromqlFunctionRegistry.PromqlContext ctx = new PromqlFunctionRegistry.PromqlContext(
            promqlCommand.timestamp(),
            /* window= */null,
            stepAttribute
        );
        return PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(function.functionName(), function.source(), null, ctx, List.of());
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
        AutomatonUtils.PatternFragment.Type firstType = sortedFragments.get(0).type();
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
