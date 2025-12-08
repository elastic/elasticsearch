/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public final class TranslatePromqlToTimeSeriesAggregate extends OptimizerRules.OptimizerRule<PromqlCommand> {

    public static final Duration DEFAULT_LOOKBACK = Duration.ofMinutes(5);

    public TranslatePromqlToTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand) {
        // Safety check: this should never occur as the parser should reject PromQL when disabled,
        // but we check here as an additional safety measure
        if (PromqlFeatures.isEnabled() == false) {
            throw new EsqlIllegalArgumentException(
                "PromQL translation attempted but feature is disabled. This should have been caught by the parser."
            );
        }

        // Extract the promqlPlan from the container
        LogicalPlan promqlPlan = promqlCommand.promqlPlan();

        // first replace the Placeholder relation with the child plan
        promqlPlan = promqlPlan.transformUp(PlaceholderRelation.class, pr -> withTimestampFilter(promqlCommand, promqlCommand.child()));

        // Translate based on plan type by converting the plan bottom-up
        TimeSeriesAggregate tsAggregate = (TimeSeriesAggregate) map(promqlCommand, promqlPlan).plan();
        promqlPlan = tsAggregate;
        // ToDouble conversion of the metric using an eval to ensure a consistent output type
        Alias convertedValue = new Alias(
            promqlCommand.source(),
            promqlCommand.valueColumnName(),
            new ToDouble(promqlCommand.source(), tsAggregate.output().getFirst().toAttribute()),
            promqlCommand.valueId()
        );
        promqlPlan = new Eval(promqlCommand.source(), promqlPlan, List.of(convertedValue));
        // Project to maintain the correct output order, as declared in AcrossSeriesAggregate#output:
        // [value, step, ...groupings]
        List<NamedExpression> projections = new ArrayList<>();
        projections.add(convertedValue.toAttribute());
        List<Attribute> output = tsAggregate.output();
        for (int i = 1; i < output.size(); i++) {
            projections.add(output.get(i));
        }
        return new Project(promqlCommand.source(), promqlPlan, projections);
    }

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

    private record MapResult(LogicalPlan plan, Map<String, Expression> extras) {}

    // Will pattern match on PromQL plan types:
    // - AcrossSeriesAggregate -> Aggregate over TimeSeriesAggregate
    // - WithinSeriesAggregate -> TimeSeriesAggregate
    // - Selector -> EsRelation + Filter
    private static MapResult map(PromqlCommand promqlCommand, LogicalPlan p) {
        if (p instanceof Selector selector) {
            return mapSelector(selector);
        }
        if (p instanceof PromqlFunctionCall functionCall) {
            return mapFunction(promqlCommand, functionCall);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", p);
    }

    private static MapResult mapSelector(Selector selector) {
        // Create a placeholder relation to be replaced later
        var matchers = selector.labelMatchers();
        Expression matcherCondition = translateLabelMatchers(selector.source(), selector.labels(), matchers);

        List<Expression> selectorConditions = new ArrayList<>();
        // name into is not null
        selectorConditions.add(new IsNotNull(selector.source(), selector.series()));
        // convert the matchers into a filter expression
        if (matcherCondition != null) {
            selectorConditions.add(matcherCondition);
        }

        Map<String, Expression> extras = new HashMap<>();
        extras.put("field", selector.series());

        // return the condition as filter
        LogicalPlan p = new Filter(selector.source(), selector.child(), Predicates.combineAnd(selectorConditions));

        return new MapResult(p, extras);
    }

    private static MapResult mapFunction(PromqlCommand promqlCommand, PromqlFunctionCall functionCall) {
        MapResult childResult = map(promqlCommand, functionCall.child());
        Map<String, Expression> extras = childResult.extras;

        Expression target = extras.get("field"); // nested expression

        if (functionCall instanceof WithinSeriesAggregate withinAggregate) {
            // expects selector
            Function esqlFunction = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
                withinAggregate.functionName(),
                withinAggregate.source(),
                List.of(target, promqlCommand.timestamp())
            );

            extras.put("field", esqlFunction);
            return new MapResult(childResult.plan, extras);
        } else if (functionCall instanceof AcrossSeriesAggregate acrossAggregate) {
            List<NamedExpression> aggs = new ArrayList<>();
            List<Expression> groupings = new ArrayList<>(acrossAggregate.groupings().size());
            Alias stepBucket = createStepBucketAlias(promqlCommand);
            initAggregatesAndGroupings(acrossAggregate, target, aggs, groupings, stepBucket.toAttribute());
            LogicalPlan p = new Eval(stepBucket.source(), childResult.plan, List.of(stepBucket));
            TimeSeriesAggregate timeSeriesAggregate = new TimeSeriesAggregate(acrossAggregate.source(), p, groupings, aggs, null);
            return new MapResult(timeSeriesAggregate, extras);
        } else {
            throw new QlIllegalArgumentException("Unsupported PromQL function call: {}", functionCall);
        }
    }

    private static void initAggregatesAndGroupings(
        AcrossSeriesAggregate acrossAggregate,
        Expression target,
        List<NamedExpression> aggs,
        List<Expression> groupings,
        Attribute stepBucket
    ) {
        // main aggregation
        Function esqlFunction = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
            acrossAggregate.functionName(),
            acrossAggregate.source(),
            List.of(target)
        );

        Alias value = new Alias(acrossAggregate.source(), acrossAggregate.sourceText(), esqlFunction);
        aggs.add(value);

        // timestamp/step
        aggs.add(stepBucket);
        groupings.add(stepBucket);

        // additional groupings (by)
        for (NamedExpression grouping : acrossAggregate.groupings()) {
            aggs.add(grouping);
            groupings.add(grouping.toAttribute());
        }
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
            promqlCommand.source(),
            promqlCommand.timestamp(),
            timeBucketSize,
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER
        );
        return new Alias(b.source(), "step", b, promqlCommand.stepId());
    }

    /**
     * Translates PromQL label matchers into ESQL filter expressions.
     *
     * Uses AutomatonUtils to detect optimizable patterns:
     * - Exact match → field == "value"
     * - Prefix pattern (prefix.*) → field STARTS_WITH "prefix"
     * - Suffix pattern (.*suffix) → field ENDS_WITH "suffix"
     * - Simple alternation (a|b|c) → field IN ("a", "b", "c")
     * - Disjoint prefixes → field STARTS_WITH "p1" OR field STARTS_WITH "p2"
     * - Disjoint suffixes → field ENDS_WITH "s1" OR field ENDS_WITH "s2"
     * - Complex patterns → field RLIKE "pattern"
     *
     * @param source the source location for error reporting
     * @param labelMatchers the PromQL label matchers to translate
     * @return an ESQL Expression combining all label matcher conditions with AND
     */
    static Expression translateLabelMatchers(Source source, List<Expression> fields, LabelMatchers labelMatchers) {
        List<Expression> conditions = new ArrayList<>();
        boolean hasNameMatcher = false;
        var matchers = labelMatchers.matchers();
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

        // Try to extract disjoint patterns (handles mixed prefix/suffix/exact)
        List<AutomatonUtils.PatternFragment> fragments = AutomatonUtils.extractFragments(matcher.value());
        if (fragments != null && fragments.isEmpty() == false) {
            return translateDisjointPatterns(source, field, fragments);
        }

        // Fallback to RLIKE with the full automaton pattern
        // Note: We need to ensure the pattern is properly anchored for PromQL semantics
        return new RLike(source, field, new RLikePattern(matcher.toString()));
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
