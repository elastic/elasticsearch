/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.PlaceholderRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Selector;

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

    public TranslatePromqlToTimeSeriesAggregate() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(PromqlCommand promqlCommand) {
        // Extract the promqlPlan from the container
        LogicalPlan promqlPlan = promqlCommand.promqlPlan();

        // first replace the Placeholder relation with the child plan
        promqlPlan = promqlPlan.transformUp(PlaceholderRelation.class, pr -> promqlCommand.child());

        // Translate based on plan type
        return translate(promqlPlan);
    }

    private LogicalPlan translate(LogicalPlan promqlPlan) {
        // convert the plan bottom-up
        MapResult result = map(promqlPlan);
        return result.plan();
    }

    private record MapResult(LogicalPlan plan, Map<String, Expression> extras) {}

    // Will pattern match on PromQL plan types:
    // - WithinSeriesAggregate -> TimeSeriesAggregate
    // - AcrossSeriesAggregate -> Aggregate over TimeSeriesAggregate
    // - Selector -> EsRelation + Filter
    private static MapResult map(LogicalPlan p) {
        if (p instanceof Selector selector) {
            return map(selector);
        }
        if (p instanceof PromqlFunctionCall functionCall) {
            return map(functionCall);
        }
        throw new QlIllegalArgumentException("Unsupported PromQL plan node: {}", p);
    }

    private static MapResult map(Selector selector) {
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
        extras.put("timestamp", selector.timestamp());

        // return the condition as filter
        LogicalPlan p = new Filter(selector.source(), selector.child(), Predicates.combineAnd(selectorConditions));

        // arguably the instant selector is a selector with range 0
        if (selector instanceof RangeSelector rangeSelector) {
            Bucket b = new Bucket(rangeSelector.source(), selector.timestamp(), rangeSelector.range(), null, null);
            Alias tbucket = new Alias(b.source(), "TBUCKET", b);
            p = new Eval(tbucket.source(), p, List.of(tbucket));
            extras.put("tbucket", tbucket.toAttribute());
        }

        return new MapResult(p, extras);
    }

    private static MapResult map(PromqlFunctionCall functionCall) {
        MapResult childResult = map(functionCall.child());
        Map<String, Expression> extras = childResult.extras;

        MapResult result;
        Expression target = extras.get("field"); // nested expression

        if (functionCall instanceof WithinSeriesAggregate withinAggregate) {
            // expects selector
            Function esqlFunction = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
                withinAggregate.functionName(),
                withinAggregate.source(),
                List.of(target, extras.get("timestamp"))
            );

           extras.put("field", esqlFunction);
           result = new MapResult(childResult.plan, extras);
        }
        else if (functionCall instanceof AcrossSeriesAggregate acrossAggregate) {
            // expects
            Function esqlFunction = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(
                acrossAggregate.functionName(),
                acrossAggregate.source(),
                List.of(target)
            );

            List<NamedExpression> aggs = new ArrayList<>();
            aggs.add(new Alias(acrossAggregate.source(), acrossAggregate.sourceText(), esqlFunction));

            List<Expression> groupings = new ArrayList<>(acrossAggregate.groupings().size());

            // add groupings
            for (Expression grouping : acrossAggregate.groupings()) {
                NamedExpression named;
                if (grouping instanceof NamedExpression ne) {
                    named = ne;
                } else {
                    named = new Alias(grouping.source(), grouping.sourceText(), grouping);
                }
                aggs.add(named);
                groupings.add(named.toAttribute());
            }

            NamedExpression bucket = (NamedExpression) extras.get("tbucket");
            if (bucket != null) {
                aggs.add(bucket);
                groupings.add(bucket.toAttribute());
            }

            LogicalPlan p = new TimeSeriesAggregate(acrossAggregate.source(), childResult.plan, groupings, aggs, null);
            result = new MapResult(p, extras);
        } else {
            throw new QlIllegalArgumentException("Unsupported PromQL function call: {}", functionCall);
        }

        return result;
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
            return new Literal(source, true, DataType.BOOLEAN); // No filter needed (matches everything)
        }

        if (matcher.matchesNone()) {
            // This is effectively FALSE - could use a constant false expression
            return new Literal(source, false, DataType.BOOLEAN);
        }

        // Try to extract exact match
        String exactMatch = AutomatonUtils.matchesExact(matcher.automaton());
        if (exactMatch != null) {
            return new Equals(source, field, new Literal(source, exactMatch, DataType.KEYWORD));
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
    private static Expression translateDisjointPatterns(
        Source source,
        Expression field,
        List<AutomatonUtils.PatternFragment> fragments
    ) {
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
                values.add(new Literal(source, fragment.value(), DataType.KEYWORD));
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
        Literal value = new Literal(source, fragment.value(), DataType.KEYWORD);

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
