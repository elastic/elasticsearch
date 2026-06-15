/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InfoCommandPlanUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineOr;

/**
 * Shared plan-building utilities for Prometheus REST handlers.
 */
final class PrometheusPlanBuilderUtils {

    /**
     * Column produced by {@link org.elasticsearch.xpack.esql.plan.logical.MetricsInfo} and
     * {@link org.elasticsearch.xpack.esql.plan.logical.TsInfo} that lists the dimension field names.
     */
    static final String DIMENSION_FIELDS = "dimension_fields";

    /**
     * Column produced by {@link org.elasticsearch.xpack.esql.plan.logical.TsInfo} and
     * {@link org.elasticsearch.xpack.esql.plan.logical.MetricsInfo}.
     */
    static final String METRIC_NAME_FIELD = "metric_name";

    private static final String METRICS_PREFIX = "metrics.";
    private static final String METRICS_PREFIX_REGEX = "metrics\\.(";

    /**
     * Query settings applied to every ESQL statement issued by Prometheus REST handlers.
     * {@code SET unmapped_fields = "NULLIFY"} makes references to fields that are absent from an
     * index's mappings evaluate to {@code null} rather than failing with an "Unknown column" error.
     * This is required because these handlers query {@code TS *} across mixed data streams (both
     * Prometheus-style streams that carry {@code labels.__name__} and OTel-style streams that do
     * not), so any label field may be absent from some indices in the pattern.
     *
     * <p>{@code labels.__name__} is the Prometheus reserved label that stores the metric name.
     * OTel metrics typically omit it and expose the metric name via the field name instead.
     */
    static final List<QuerySetting> QUERY_SETTINGS = List.of(
        new QuerySetting(Source.EMPTY, new Alias(Source.EMPTY, "unmapped_fields", Literal.keyword(Source.EMPTY, "NULLIFY")))
    );

    private PrometheusPlanBuilderUtils() {}

    /**
     * Returns an {@link UnresolvedRelation} for the given index pattern using the {@code TS} source command.
     */
    static UnresolvedRelation tsSource(String index) {
        IndexPattern pattern = new IndexPattern(Source.EMPTY, index);
        return new UnresolvedRelation(Source.EMPTY, pattern, false, List.of(), null, SourceCommand.TS);
    }

    static MetricsInfo metricsInfo(Source source, LogicalPlan child) {
        return new MetricsInfo(source, InfoCommandPlanUtils.injectDocAttribute(source, child));
    }

    static TsInfo tsInfo(Source source, LogicalPlan child) {
        return new TsInfo(source, InfoCommandPlanUtils.injectDocAttribute(source, child));
    }

    /**
     * Parses each {@code match[]} selector string into an {@link InstantSelector}.
     *
     * @param matchSelectors PromQL instant vector selector strings (may be empty)
     * @return list of parsed selectors; empty if {@code matchSelectors} is empty
     * @throws IllegalArgumentException if a selector is syntactically invalid or not an instant vector selector
     */
    static List<InstantSelector> parseInstantSelectors(List<String> matchSelectors) {
        List<InstantSelector> result = new ArrayList<>();
        PromqlParser parser = new PromqlParser();
        for (String selector : matchSelectors) {
            LogicalPlan parsed;
            try {
                parsed = parser.createStatement(selector);
            } catch (ParsingException e) {
                throw new IllegalArgumentException("Invalid match[] selector [" + selector + "]: " + e.getMessage(), e);
            }
            if (parsed instanceof InstantSelector instantSelector) {
                result.add(instantSelector);
            } else {
                throw new IllegalArgumentException("match[] selector must be an instant vector selector, got: [" + selector + "]");
            }
        }
        return result;
    }

    /**
     * Builds pre-info selector conditions with a fallback for non-EQ {@code __name__} matchers,
     * for use in the regular-label values plan which has no post-{@code TsInfo} step.
     *
     * @param selectors pre-parsed instant vector selectors (may be empty)
     * @return list of per-selector conditions; empty if no conditions apply
     * @see #buildPreInfoSelectorConditionWithNameFallback(InstantSelector)
     */
    static List<Expression> buildPreInfoConditionsWithNameFallback(List<InstantSelector> selectors) {
        List<Expression> conditions = new ArrayList<>();
        for (InstantSelector selector : selectors) {
            Expression cond = buildPreInfoSelectorConditionWithNameFallback(selector);
            if (cond != null) {
                conditions.add(cond);
            }
        }
        return conditions;
    }

    /**
     * Like {@link #buildPreInfoSelectorCondition(InstantSelector)} but additionally handles
     * NEQ/REG/NREG {@code __name__} matchers by filtering on the {@code __name__} field
     * ({@code labels.__name__} for Prometheus-style time series).
     *
     * <p>Used only in the regular-label values plan, which has no post-{@code TsInfo} step and
     * therefore cannot access {@link #METRIC_NAME_FIELD}. OTel metrics that lack
     * {@code labels.__name__} are excluded when a non-EQ {@code __name__} matcher is present
     * — this is a known limitation of the current plan shape for that endpoint.
     */
    static Expression buildPreInfoSelectorConditionWithNameFallback(InstantSelector selector) {
        var matchers = selector.labelMatchers().matchers();
        List<Expression> conditions = new ArrayList<>(matchers.size());

        for (var matcher : matchers) {
            if (LabelMatcher.NAME.equals(matcher.name())) {
                if (matcher.matcher() == LabelMatcher.Matcher.EQ) {
                    // Parser guarantees that "__name__" equality is represented by a non-null series.
                    assert selector.series() != null : "EQ __name__ matcher should always have a non-null series";
                    conditions.add(new IsNotNull(Source.EMPTY, selector.series()));
                } else if (matcher.matchesAll() == false) {
                    // Non-equality "__name__" matchers (e.g. {__name__!="foo"}, {__name__=~"bar"}) cannot use
                    // selector.series(). Use the "__name__" label; series without it are excluded.
                    Expression nameField = new UnresolvedAttribute(Source.EMPTY, LabelMatcher.NAME);
                    Expression matcherCond = TranslatePromqlToEsqlPlan.translateLabelMatcher(Source.EMPTY, nameField, matcher);
                    if (matcherCond != null) {
                        conditions.add(combineAnd(List.of(new IsNotNull(Source.EMPTY, nameField), matcherCond)));
                    }
                }
                // A matcher that accepts every metric name adds no useful filter.
                continue;
            }

            // Regular label matcher, e.g. job="myjob".
            Expression labelField = new UnresolvedAttribute(Source.EMPTY, matcher.name());
            Expression cond = TranslatePromqlToEsqlPlan.translateLabelMatcher(Source.EMPTY, labelField, matcher);
            if (cond != null) {
                conditions.add(cond);
            }
        }

        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    /**
     * Converts an InstantSelector's LabelMatchers into a single AND expression for pre-info filtering.
     * Returns {@code null} if all matchers are handled post-info or match everything.
     *
     * <p>Special handling for {@code __name__}:
     * <ul>
     *   <li>EQ (e.g. {@code {__name__="up"}}): emits {@code IsNotNull(series)} — checks the metric
     *       field itself exists, which works for both Prometheus ({@code labels.__name__} present) and
     *       OTel (field named "up" exists). The parser always provides a non-null {@code series()} for
     *       EQ.</li>
     *   <li>NEQ / REG / NREG: adds a nullable {@code labels.__name__} pre-filter hint and also
     *       evaluates post-info against {@link #METRIC_NAME_FIELD}.</li>
     * </ul>
     */
    static Expression buildPreInfoSelectorCondition(InstantSelector selector) {
        List<Expression> conditions = new ArrayList<>();
        for (LabelMatcher matcher : selector.labelMatchers().matchers()) {
            if (LabelMatcher.NAME.equals(matcher.name())) {
                if (matcher.matcher() == LabelMatcher.Matcher.EQ) {
                    // Parser contract: EQ __name__ always carries a non-null series expression
                    assert selector.series() != null : "EQ __name__ matcher should always have a non-null series";
                    conditions.add(new IsNotNull(Source.EMPTY, selector.series()));
                } else if (matcher.matchesAll() == false) {
                    conditions.add(buildNullableNameHint(matcher));
                }
            } else {
                Expression cond = TranslatePromqlToEsqlPlan.translateLabelMatcher(
                    Source.EMPTY,
                    new UnresolvedAttribute(Source.EMPTY, matcher.name()),
                    matcher
                );
                if (cond != null) {
                    conditions.add(cond);
                }
            }
        }
        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    private static Expression buildNullableNameHint(LabelMatcher matcher) {
        Expression nameField = new UnresolvedAttribute(Source.EMPTY, LabelMatcher.NAME);
        Expression matcherCond = TranslatePromqlToEsqlPlan.translateLabelMatcher(Source.EMPTY, nameField, matcher);
        return combineOr(List.of(new IsNull(Source.EMPTY, nameField), matcherCond));
    }

    /**
     * Converts an InstantSelector's non-exact {@code __name__} matchers into a single AND expression
     * evaluated against {@link #METRIC_NAME_FIELD}. Returns {@code null} if no such matchers exist.
     *
     * <p>No {@code IsNotNull} wrapper is added: {@link TranslatePromqlToEsqlPlan#translateLabelMatcher}
     * already handles the null/empty-string case by emitting {@code IsNull(field) OR NOT(matcher)}
     * when the matcher automaton accepts the empty string (e.g. for NEQ). Adding an outer
     * {@code IsNotNull} would cancel that {@code IsNull} branch and incorrectly exclude series whose
     * {@code metric_name} is null.
     */
    static Expression buildPostInfoSelectorCondition(InstantSelector selector) {
        List<Expression> conditions = new ArrayList<>();
        Expression metricNameField = new UnresolvedAttribute(Source.EMPTY, METRIC_NAME_FIELD);
        for (LabelMatcher matcher : selector.labelMatchers().matchers()) {
            if (LabelMatcher.NAME.equals(matcher.name()) && matcher.matcher() != LabelMatcher.Matcher.EQ && matcher.matchesAll() == false) {
                Expression matcherCond = translateMetricNameMatcher(metricNameField, matcher);
                conditions.add(matcherCond);
            }
        }
        return conditions.isEmpty() ? null : combineAnd(conditions);
    }

    private static Expression translateMetricNameMatcher(Expression metricNameField, LabelMatcher matcher) {
        Expression rawMetricNameCondition = TranslatePromqlToEsqlPlan.translateLabelMatcher(Source.EMPTY, metricNameField, matcher);
        Expression prefixedMetricNameCondition = TranslatePromqlToEsqlPlan.translateLabelMatcher(
            Source.EMPTY,
            metricNameField,
            prefixedMetricNameMatcher(matcher)
        );
        return switch (matcher.matcher()) {
            case REG -> combineOr(List.of(rawMetricNameCondition, prefixedMetricNameCondition));
            case NEQ, NREG -> combineAnd(List.of(rawMetricNameCondition, prefixedMetricNameCondition));
            case EQ -> throw new IllegalArgumentException("exact __name__ matchers are handled before info nodes");
        };
    }

    private static LabelMatcher prefixedMetricNameMatcher(LabelMatcher matcher) {
        List<String> values = matcher.values().stream().map(value -> prefixedMetricNameValue(value, matcher.matcher())).toList();
        return new LabelMatcher(matcher.name(), values, matcher.matcher());
    }

    private static String prefixedMetricNameValue(String value, LabelMatcher.Matcher matcher) {
        if (matcher.isRegex()) {
            return METRICS_PREFIX_REGEX + value + ")";
        }
        return METRICS_PREFIX + value;
    }

    /**
     * Builds {@code @timestamp >= start AND @timestamp <= end}.
     */
    static Expression buildTimeCondition(Instant start, Instant end) {
        Expression ts = new UnresolvedTimestamp(Source.EMPTY);
        Expression ge = new GreaterThanOrEqual(Source.EMPTY, ts, Literal.dateTime(Source.EMPTY, start), ZoneOffset.UTC);
        Expression le = new LessThanOrEqual(Source.EMPTY, ts, Literal.dateTime(Source.EMPTY, end), ZoneOffset.UTC);
        return combineAnd(List.of(ge, le));
    }

    private static Expression buildPreInfoCondition(List<InstantSelector> selectors) {
        List<Expression> preConditions = new ArrayList<>();
        for (InstantSelector selector : selectors) {
            Expression condition = buildPreInfoSelectorCondition(selector);
            if (condition == null) {
                return null;
            }
            preConditions.add(condition);
        }
        return combineSelectorConditions(preConditions);
    }

    private static Expression buildPostInfoCondition(List<InstantSelector> selectors) {
        List<Expression> postConditions = new ArrayList<>();
        for (InstantSelector selector : selectors) {
            Expression condition = buildPostInfoSelectorCondition(selector);
            if (condition == null) {
                // Applying a post-info filter for only some repeated selectors would turn
                // (pre_a AND post_a) OR pre_b into (pre_a OR pre_b) AND post_a.
                return null;
            }
            postConditions.add(condition);
        }
        return combineSelectorConditions(postConditions);
    }

    private static Expression combineSelectorConditions(List<Expression> conditions) {
        if (conditions.isEmpty()) {
            return null;
        }
        return conditions.size() == 1 ? conditions.get(0) : combineOr(conditions);
    }

    /**
     * Builds a {@code TS -> Filter -> info node} plan. Repeated selectors are combined in one
     * pre-info OR filter. For non-exact {@code __name__} matchers we add a nullable
     * {@code labels.__name__} hint: metrics that carry {@code __name__} are filtered before the
     * expensive info node, while metrics without that label can still reach the post-info
     * {@code metric_name} filter.
     *
     * <p>For Prometheus data, where {@code labels.__name__} is present, the pre-info OR preserves
     * repeated {@code match[]} grouping exactly. The post-info {@code metric_name} filter is applied
     * only when every repeated selector has such a condition; otherwise it is skipped so selectors
     * without {@code __name__} constraints are not filtered out.
     *
     * <p>For OTel data without {@code labels.__name__}, single-selector requests remain exact via
     * the post-info {@code metric_name} filter. Repeated {@code match[]} requests that mix pre-info
     * labels and non-exact {@code __name__} constraints are approximate because a single linear plan
     * cannot preserve each selector's full grouping without a union.
     */
    static LogicalPlan buildFilteredInfoPlan(
        String index,
        List<InstantSelector> selectors,
        Instant start,
        Instant end,
        Function<LogicalPlan, LogicalPlan> infoNode
    ) {
        Expression preInfoCondition = buildPreInfoCondition(selectors);
        Expression postInfoCondition = buildPostInfoCondition(selectors);
        LogicalPlan plan = tsSource(index);
        List<Expression> preFilterParts = new ArrayList<>();
        preFilterParts.add(buildTimeCondition(start, end));
        if (preInfoCondition != null) {
            preFilterParts.add(preInfoCondition);
        }
        plan = new Filter(Source.EMPTY, plan, combineAnd(preFilterParts));
        plan = infoNode.apply(plan);
        if (postInfoCondition != null) {
            plan = new Filter(Source.EMPTY, plan, postInfoCondition);
        }
        return plan;
    }
}
