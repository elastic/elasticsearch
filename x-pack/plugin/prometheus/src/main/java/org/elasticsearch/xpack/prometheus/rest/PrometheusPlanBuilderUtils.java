/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineAnd;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineOr;

/**
 * Shared plan-building utilities for Prometheus REST handlers.
 */
final class PrometheusPlanBuilderUtils {

    /** Column produced by {@link org.elasticsearch.xpack.esql.plan.logical.TsInfo} that lists the dimension field names. */
    static final String DIMENSION_FIELDS = "dimension_fields";

    private PrometheusPlanBuilderUtils() {}

    /**
     * Returns an {@link UnresolvedRelation} for the given index pattern using the {@code TS} source command.
     */
    static UnresolvedRelation tsSource(String index) {
        IndexPattern pattern = new IndexPattern(Source.EMPTY, index);
        return new UnresolvedRelation(Source.EMPTY, pattern, false, List.of(), null, SourceCommand.TS);
    }

    /**
     * Builds a filter expression combining a time-range condition with optional selector conditions.
     *
     * @param matchSelectors PromQL instant vector selectors (may be empty)
     * @param start          start of the time range (inclusive)
     * @param end            end of the time range (inclusive)
     * @return a single {@link Expression} suitable for use in a {@link org.elasticsearch.xpack.esql.plan.logical.Filter} node
     */
    static Expression filterExpression(List<String> matchSelectors, Instant start, Instant end) {
        List<Expression> allParts = new ArrayList<>();
        allParts.add(buildTimeCondition(start, end));
        List<Expression> selectorConditions = parseSelectorConditions(matchSelectors);
        if (selectorConditions.isEmpty() == false) {
            allParts.add(combineOr(selectorConditions));
        }
        return allParts.size() == 1 ? allParts.get(0) : combineAnd(allParts);
    }

    /**
     * Parses each {@code match[]} selector string into an ESQL {@link Expression} condition,
     * delegating per-selector translation to {@link #buildSelectorCondition(InstantSelector)}.
     *
     * @param matchSelectors PromQL selector strings (may be empty)
     * @return list of per-selector conditions; empty if {@code matchSelectors} is empty
     * @throws IllegalArgumentException if a selector is syntactically invalid or not an instant vector selector
     */
    static List<Expression> parseSelectorConditions(List<String> matchSelectors) {
        List<Expression> selectorConditions = new ArrayList<>();
        PromqlParser parser = new PromqlParser();
        for (String selector : matchSelectors) {
            LogicalPlan parsed;
            try {
                parsed = parser.createStatement(selector);
            } catch (ParsingException e) {
                throw new IllegalArgumentException("Invalid match[] selector [" + selector + "]: " + e.getMessage(), e);
            }
            if (parsed instanceof InstantSelector instantSelector) {
                Expression cond = buildSelectorCondition(instantSelector);
                if (cond != null) {
                    selectorConditions.add(cond);
                }
            } else {
                throw new IllegalArgumentException("match[] selector must be an instant vector selector, got: [" + selector + "]");
            }
        }
        return selectorConditions;
    }

    /**
     * Converts an InstantSelector's LabelMatchers into a single AND expression.
     * Returns {@code null} if all matchers match everything (e.g. bare metric name with no labels).
     *
     * <p>Special handling for {@code __name__}:
     * <ul>
     *   <li>EQ (e.g. {@code {__name__="up"}}): emits {@code IsNotNull(series)} — checks the metric
     *       field itself exists, which works for both Prometheus ({@code labels.__name__} present) and
     *       OTel (field named "up" exists). The parser always provides a non-null {@code series()} for
     *       EQ.</li>
     *   <li>NEQ / REG / NREG whose automaton does not match all strings: falls back to filtering on
     *       {@code __name__}. OTel metrics that lack this label will be excluded — unavoidable,
     *       as we have no way to enumerate all field names by regex or negation.</li>
     *   <li>NEQ / REG / NREG whose automaton matches all strings (e.g. {@code =~".*"}): no constraint
     *       is emitted — the constraint would always be satisfied, and omitting it also preserves
     *       OTel metrics that lack {@code __name__}.</li>
     * </ul>
     */
    static Expression buildSelectorCondition(InstantSelector selector) {
        List<Expression> conditions = new ArrayList<>();
        for (LabelMatcher matcher : selector.labelMatchers().matchers()) {
            if (LabelMatcher.NAME.equals(matcher.name())) {
                if (matcher.matcher() == LabelMatcher.Matcher.EQ) {
                    // Parser contract: EQ __name__ always carries a non-null series expression
                    assert selector.series() != null : "EQ __name__ matcher should always have a non-null series";
                    conditions.add(new IsNotNull(Source.EMPTY, selector.series()));
                } else if (matcher.matchesAll() == false) {
                    // NEQ / REG / NREG: use __name__ for filtering.
                    // OTel metrics that lack this label will be excluded — unavoidable, as we have no
                    // way to enumerate all field names by regex or negation.
                    Expression nameField = new UnresolvedAttribute(Source.EMPTY, "__name__");
                    Expression matcherCond = TranslatePromqlToEsqlPlan.translateLabelMatcher(Source.EMPTY, nameField, matcher);
                    conditions.add(combineAnd(List.of(new IsNotNull(Source.EMPTY, nameField), matcherCond)));
                }
                // matchesAll() == true: the automaton accepts every string (e.g. =~".*"), so this
                // constraint would always be satisfied — omitting it also preserves OTel metrics
                // that lack __name__.
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

    /**
     * Builds {@code @timestamp >= start AND @timestamp <= end}.
     */
    static Expression buildTimeCondition(Instant start, Instant end) {
        Expression ts = new UnresolvedTimestamp(Source.EMPTY);
        Expression ge = new GreaterThanOrEqual(Source.EMPTY, ts, Literal.dateTime(Source.EMPTY, start), ZoneOffset.UTC);
        Expression le = new LessThanOrEqual(Source.EMPTY, ts, Literal.dateTime(Source.EMPTY, end), ZoneOffset.UTC);
        return combineAnd(List.of(ge, le));
    }
}
