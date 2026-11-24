/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PrometheusPlanBuilderUtilsTests extends ESTestCase {

    public void testParseInstantSelectorsRejectsRangeSelector() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusPlanBuilderUtils.parseInstantSelectors(List.of("up[5m]"))
        );
        assertThat(ex.getMessage(), containsString("match[] selector must be an instant vector selector, got: [up[5m]]"));
    }

    public void testParseInstantSelectorsRejectsInvalidSyntax() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusPlanBuilderUtils.parseInstantSelectors(List.of("{not valid!!!}"))
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "Invalid match[] selector [{not valid!!!}]: line 1:6: mismatched input 'valid' expecting {'!=', '=', '=~', '!~', '}', ','}"
            )
        );
    }

    public void testParseInstantSelectorsRejectsOversizedBatch() {
        // Individually valid selectors whose operator count exceeds the limit in total
        String oneOp = "up == 1";
        List<String> batch = Collections.nCopies(PromqlParser.MAX_BINARY_OPERATORS + 1, oneOp);
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusPlanBuilderUtils.parseInstantSelectors(batch)
        );
        assertThat(ex.getMessage(), containsString("maximum number of binary operators allowed in total"));
    }

    public void testParseInstantSelectorsAcceptsBatchWithinLimits() {
        List<InstantSelector> selectors = PrometheusPlanBuilderUtils.parseInstantSelectors(List.of("up", "{job=\"api\"}"));
        assertThat(selectors.size(), is(2));
    }

    public void testBuildPreInfoSelectorConditionReturnsIsNotNullForBareMetricName() {
        InstantSelector selector = parseInstantSelector("up");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector);
        assertThat(cond, instanceOf(IsNotNull.class));
        assertThat(PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(selector), nullValue());
    }

    public void testBuildPreInfoSelectorConditionForSingleLabelMatcher() {
        InstantSelector selector = parseInstantSelector("{job=\"myjob\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector);
        assertThat(cond, instanceOf(Equals.class));
        assertThat(containsAttribute(cond, "job"), is(true));
    }

    public void testBuildPreInfoSelectorConditionForMultipleLabelMatchers() {
        InstantSelector selector = parseInstantSelector("{job=\"myjob\",env=\"prod\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector);
        assertThat(cond, instanceOf(And.class));
        assertThat(containsAttribute(cond, "job"), is(true));
        assertThat(containsAttribute(cond, "env"), is(true));
    }

    public void testBuildPreInfoSelectorConditionForNameEqMatcherUsesIsNotNull() {
        InstantSelector selector = parseInstantSelector("{__name__=\"up\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector);
        assertThat(cond, instanceOf(IsNotNull.class));
    }

    public void testBuildPreInfoSelectorConditionAddsNullableRegexNameHint() {
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector);
        assertThat(cond, notNullValue());
        assertThat(containsAttribute(cond, "__name__"), is(true));
    }

    public void testBuildPostInfoSelectorConditionForNameRegexMatcherUsesMetricName() {
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(selector);
        assertThat(containsAttribute(cond, PrometheusPlanBuilderUtils.METRIC_NAME_FIELD), is(true));
        assertThat(containsAttribute(cond, "__name__"), is(false));
    }

    /**
     * A metric stored as {@code metrics.<name>} is matched by its name without the prefix, so the {@code ^} of the regex
     * holds at the start of the name. A name of eight other characters before the same suffix has no prefix to take off.
     */
    public void testBuildPostInfoSelectorConditionMatchesPrefixedNameWithoutThePrefix() {
        for (String regex : List.of("^system\\..*", "^system\\.(cpu|memory)\\..*")) {
            String escaped = regex.replace("\\", "\\\\");
            Expression matches = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(
                parseInstantSelector("{__name__=~\"" + escaped + "\"}")
            );
            Expression doesNotMatch = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(
                parseInstantSelector("{__name__!~\"" + escaped + "\", job=\"a\"}")
            );
            for (String name : List.of("system.cpu.time", "metrics.system.cpu.time")) {
                assertThat(regex + " on " + name, matchesMetricName(matches, name), is(true));
                assertThat(regex + " on " + name, matchesMetricName(doesNotMatch, name), is(false));
            }
            for (String name : List.of("abcdefghsystem.cpu.time", "metrics.xsystem.cpu.time", "metrics.")) {
                assertThat(regex + " on " + name, matchesMetricName(matches, name), is(false));
                assertThat(regex + " on " + name, matchesMetricName(doesNotMatch, name), is(true));
            }
        }
    }

    /** A literal is compared with the prefix prepended, which needs no regex. */
    public void testBuildPostInfoSelectorConditionComparesPrefixedNameForLiteral() {
        Expression cond = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(parseInstantSelector("{__name__!=\"up\", job=\"a\"}"));
        assertThat(containsExpressionOfType(cond, Substring.class), is(false));
        assertThat(matchesMetricName(cond, "up"), is(false));
        assertThat(matchesMetricName(cond, "metrics.up"), is(false));
        assertThat(matchesMetricName(cond, "down"), is(true));
        assertThat(matchesMetricName(cond, "abcdefghup"), is(true));
    }

    /** Evaluates the condition on a metric named {@code name}. */
    private static boolean matchesMetricName(Expression condition, String name) {
        Expression bound = condition.transformUp(
            UnresolvedAttribute.class,
            a -> a.name().equals(PrometheusPlanBuilderUtils.METRIC_NAME_FIELD) ? Literal.keyword(Source.EMPTY, name) : a
        );
        return Boolean.TRUE.equals(bound.fold(FoldContext.small()));
    }

    public void testBuildPostInfoSelectorConditionForNameRegexMatcherDoesNotWrapWithIsNotNull() {
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(selector);
        assertThat(containsExpressionOfType(cond, IsNotNull.class), is(false));
    }

    public void testBuildPostInfoSelectorConditionForNEQMatcherDoesNotWrapWithIsNotNull() {
        LabelMatcher neqMatcher = new LabelMatcher(LabelMatcher.NAME, "up", LabelMatcher.Matcher.NEQ);
        InstantSelector selector = new InstantSelector(
            Source.EMPTY,
            null,
            List.of(),
            new LabelMatchers(List.of(neqMatcher)),
            Evaluation.NONE
        );
        Expression cond = PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(selector);
        assertThat(containsAttribute(cond, PrometheusPlanBuilderUtils.METRIC_NAME_FIELD), is(true));
        assertThat(containsExpressionOfType(cond, IsNotNull.class), is(false));
    }

    public void testBuildPreInfoSelectorConditionEmptyLabelMatchersReturnsNull() {
        InstantSelector selector = new InstantSelector(Source.EMPTY, null, List.of(), LabelMatchers.EMPTY, Evaluation.NONE);
        assertThat(PrometheusPlanBuilderUtils.buildPreInfoSelectorCondition(selector), nullValue());
        assertThat(PrometheusPlanBuilderUtils.buildPostInfoSelectorCondition(selector), nullValue());
    }

    public void testBuildPreInfoSelectorConditionWithNameFallbackForRegexUsesNameAttribute() {
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorConditionWithNameFallback(selector);
        assertThat(containsAttribute(cond, "__name__"), is(true));
    }

    public void testBuildPreInfoSelectorConditionWithNameFallbackForNEQUsesNameAttribute() {
        InstantSelector selector = parseInstantSelector("{job=~\".+\", __name__!=\"up\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildPreInfoSelectorConditionWithNameFallback(selector);
        assertThat(containsAttribute(cond, "__name__"), is(true));
        assertThat(containsAttribute(cond, "job"), is(true));
    }

    public void testBuildFilteredInfoPlanCombinesMultipleSelectors() {
        List<InstantSelector> selectors = PrometheusPlanBuilderUtils.parseInstantSelectors(
            List.of("{job=\"api\"}", "{__name__=~\"http.*\"}")
        );
        LogicalPlan plan = PrometheusPlanBuilderUtils.buildFilteredInfoPlan(
            "*",
            selectors,
            Instant.EPOCH,
            Instant.EPOCH.plusSeconds(60),
            child -> new TsInfo(Source.EMPTY, child)
        );

        assertThat(containsAttribute(plan, "job"), is(true));
        assertThat(containsAttribute(plan, "__name__"), is(true));
    }

    private static InstantSelector parseInstantSelector(String selector) {
        PromqlParser parser = new PromqlParser();
        LogicalPlan parsed = parser.createStatement(selector);
        assertThat("Expected InstantSelector for [" + selector + "]", parsed, instanceOf(InstantSelector.class));
        return (InstantSelector) parsed;
    }

    private static boolean containsAttribute(Expression expr, String name) {
        if (expr instanceof UnresolvedAttribute attr && name.equals(attr.name())) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsAttribute(child, name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAttribute(LogicalPlan plan, String name) {
        for (Expression expression : plan.expressions()) {
            if (containsAttribute(expression, name)) {
                return true;
            }
        }
        for (LogicalPlan child : plan.children()) {
            if (containsAttribute(child, name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsExpressionOfType(Expression expr, Class<? extends Expression> type) {
        if (type.isInstance(expr)) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsExpressionOfType(child, type)) {
                return true;
            }
        }
        return false;
    }
}
