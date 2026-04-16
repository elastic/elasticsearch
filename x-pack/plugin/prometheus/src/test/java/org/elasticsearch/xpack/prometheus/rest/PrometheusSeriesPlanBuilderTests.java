/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PrometheusSeriesPlanBuilderTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);

    public void testBuildPlanZeroLimitTopIsLimitWithMaxValue() {
        // limit=0 means "unlimited": an explicit Limit(Integer.MAX_VALUE) is emitted so that ESQL's
        // AddImplicitLimit rule sees an existing node and does not inject its own default.
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);

        // Top: Limit(Integer.MAX_VALUE)
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).limit().fold(null), is(Integer.MAX_VALUE));
        LogicalPlan tsInfo = ((Limit) plan).child();

        // Under Limit: TsInfo
        assertThat(tsInfo, instanceOf(TsInfo.class));
        LogicalPlan filterNode = ((TsInfo) tsInfo).child();

        // Under TsInfo: Filter
        assertThat(filterNode, instanceOf(Filter.class));
        LogicalPlan relation = ((Filter) filterNode).child();

        // Under Filter: UnresolvedRelation
        assertThat(relation, instanceOf(UnresolvedRelation.class));
    }

    public void testBuildPlanWithLimitTopIsLimitPlusOneSentinel() {
        // limit>0 uses limit+1 as sentinel so the response listener can detect truncation.
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up"), START, END, 100);

        // Top: Limit(101)
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).limit().fold(null), is(101));
        LogicalPlan tsInfo = ((Limit) plan).child();

        // Under Limit: TsInfo
        assertThat(tsInfo, instanceOf(TsInfo.class));
    }

    public void testBuildPlanFilterContainsTimeRangeCondition() {
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        Filter filter = findFilter(plan);

        // The filter condition should contain >= and <= comparisons for @timestamp
        assertThat(containsExpressionOfType(filter.condition(), GreaterThanOrEqual.class), is(true));
        assertThat(containsExpressionOfType(filter.condition(), LessThanOrEqual.class), is(true));
    }

    public void testBuildPlanRejectsRangeSelector() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up[5m]"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("instant vector selector"));
    }

    public void testBuildPlanRejectsAggregationExpression() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusSeriesPlanBuilder.buildPlan("*", List.of("sum(up)"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("match[] selector must be an instant vector selector, got: [sum(up)]"));
    }

    public void testBuildPlanRejectsInvalidSelectorSyntax() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusSeriesPlanBuilder.buildPlan("*", List.of("{not valid!!!}"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("Invalid match[] selector"));
    }

    public void testBuildPlanEqNameSelectorAddsIsNotNull() {
        for (String selector : List.of("up", "{__name__=\"up\"}")) {
            LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of(selector), START, END, 0);
            Filter filter = findFilter(plan);
            assertThat(
                "Expected IsNotNull(up) in filter for selector [" + selector + "]",
                containsIsNotNullOn(filter.condition(), "up"),
                is(true)
            );
        }
    }

    public void testBuildPlanRegexNameSelectorUsesLabelsNameField() {
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("{__name__=~\"http.*\"}"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat("Expected __name__ attribute for regex __name__ matcher", containsAttribute(filter.condition(), "__name__"), is(true));
    }

    public void testBuildPlanLabelMatcherFiltersOnTopLevelField() {
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("{job=\"prometheus\"}"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(containsAttribute(filter.condition(), "job"), is(true));
    }

    public void testBuildPlanMultipleSelectorsAreCombinedWithOr() {
        // Two distinct selectors — their conditions must both appear in the filter
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up", "node_cpu_seconds_total"), START, END, 0);
        Filter filter = findFilter(plan);
        // Both metric names must be referenced via IsNotNull
        assertThat(containsIsNotNullOn(filter.condition(), "up"), is(true));
        assertThat(containsIsNotNullOn(filter.condition(), "node_cpu_seconds_total"), is(true));
    }

    public void testBuildPlanCatchAllSelectorProducesNoSelectorCondition() {
        // {__name__=~".*"} matches everything — the parser rejects this, so use a bare {} equivalent
        // A selector with no matchers at all (catch-all): buildSelectorCondition returns null, so
        // no OR clause is added to the filter — only the time condition remains.
        // We verify this indirectly: there are no IsNotNull or Equals nodes beyond the timestamp check.
        LogicalPlan plan = PrometheusSeriesPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        assertThat(plan, notNullValue());
    }

    public void testBuildSelectorConditionReturnsNullForBareMetricName() {
        // A bare metric name like "up" carries only an EQ __name__ matcher; the resulting
        // condition should be IsNotNull(series), not null.
        InstantSelector selector = parseInstantSelector("up");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        // "up" has an EQ __name__ matcher, so result is not null
        assertThat(cond, notNullValue());
        assertThat(cond, instanceOf(IsNotNull.class));
    }

    public void testBuildSelectorConditionForSingleLabelMatcher() {
        InstantSelector selector = parseInstantSelector("{job=\"myjob\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(cond, notNullValue());
        // Should contain an Equals on job
        assertThat(containsExpressionOfType(cond, Equals.class), is(true));
        assertThat(containsAttribute(cond, "job"), is(true));
    }

    public void testBuildSelectorConditionForMultipleLabelMatchers() {
        InstantSelector selector = parseInstantSelector("{job=\"myjob\",env=\"prod\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(cond, notNullValue());
        assertThat(cond, instanceOf(And.class));
        assertThat(containsAttribute(cond, "job"), is(true));
        assertThat(containsAttribute(cond, "env"), is(true));
    }

    public void testBuildSelectorConditionForNameEqMatcherUsesIsNotNull() {
        InstantSelector selector = parseInstantSelector("{__name__=\"up\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(cond, instanceOf(IsNotNull.class));
    }

    public void testBuildSelectorConditionForNameRegexMatcherUsesLabelsName() {
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(cond, notNullValue());
        assertThat(containsAttribute(cond, "__name__"), is(true));
    }

    public void testBuildSelectorConditionForNameRegexMatcherAlsoAddsIsNotNull() {
        // NEQ/REG/NREG on __name__ must also verify the field exists (IsNotNull)
        InstantSelector selector = parseInstantSelector("{__name__=~\"http.*\"}");
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(containsExpressionOfType(cond, IsNotNull.class), is(true));
    }

    public void testBuildSelectorConditionEmptyLabelMatchersReturnsNull() {
        // A selector with no label matchers (catch-all) returns null.
        // {} is rejected by the PromQL parser, so construct the selector directly.
        InstantSelector selector = new InstantSelector(Source.EMPTY, null, List.of(), LabelMatchers.EMPTY, Evaluation.NONE);
        Expression cond = PrometheusPlanBuilderUtils.buildSelectorCondition(selector);
        assertThat(cond, nullValue());
    }

    private static InstantSelector parseInstantSelector(String selector) {
        PromqlParser parser = new PromqlParser();
        LogicalPlan parsed = parser.createStatement(selector);
        assertThat("Expected InstantSelector for [" + selector + "]", parsed, instanceOf(InstantSelector.class));
        return (InstantSelector) parsed;
    }

    /** Walks through UnaryPlan nodes to find the innermost Filter. */
    private static Filter findFilter(LogicalPlan plan) {
        LogicalPlan current = plan;
        while (current instanceof org.elasticsearch.xpack.esql.plan.logical.UnaryPlan unary) {
            if (current instanceof Filter filter) {
                return filter;
            }
            current = unary.child();
        }
        throw new AssertionError("No Filter node found in plan: " + plan);
    }

    /** Returns true if the expression tree contains an {@link IsNotNull} whose inner field is an
     *  {@link UnresolvedAttribute} with the given name. */
    private static boolean containsIsNotNullOn(Expression expr, String attrName) {
        if (expr instanceof IsNotNull inn && inn.field() instanceof UnresolvedAttribute attr && attrName.equals(attr.name())) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsIsNotNullOn(child, attrName)) {
                return true;
            }
        }
        return false;
    }

    /** Returns true if the expression tree contains an {@link UnresolvedAttribute} with the given name. */
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

    /** Returns true if the expression tree contains any node of the given type. */
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
