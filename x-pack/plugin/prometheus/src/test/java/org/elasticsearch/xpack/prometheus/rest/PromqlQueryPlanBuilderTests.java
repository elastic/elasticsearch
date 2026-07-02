/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatementResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class PromqlQueryPlanBuilderTests extends ESTestCase {

    public void testBuildStatementPlanStructure() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        // Top-level plan is Eval (no OrderBy — timestamps are chronologically ordered by construction)
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        assertThat(collapse.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
        assertThat(promqlCommand.isRangeQuery(), equalTo(true));
        assertThat(promqlCommand.child(), instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(15)));
        assertThat(promqlCommand.promqlPlan(), instanceOf(InstantSelector.class));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildStatementWithCustomIndex() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "metrics-*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("metrics-*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(15)));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildStatementWithGroupByAbsentLabel() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "sum(rate(http_request_duration_microseconds_count[1m])) by (handler)",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        assertThat(((TimeSeriesCollapse) eval.child()).child(), instanceOf(PromqlCommand.class));
    }

    public void testBuildStatementWithNumericStep() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60", QueryMode.RANGE);
        assertThat(result.resultType(), equalTo("matrix"));
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        assertThat(collapse.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(60)));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildRangeStatementWithLimitAddsSentinelLimit() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "*",
            "1735689600",
            "1735693200",
            "60",
            10,
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        assertThat(result.esqlStatement().plan(), instanceOf(Limit.class));
        Limit limit = (Limit) result.esqlStatement().plan();
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        assertThat(limit.child(), instanceOf(Eval.class));
        assertThat(((Eval) limit.child()).child(), instanceOf(TimeSeriesCollapse.class));
    }

    public void testBuildInstantStatementPlanStructure() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("up", "*", evaluationTime, QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.child(), instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("*"));
        assertThat(promqlCommand.step().value(), nullValue());
        assertThat(promqlCommand.buckets().value(), nullValue());
        assertThat(promqlCommand.promqlPlan(), instanceOf(InstantSelector.class));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildInstantStatementIsNativeInstantQuery() {
        Instant evaluationTime = Instant.parse("2026-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("up", "*", evaluationTime, QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
        PromqlCommand promqlCommand = (PromqlCommand) ((TimeSeriesCollapse) ((Eval) result.esqlStatement().plan()).child()).child();

        assertThat(promqlCommand.isInstantQuery(), equalTo(true));
        assertThat(promqlCommand.isRangeQuery(), equalTo(false));
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
        assertThat(promqlCommand.end().value(), equalTo(evaluationTime.toEpochMilli()));
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(PromqlCommand.DEFAULT_LOOKBACK));
    }

    public void testBuildStatementUsesRangeSelectorWindowForSourceFilter() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("rate(up[10m])", "*", evaluationTime, QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
        Eval eval = (Eval) result.esqlStatement().plan();
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(Duration.ofMinutes(10)));
    }

    public void testSourceFilterWindowFloorsShortRangeSelectorToDefaultLookback() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("rate(up[1m])", "*", evaluationTime, QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
        Eval eval = (Eval) result.esqlStatement().plan();
        PromqlCommand promqlCommand = (PromqlCommand) ((TimeSeriesCollapse) eval.child()).child();
        // sourceFilterWindow floors up to DEFAULT_LOOKBACK (5m) even when the explicit range is shorter
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(PromqlCommand.DEFAULT_LOOKBACK));
        // resolveInstantQueryWindow respects the explicit 1m window without flooring
        assertThat(promqlCommand.resolveInstantQueryWindow(), equalTo(Duration.ofMinutes(1)));
    }

    public void testBuildInstantScalarStatementResultType() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("time()", "*", evaluationTime, QueryMode.INSTANT);
        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "scalar", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    public void testBuildInstantFoldableTimeArithmeticStatementUsesPromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("time() + 60", "*", evaluationTime, QueryMode.INSTANT);
        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "scalar", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    public void testBuildInstantFoldableTimeExtractionStatementUsesPromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T05:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("hour()", "*", evaluationTime, QueryMode.INSTANT);
        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "vector", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    public void testBuildInstantFoldableValueTransformationStatementUsesPromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "round(vector(1.23), 0.1)",
            "*",
            evaluationTime,
            QueryMode.INSTANT
        );
        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "vector", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    public void testBuildInstantFoldableScalarStatementUsesPromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("1 * 2 + 4 / 2", "*", evaluationTime, QueryMode.INSTANT);

        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "scalar", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    public void testBuildInstantScalarLiteralStatementsUsePromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        record Case(String query, String expectedType) {}
        List<Case> cases = List.of(
            new Case("42", "scalar"),
            new Case("1.234", "scalar"),
            new Case(".123", "scalar"),
            new Case("1.23e-3", "scalar"),
            new Case("0x3d", "scalar"),
            new Case("Inf", "scalar"),
            new Case("+Inf", "scalar"),
            new Case("-Inf", "scalar"),
            new Case("NaN", "scalar")
        );

        for (Case c : cases) {
            PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(c.query(), "*", evaluationTime, QueryMode.INSTANT);

            assertPromqlCommandStatement(result, c.expectedType(), false);
        }
    }

    public void testBuildRangeScalarStatementUsesMatrixResultType() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "time()",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:00:45Z",
            "15s",
            QueryMode.RANGE
        );
        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "matrix", true);
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(15)));
    }

    public void testBuildRangeFoldableTimeArithmeticStatementUsesPromqlCommand() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "time() + 1",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:02:00Z",
            "60",
            QueryMode.RANGE
        );

        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "matrix", true);
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(60)));
    }

    public void testBuildRangeFoldableScalarStatementUsesPromqlCommand() {
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement(
            "42",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:02:00Z",
            "60",
            QueryMode.RANGE
        );

        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "matrix", true);
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(60)));
    }

    public void testBuildFoldablePiStatementUsesPromqlCommand() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        PromqlStatementResult result = PromqlQueryPlanBuilder.buildStatement("pi()", "*", evaluationTime, QueryMode.INSTANT);

        PromqlCommand promqlCommand = assertPromqlCommandStatement(result, "scalar", false);
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
    }

    private static PromqlCommand assertPromqlCommandStatement(PromqlStatementResult result, String resultType, boolean rangeQuery) {
        assertThat(result.resultType(), equalTo(resultType));
        assertThat(result.esqlStatement().plan(), instanceOf(Eval.class));
        Eval eval = (Eval) result.esqlStatement().plan();
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        assertThat(collapse.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.isRangeQuery(), equalTo(rangeQuery));
        return promqlCommand;
    }
}
