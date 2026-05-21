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
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;

import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class PromqlQueryPlanBuilderTests extends ESTestCase {

    public void testBuildStatementPlanStructure() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildRangeStatement(
            "up",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        // Top-level plan is Eval (no OrderBy — timestamps are chronologically ordered by construction)
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
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
        EsqlStatement statement = PromqlQueryPlanBuilder.buildRangeStatement(
            "up",
            "metrics-*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("metrics-*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(15)));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildStatementWithGroupByAbsentLabel() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildRangeStatement(
            "sum(rate(http_request_duration_microseconds_count[1m])) by (handler)",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        assertThat(((TimeSeriesCollapse) eval.child()).child(), instanceOf(PromqlCommand.class));
    }

    public void testBuildStatementWithNumericStep() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildRangeStatement("up", "*", "1735689600", "1735693200", "60");
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
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
        EsqlStatement statement = PromqlQueryPlanBuilder.buildRangeStatement("up", "*", "1735689600", "1735693200", "60", 10);
        assertThat(statement.plan(), instanceOf(Limit.class));
        Limit limit = (Limit) statement.plan();
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        assertThat(limit.child(), instanceOf(Eval.class));
        assertThat(((Eval) limit.child()).child(), instanceOf(TimeSeriesCollapse.class));
    }

    public void testBuildInstantStatementPlanStructure() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        EsqlStatement statement = PromqlQueryPlanBuilder.buildInstantStatement("up", "*", evaluationTime);
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
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
        EsqlStatement statement = PromqlQueryPlanBuilder.buildInstantStatement("up", "*", evaluationTime);
        PromqlCommand promqlCommand = (PromqlCommand) ((TimeSeriesCollapse) ((Eval) statement.plan()).child()).child();

        assertThat(promqlCommand.isInstantQuery(), equalTo(true));
        assertThat(promqlCommand.isRangeQuery(), equalTo(false));
        assertThat(promqlCommand.start().value(), equalTo(evaluationTime.toEpochMilli()));
        assertThat(promqlCommand.end().value(), equalTo(evaluationTime.toEpochMilli()));
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(PromqlCommand.DEFAULT_LOOKBACK));
    }

    public void testBuildStatementUsesRangeSelectorWindowForSourceFilter() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        EsqlStatement statement = PromqlQueryPlanBuilder.buildInstantStatement("rate(up[10m])", "*", evaluationTime);
        Eval eval = (Eval) statement.plan();
        TimeSeriesCollapse collapse = (TimeSeriesCollapse) eval.child();
        PromqlCommand promqlCommand = (PromqlCommand) collapse.child();
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(Duration.ofMinutes(10)));
    }

    public void testSourceFilterWindowFloorsShortRangeSelectorToDefaultLookback() {
        Instant evaluationTime = Instant.parse("2025-01-01T00:05:00Z");
        EsqlStatement statement = PromqlQueryPlanBuilder.buildInstantStatement("rate(up[1m])", "*", evaluationTime);
        Eval eval = (Eval) statement.plan();
        PromqlCommand promqlCommand = (PromqlCommand) ((TimeSeriesCollapse) eval.child()).child();
        // sourceFilterWindow floors up to DEFAULT_LOOKBACK (5m) even when the explicit range is shorter
        assertThat(promqlCommand.sourceFilterWindow(), equalTo(PromqlCommand.DEFAULT_LOOKBACK));
        // resolveInstantQueryWindow respects the explicit 1m window without flooring
        assertThat(promqlCommand.resolveInstantQueryWindow(), equalTo(Duration.ofMinutes(1)));
    }
}
