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
import org.elasticsearch.xpack.prometheus.rest.PrometheusQueryResponseListener.QueryMode;
import org.elasticsearch.xpack.prometheus.rest.PromqlQueryPlanBuilder.PromqlStatement;

import java.time.Duration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlQueryPlanBuilderTests extends ESTestCase {

    public void testBuildStatementPlanStructure() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        // Top-level plan is Eval (no OrderBy — timestamps are chronologically ordered by construction)
        EsqlStatement statement = result.esqlStatement();
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
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "metrics-*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        assertThat(result.resultType(), equalTo("matrix"));
        EsqlStatement statement = result.esqlStatement();
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
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement(
            "sum(rate(http_request_duration_microseconds_count[1m])) by (handler)",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s",
            QueryMode.RANGE
        );
        // Return type defaults to "matrix" for unresolved functions in range queries
        assertThat(result.resultType(), equalTo("matrix"));
        EsqlStatement statement = result.esqlStatement();
        assertThat(statement.plan(), instanceOf(Eval.class));
        Eval eval = (Eval) statement.plan();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(TimeSeriesCollapse.class));
        assertThat(((TimeSeriesCollapse) eval.child()).child(), instanceOf(PromqlCommand.class));
    }

    public void testBuildStatementWithNumericStep() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60", QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
        EsqlStatement statement = result.esqlStatement();
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

    public void testBuildStatementWithLimitAddsSentinelLimit() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60", 10, QueryMode.RANGE);
        assertThat(result.resultType(), equalTo("matrix"));
        EsqlStatement statement = result.esqlStatement();
        assertThat(statement.plan(), instanceOf(Limit.class));
        Limit limit = (Limit) statement.plan();
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        assertThat(limit.child(), instanceOf(Eval.class));
        assertThat(((Eval) limit.child()).child(), instanceOf(TimeSeriesCollapse.class));
    }

    public void testBuildStatementScalarReturnsScalarType() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement("42", "*", "1735689600", "1735693200", "60", QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("scalar"));
    }

    public void testBuildStatementInstantVectorInRangeReturnsMatrix() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60", QueryMode.RANGE);
        assertThat(result.resultType(), equalTo("matrix"));
    }

    public void testBuildStatementInstantVectorInInstantReturnsVector() {
        PromqlStatement result = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60", QueryMode.INSTANT);
        assertThat(result.resultType(), equalTo("vector"));
    }
}
