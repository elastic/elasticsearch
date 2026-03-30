/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;

import java.time.Duration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlQueryPlanBuilderTests extends ESTestCase {

    public void testBuildStatementPlanStructure() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement("up", "*", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z", "15s");
        assertThat(statement.plan(), instanceOf(OrderBy.class));
        Eval eval = (Eval) ((OrderBy) statement.plan()).child();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) eval.child();
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
        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement(
            "up",
            "metrics-*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(OrderBy.class));
        Eval eval = (Eval) ((OrderBy) statement.plan()).child();
        PromqlCommand promqlCommand = (PromqlCommand) eval.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("metrics-*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(15)));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }

    public void testBuildStatementWithGroupByAbsentLabel() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement(
            "sum(rate(http_request_duration_microseconds_count[1m])) by (handler)",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(OrderBy.class));
        Eval eval = (Eval) ((OrderBy) statement.plan()).child();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(PromqlCommand.class));
    }

    public void testBuildStatementWithNumericStep() {
        EsqlStatement statement = PromqlQueryPlanBuilder.buildStatement("up", "*", "1735689600", "1735693200", "60");
        assertThat(statement.plan(), instanceOf(OrderBy.class));
        Eval eval = (Eval) ((OrderBy) statement.plan()).child();
        assertThat(eval.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) eval.child();
        assertThat(((UnresolvedRelation) promqlCommand.child()).indexPattern().indexPattern(), equalTo("*"));
        assertThat(promqlCommand.hasTimeRange(), equalTo(true));
        assertThat(promqlCommand.step().value(), equalTo(Duration.ofSeconds(60)));
        assertThat(((NamedExpression) ((InstantSelector) promqlCommand.promqlPlan()).series()).name(), equalTo("up"));
    }
}
