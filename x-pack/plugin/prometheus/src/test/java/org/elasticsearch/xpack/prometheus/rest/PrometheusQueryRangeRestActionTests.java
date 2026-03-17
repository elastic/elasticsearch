/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PrometheusQueryRangeRestActionTests extends ESTestCase {

    public void testPrepareRequestMissingParams() {
        PrometheusQueryRangeRestAction action = new PrometheusQueryRangeRestAction();
        Map<String, String> allParams = Map.of(
            "query",
            "up",
            "start",
            "2026-01-01T00:00:00Z",
            "end",
            "2026-01-01T01:00:00Z",
            "step",
            "15s"
        );

        for (String missingParam : allParams.keySet()) {
            Map<String, String> params = new HashMap<>(allParams);
            params.remove(missingParam);
            RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
            assertThat(e.getMessage(), equalTo("required parameter \"" + missingParam + "\" is missing"));
        }
    }

    public void testBuildStatementPlanStructure() {
        EsqlStatement statement = PrometheusQueryRangeRestAction.buildStatement(
            "up",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(Project.class));
        Project project = (Project) statement.plan();
        assertThat(project.child(), instanceOf(Eval.class));
        Eval eval = (Eval) project.child();
        assertThat(eval.fields().size(), equalTo(1));
        assertThat(eval.fields().get(0).name(), equalTo("step"));
        assertThat(eval.child(), instanceOf(PromqlCommand.class));
        PromqlCommand promqlCommand = (PromqlCommand) eval.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
        assertThat(promqlCommand.isRangeQuery(), equalTo(true));
    }

    public void testBuildStatementWithCustomIndex() {
        EsqlStatement statement = PrometheusQueryRangeRestAction.buildStatement(
            "up",
            "logs-*,metrics-*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(statement.plan(), instanceOf(Project.class));
        Project project = (Project) statement.plan();
        Eval eval = (Eval) project.child();
        PromqlCommand promqlCommand = (PromqlCommand) eval.child();
        assertThat(promqlCommand.valueColumnName(), equalTo("value"));
    }

    public void testBuildStatementWithNumericStep() {
        EsqlStatement statement = PrometheusQueryRangeRestAction.buildStatement("up", "*", "1735689600", "1735693200", "60");
        assertThat(statement.plan(), instanceOf(Project.class));
        Project project = (Project) statement.plan();
        assertThat(project.child(), instanceOf(Eval.class));
        Eval eval = (Eval) project.child();
        assertThat(eval.child(), instanceOf(PromqlCommand.class));
    }
}
