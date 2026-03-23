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
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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

    public void testEsqlQueryConstant() {
        assertThat(
            PrometheusQueryRangeRestAction.ESQL_QUERY,
            equalTo("PROMQL step=?step start=?start end=?end index=?index value=(?query) | EVAL step = TO_LONG(step)")
        );
    }

    public void testBuildQueryParams() {
        QueryParams params = PrometheusQueryRangeRestAction.buildQueryParams(
            "rate(http_requests_total[5m])",
            "*",
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "15s"
        );
        assertThat(params.get("query").value(), equalTo("rate(http_requests_total[5m])"));
        assertThat(params.get("index").value(), equalTo("*"));
        assertThat(params.get("start").value(), equalTo("2025-01-01T00:00:00Z"));
        assertThat(params.get("end").value(), equalTo("2025-01-01T01:00:00Z"));
        assertThat(params.get("step").value(), equalTo("15s"));
    }

}
