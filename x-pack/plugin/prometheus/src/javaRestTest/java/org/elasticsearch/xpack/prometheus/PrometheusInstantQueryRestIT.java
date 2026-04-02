/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the Prometheus {@code /api/v1/query} instant query endpoint.
 */
public class PrometheusInstantQueryRestIT extends AbstractPrometheusRestIT {

    /**
     * Verifies that querying when no Prometheus indices exist returns an empty result instead of an error.
     */
    public void testInstantQueryWithNoPrometheusIndicesReturnsEmptyResult() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/query");
        request.addParameter("query", "nonexistent_metric");
        request.addParameter("time", "2026-01-01T00:05:00Z");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    public void testInstantQueryWithIngestedData() throws Exception {
        ingestTestData("test_gauge_iq");

        ObjectPath responsePath = executeInstantQuery(null);
        assertMetricResult(responsePath);
    }

    public void testInstantQueryWithIndexPattern() throws Exception {
        ingestTestData("test_gauge_iq");

        ObjectPath responsePath = executeInstantQuery("metrics-generic.prometheus-*");
        assertMetricResult(responsePath);
    }

    /**
     * Verifies that omitting the {@code time} parameter defaults to current server time without error.
     * Since test data is in the past, the result will be empty — but the request must succeed.
     */
    public void testInstantQueryWithoutTimeDefaultsToNow() throws Exception {
        ingestTestData("test_gauge_iq");

        Request request = new Request("GET", "/_prometheus/api/v1/query");
        request.addParameter("query", "test_gauge_iq{job=\"test_job\"}");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        // Test data is in the past, so current-time lookback returns no results — that's expected.
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    private static void assertMetricResult(ObjectPath responsePath) throws IOException {
        assertThat(responsePath.evaluate("data.result"), hasSize(1));
        assertThat(responsePath.evaluate("data.result.0.metric.job"), equalTo("test_job"));
        assertThat(responsePath.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));

        // Instant query returns a single "value" pair, not a "values" array
        List<Object> value = responsePath.evaluate("data.result.0.value");
        assertThat(value, hasSize(2));
        assertThat(value.get(0), instanceOf(Number.class));
        assertThat(value.get(1), instanceOf(String.class));
    }

    private ObjectPath executeInstantQuery(String index) throws Exception {
        String path = index == null ? "/_prometheus/api/v1/query" : "/_prometheus/" + index + "/api/v1/query";
        Request request = new Request("GET", path);
        request.addParameter("query", "test_gauge_iq{job=\"test_job\"}");
        request.addParameter("time", "2026-01-01T00:05:00Z");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }

}
