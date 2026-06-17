/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
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
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", "nonexistent_metric"),
            new BasicNameValuePair("time", "2026-01-01T00:05:00Z")
        );

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

        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", "test_gauge_iq{job=\"test_job\"}")
        );

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        // Test data is in the past, so current-time lookback returns no results — that's expected.
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    public void testInstantQueryReturnsLatestSampleWithinDefaultLookback() throws Exception {
        ingestTestData("test_gauge_iq");
        // Evaluation time T = 00:08:00; default lookback = 5m, so window is (00:03:00, 00:08:00].
        ObjectPath responsePath = executeInstantQuery("test_gauge_iq{job=\"test_job\"}", "2026-01-01T00:08:00Z", null);
        assertThat(responsePath.evaluate("data.result"), hasSize(1));
        assertThat(responsePath.evaluate("data.result.0.value"), equalTo(List.of(1767226080.0 /*=2026-01-01T00:08:00Z*/, "40.0")));
    }

    /**
     * A pure scalar constant requires no index data: the result is produced entirely from the literal value.
     */
    public void testInstantQueryScalarConstantRequiresNoIndexData() throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", "3.14"),
            new BasicNameValuePair("time", "2026-01-01T00:05:00Z")
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath path = ObjectPath.createFromResponse(response);
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data.resultType"), equalTo("scalar"));
        // scalar result is [timestamp_seconds, value_string]
        assertThat(path.evaluate("data.result"), equalTo(List.of(1767225900.0, "3.14")));
    }

    public void testInstantQueryDropsSeriesOutsideDefaultLookback() throws Exception {
        ingestTestData("test_gauge_iq");

        ObjectPath responsePath = executeInstantQuery("test_gauge_iq{job=\"test_job\"}", "2026-01-01T00:10:00Z", null);
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
        return executeInstantQuery("test_gauge_iq{job=\"test_job\"}", "2026-01-01T00:05:00Z", index);
    }

    private ObjectPath executeInstantQuery(String query, String time, String index) throws Exception {
        String path = index == null ? "/_prometheus/api/v1/query" : "/_prometheus/" + index + "/api/v1/query";
        Request request = prometheusReadRequest(path, new BasicNameValuePair("query", query), new BasicNameValuePair("time", time));

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }

}
