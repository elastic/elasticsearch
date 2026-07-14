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
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the Prometheus {@code /api/v1/query_range} endpoint.
 */
public class PrometheusQueryRangeRestIT extends AbstractPrometheusRestIT {

    /**
     * Verifies that querying when no Prometheus indices exist returns an empty result instead of an error.
     * ESRestTestCase wipes all indices between test methods, so this test always runs on a clean cluster.
     */
    public void testQueryRangeWithNoPrometheusIndicesReturnsEmptyResult() throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", "nonexistent_metric"),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:05:00Z"),
            new BasicNameValuePair("step", "60s")
        );

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        assertThat(responsePath.evaluate("data.result"), empty());
    }

    /**
     * A pure scalar constant requires no index data: each step in the range produces the literal value.
     */
    public void testQueryRangeScalarConstantRequiresNoIndexData() throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", "3.14"),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:02:00Z"),
            new BasicNameValuePair("step", "60s")
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath path = ObjectPath.createFromResponse(response);
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data.resultType"), equalTo("matrix"));
        // one series entry with no labels and one sample per step
        assertThat(path.evaluate("data.result"), hasSize(1));
        assertThat(path.evaluate("data.result.0.metric"), equalTo(Map.of()));
        assertThat(
            path.evaluate("data.result.0.values"),
            equalTo(List.of(List.of(1767225600.0, "3.14"), List.of(1767225660.0, "3.14"), List.of(1767225720.0, "3.14")))
        );
    }

    public void testQueryRangeWithIngestedData() throws Exception {
        ingestTestData("test_gauge_qr");

        ObjectPath responsePath = executeQueryRange();
        assertMetricResults(responsePath);
    }

    public void testQueryRangeWithIndexPattern() throws Exception {
        ingestTestData("test_gauge_qr");

        ObjectPath responsePath = executeQueryRangeWithIndex("metrics-generic.prometheus-*");
        assertMetricResults(responsePath);
    }

    private static void assertMetricResults(ObjectPath responsePath) throws IOException {
        assertThat(responsePath.evaluate("data.result"), hasSize(1));
        assertThat(responsePath.evaluate("data.result.0.metric.job"), equalTo("test_job"));
        assertThat(responsePath.evaluate("data.result.0.metric.instance"), equalTo("localhost:9090"));
        List<List<Object>> values = responsePath.evaluate("data.result.0.values");
        assertThat(values, hasSize(5));

        // Assert timestamps are in strictly ascending order
        double prevTimestamp = -1;
        for (List<Object> point : values) {
            double timestamp = ((Number) point.getFirst()).doubleValue();
            assertThat(timestamp, greaterThan(prevTimestamp));
            prevTimestamp = timestamp;
        }
    }

    private ObjectPath executeQueryRange() throws Exception {
        return executeQueryRangeWithIndex(null);
    }

    private ObjectPath executeQueryRangeWithIndex(String index) throws Exception {
        String path = index == null ? "/_prometheus/api/v1/query_range" : "/_prometheus/" + index + "/api/v1/query_range";
        Request request = prometheusReadRequest(
            path,
            new BasicNameValuePair("query", "test_gauge_qr{job=\"test_job\"}"),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:05:00Z"),
            new BasicNameValuePair("step", "60s")
        );

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        return responsePath;
    }

}
