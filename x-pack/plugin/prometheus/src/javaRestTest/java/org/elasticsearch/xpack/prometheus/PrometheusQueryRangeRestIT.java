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
        Request request = new Request("GET", "/_prometheus/api/v1/query_range");
        request.addParameter("query", "nonexistent_metric");
        request.addParameter("start", "2026-01-01T00:00:00Z");
        request.addParameter("end", "2026-01-01T00:05:00Z");
        request.addParameter("step", "60s");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        assertThat(responsePath.evaluate("data.result"), empty());
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
        Request request = new Request("GET", path);
        request.addParameter("query", "test_gauge_qr{job=\"test_job\"}");
        request.addParameter("start", "2026-01-01T00:00:00Z");
        request.addParameter("end", "2026-01-01T00:05:00Z");
        request.addParameter("step", "60s");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        return responsePath;
    }

}
