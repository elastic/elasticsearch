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

import static org.hamcrest.Matchers.equalTo;

/**
 * Each Prometheus API endpoint runs one ES|QL query, which the ES|QL usage counts under the endpoint's own entry point.
 */
public class PrometheusTelemetryRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "telemetry_test_metric";

    private static final List<String> ENTRY_POINTS = List.of(
        "prometheus_query",
        "prometheus_query_range",
        "prometheus_labels",
        "prometheus_label_values",
        "prometheus_series",
        "prometheus_metadata"
    );

    public void testInstantQueryIsCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(
            prometheusReadRequest(
                "/_prometheus/api/v1/query",
                new BasicNameValuePair("query", METRIC),
                new BasicNameValuePair("time", "2026-01-01T00:05:00Z")
            ),
            "prometheus_query"
        );
    }

    public void testRangeQueryIsCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(
            prometheusReadRequest(
                "/_prometheus/api/v1/query_range",
                new BasicNameValuePair("query", METRIC),
                new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
                new BasicNameValuePair("end", "2026-01-01T00:05:00Z"),
                new BasicNameValuePair("step", "60s")
            ),
            "prometheus_query_range"
        );
    }

    public void testLabelsAreCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(prometheusReadRequest("/_prometheus/api/v1/labels"), "prometheus_labels");
    }

    public void testLabelValuesAreCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(prometheusGetRequest("/_prometheus/api/v1/label/job/values", readApiKey), "prometheus_label_values");
    }

    public void testSeriesAreCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(prometheusReadRequest("/_prometheus/api/v1/series", new BasicNameValuePair("match[]", METRIC)), "prometheus_series");
    }

    public void testMetadataIsCounted() throws IOException {
        ingestTestData(METRIC);
        assertCounted(prometheusGetRequest("/_prometheus/api/v1/metadata", readApiKey), "prometheus_metadata");
    }

    private void assertCounted(Request request, String expectedEntryPoint) throws IOException {
        ObjectPath before = esqlUsage();
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath after = esqlUsage();

        for (String entryPoint : ENTRY_POINTS) {
            int expectedDelta = entryPoint.equals(expectedEntryPoint) ? 1 : 0;
            assertThat(entryPoint, delta(before, after, "esql.entry_points." + entryPoint + ".total"), equalTo(expectedDelta));
        }
        assertThat(delta(before, after, "esql.entry_points.query.total"), equalTo(0));
        assertThat(delta(before, after, "esql.entry_points.internal.total"), equalTo(0));
        assertThat(delta(before, after, "esql.queries.rest.total"), equalTo(1));
    }

    private static ObjectPath esqlUsage() throws IOException {
        return ObjectPath.createFromResponse(adminClient().performRequest(new Request("GET", "/_xpack/usage")));
    }

    private static int delta(ObjectPath before, ObjectPath after, String counter) throws IOException {
        return ((Number) after.evaluate(counter)).intValue() - ((Number) before.evaluate(counter)).intValue();
    }
}
