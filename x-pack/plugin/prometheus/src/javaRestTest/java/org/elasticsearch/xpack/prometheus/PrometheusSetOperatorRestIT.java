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
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end tests for the top-level PromQL {@code or} (UNION) set operator via the native Prometheus
 * query endpoints.
 *
 * <p>The native endpoints wrap the parsed PromQL plan in a {@code TimeSeriesCollapse} whose output is
 * derived from the PromQL plan's {@code output()} (see {@code PromqlQueryPlanBuilder}). For a union the
 * two branches can carry
 * different label sets, so the merged column layout contains label columns that only exist on one side.
 * These tests confirm the response writer reconstructs each series with only its own labels (null-filled
 * columns from the other branch are omitted), which is the column-layout concern unique to set operators.
 */
public class PrometheusSetOperatorRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC_A = "set_op_a";
    private static final String METRIC_B = "set_op_b";

    /**
     * Instant query of {@code a or b} where {@code a} and {@code b} are distinct metrics with disjoint
     * label keys ({@code region} only on {@code a}, {@code tier} only on {@code b}). Both series must be
     * returned, each carrying only its own labels.
     */
    public void testInstantUnionCombinesSeriesWithDifferentLabelSets() throws Exception {
        ingestUnionTestData();

        ObjectPath responsePath = executeInstantQuery(METRIC_A + " or " + METRIC_B, "2026-01-01T00:04:00Z");
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));

        List<Map<String, Object>> results = responsePath.evaluate("data.result");
        assertThat(results, hasSize(2));

        Map<String, Object> seriesA = findSeriesByName(results, METRIC_A);
        ObjectPath a = new ObjectPath(seriesA);
        assertThat(a.evaluate("metric.job"), equalTo("test_job"));
        assertThat(a.evaluate("metric.instance"), equalTo("localhost:9090"));
        assertThat(a.evaluate("metric.region"), equalTo("west"));
        // The `tier` label only exists on the right branch and must not leak onto a left-branch series.
        assertThat(((Map<?, ?>) a.evaluate("metric")).keySet(), not(contains("tier")));
        assertThat(a.evaluate("value"), equalTo(List.of(1767225840.0 /*=2026-01-01T00:04:00Z*/, "40.0")));

        Map<String, Object> seriesB = findSeriesByName(results, METRIC_B);
        ObjectPath b = new ObjectPath(seriesB);
        assertThat(b.evaluate("metric.job"), equalTo("test_job"));
        assertThat(b.evaluate("metric.instance"), equalTo("localhost:9091"));
        assertThat(b.evaluate("metric.tier"), equalTo("backend"));
        assertThat(((Map<?, ?>) b.evaluate("metric")).keySet(), not(contains("region")));
        assertThat(b.evaluate("value"), equalTo(List.of(1767225840.0, "140.0")));
    }

    /**
     * Range query of {@code a or b} over the full ingest window: both series appear with a full
     * {@code values} array, confirming the merged column layout survives matrix formatting too.
     */
    public void testRangeUnionCombinesSeriesWithDifferentLabelSets() throws Exception {
        ingestUnionTestData();

        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", METRIC_A + " or " + METRIC_B),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:04:00Z"),
            new BasicNameValuePair("step", "60s")
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));

        List<Map<String, Object>> results = responsePath.evaluate("data.result");
        assertThat(results, hasSize(2));

        ObjectPath a = new ObjectPath(findSeriesByName(results, METRIC_A));
        assertThat(((Map<?, ?>) a.evaluate("metric")).keySet(), containsInAnyOrder("__name__", "job", "instance", "region"));
        assertThat(a.evaluate("values"), hasSize(5));

        ObjectPath b = new ObjectPath(findSeriesByName(results, METRIC_B));
        assertThat(((Map<?, ?>) b.evaluate("metric")).keySet(), containsInAnyOrder("__name__", "job", "instance", "tier"));
        assertThat(b.evaluate("values"), hasSize(5));
    }

    private static Map<String, Object> findSeriesByName(List<Map<String, Object>> results, String name) {
        return results.stream().filter(r -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> metric = (Map<String, Object>) r.get("metric");
            return name.equals(metric.get("__name__"));
        }).findFirst().orElseThrow(() -> new AssertionError("no series found with __name__=" + name + " in " + results));
    }

    private ObjectPath executeInstantQuery(String query, String time) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("time", time)
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return ObjectPath.createFromResponse(response);
    }

    /**
     * Writes 5 evenly-spaced samples (1-minute step starting at 2026-01-01T00:00:00Z) for two metrics
     * with disjoint label keys: {@value #METRIC_A} carries {@code region=west} and {@value #METRIC_B}
     * carries {@code tier=backend}.
     */
    private void ingestUnionTestData() throws IOException {
        long baseTimestamp = 1767225600000L; // 2026-01-01T00:00:00Z

        RemoteWrite.WriteRequest.Builder writeRequestBuilder = RemoteWrite.WriteRequest.newBuilder();
        for (int i = 0; i < 5; i++) {
            writeRequestBuilder.addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(label("__name__", METRIC_A))
                    .addLabels(label("job", "test_job"))
                    .addLabels(label("instance", "localhost:9090"))
                    .addLabels(label("region", "west"))
                    .addSamples(sample(i * 10.0, baseTimestamp + i * 60_000L))
                    .build()
            );
            writeRequestBuilder.addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(label("__name__", METRIC_B))
                    .addLabels(label("job", "test_job"))
                    .addLabels(label("instance", "localhost:9091"))
                    .addLabels(label("tier", "backend"))
                    .addSamples(sample(100.0 + i * 10.0, baseTimestamp + i * 60_000L))
                    .build()
            );
        }

        ingestTestData(writeRequestBuilder.build());
    }
}
