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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for the Prometheus {@code /api/v1/query_range} endpoint.
 */
public class PrometheusQueryRangeRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "test_gauge_labels_qr";

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

    /**
     * {@code by (...)} must key on the named label whichever side of the {@code labels.} passthrough prefix it sorts
     * on, and the result must expose that label and nothing else.
     */
    public void testQueryRangeSumByEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(sumOf("sum by (cluster) (" + METRIC + ")"), equalTo(Map.of(Map.of("cluster", "a"), "3", Map.of("cluster", "b"), "7")));
        assertThat(sumOf("sum by (pod) (" + METRIC + ")"), equalTo(Map.of(Map.of("pod", "p1"), "4", Map.of("pod", "p2"), "6")));
        assertThat(sumOf("sum by (region) (" + METRIC + ")"), equalTo(Map.of(Map.of("region", "r1"), "5", Map.of("region", "r2"), "5")));
        assertThat(sumOf("sum by (job) (" + METRIC + ")"), equalTo(Map.of(Map.of("job", "test_job"), "10")));
        assertThat(sumOf("sum by (instance) (" + METRIC + ")"), equalTo(Map.of(Map.of("instance", "localhost:9090"), "10")));
    }

    /**
     * {@code without (...)} must drop the named label and keep the rest, again regardless of how the label sorts
     * against the passthrough prefix. Every series stays distinct after dropping any single label, so the four input
     * values survive unchanged.
     */
    public void testQueryRangeSumWithoutEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        for (String dropped : List.of("cluster", "instance", "job", "pod", "region")) {
            Map<Map<String, String>, String> series = sumOf("sum without (" + dropped + ") (" + METRIC + ")");
            assertThat("without(" + dropped + ")", series.keySet(), hasSize(4));
            for (Map<String, String> labels : series.keySet()) {
                assertThat("without(" + dropped + ") leaked [" + dropped + "]: " + labels, labels.containsKey(dropped), equalTo(false));
                assertThat("without(" + dropped + ") lost labels: " + labels, labels.keySet(), hasSize(4));
            }
            assertThat(Set.copyOf(series.values()), equalTo(Set.of("1", "2", "3", "4")));
        }
    }

    /**
     * An opaque {@code without} child feeding a {@code by} parent: the inner aggregation packs its identity, and the
     * outer one must still resolve {@code pod} out of it.
     */
    public void testQueryRangeNestedRegrouping() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            sumOf("sum by (pod) (sum without (region) (" + METRIC + "))"),
            equalTo(Map.of(Map.of("pod", "p1"), "4", Map.of("pod", "p2"), "6"))
        );
    }

    /**
     * {@code or} aligns its branches by column name, so branches carrying different label sets are the interesting
     * case: neither side's labelset appears on the other, so all four series survive.
     */
    public void testQueryRangeUnionOfDifferentlyShapedBranches() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            sumOf("sum by (cluster) (" + METRIC + ") or sum by (pod) (" + METRIC + ")"),
            equalTo(Map.of(Map.of("cluster", "a"), "3", Map.of("cluster", "b"), "7", Map.of("pod", "p1"), "4", Map.of("pod", "p2"), "6"))
        );
    }

    /** Elementwise arithmetic keeps every label of the operand series. */
    public void testQueryRangeArithmeticKeepsSeriesLabels() throws Exception {
        ingestLabelledSeries(METRIC);

        Map<Map<String, String>, String> doubled = sumOf(METRIC + " * 2");
        assertThat(doubled.keySet(), hasSize(4));
        assertThat(Set.copyOf(doubled.values()), equalTo(Set.of("2", "4", "6", "8")));
        for (Map<String, String> labels : doubled.keySet()) {
            assertThat(labels.keySet(), equalTo(Set.of("cluster", "instance", "job", "pod", "region")));
        }
    }

    /**
     * Runs a range query over the ingested window and maps each returned series to its labels (without
     * {@code __name__}) and its last sample value.
     */
    private Map<Map<String, String>, String> sumOf(String promql) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", promql),
            new BasicNameValuePair("start", "2026-01-01T00:01:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:03:00Z"),
            new BasicNameValuePair("step", "60s")
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath path = ObjectPath.createFromResponse(response);
        assertThat(path.evaluate("status"), equalTo("success"));

        List<Map<String, Object>> result = path.evaluate("data.result");
        Map<Map<String, String>, String> seriesByLabels = new HashMap<>();
        for (Map<String, Object> series : result) {
            // The Prometheus response shape: "metric" is a label map, "values" a list of [epochSeconds, value] pairs.
            @SuppressWarnings("unchecked")
            Map<String, String> metric = new HashMap<>((Map<String, String>) series.get("metric"));
            metric.remove("__name__");
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) series.get("values");
            assertThat("no samples for " + metric + " in [" + promql + "]", values, not(empty()));
            seriesByLabels.put(Map.copyOf(metric), stripTrailingZero((String) values.getLast().get(1)));
        }
        return seriesByLabels;
    }

    /** Prometheus renders whole numbers as {@code 3} or {@code 3.0} depending on the path; compare on the integer. */
    private static String stripTrailingZero(String value) {
        return value.endsWith(".0") ? value.substring(0, value.length() - 2) : value;
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
