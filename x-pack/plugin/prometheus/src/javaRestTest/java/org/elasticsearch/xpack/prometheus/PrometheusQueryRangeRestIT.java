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
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.prometheus.PromqlSeries.of;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

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
     * on, and the result must carry that label and nothing else.
     */
    public void testQueryRangeSumByEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(rangeSeries("sum by (cluster) (" + METRIC + ")"), containsInAnyOrder(of("cluster", "a", 3.0), of("cluster", "b", 7.0)));
        assertThat(rangeSeries("sum by (pod) (" + METRIC + ")"), containsInAnyOrder(of("pod", "p1", 4.0), of("pod", "p2", 6.0)));
        assertThat(rangeSeries("sum by (region) (" + METRIC + ")"), containsInAnyOrder(of("region", "r1", 5.0), of("region", "r2", 5.0)));
        assertThat(rangeSeries("sum by (job) (" + METRIC + ")"), contains(of("job", "test_job", 10.0)));
        assertThat(rangeSeries("sum by (instance) (" + METRIC + ")"), contains(of("instance", "localhost:9090", 10.0)));
    }

    /** {@code without (...)} must drop the named label and leave every other label, and value, untouched. */
    public void testQueryRangeSumWithoutEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        for (String dropped : LABELLED_SERIES_LABELS) {
            assertThat(
                "without(" + dropped + ")",
                rangeSeries("sum without (" + dropped + ") (" + METRIC + ")"),
                containsInAnyOrder(expected(series -> series.without(dropped)))
            );
        }
    }

    /**
     * An opaque {@code without} child feeding a {@code by} parent: the inner aggregation packs its identity into
     * {@code _timeseries}, and the outer one must still resolve {@code pod} out of it.
     */
    public void testQueryRangeNestedRegrouping() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            rangeSeries("sum by (pod) (sum without (region) (" + METRIC + "))"),
            containsInAnyOrder(of("pod", "p1", 4.0), of("pod", "p2", 6.0))
        );
    }

    /**
     * {@code or} aligns its branches by column name, so branches carrying different label sets are the interesting
     * case: neither side's labelset appears on the other, so all four series survive.
     */
    public void testQueryRangeUnionOfDifferentlyShapedBranches() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            rangeSeries("sum by (cluster) (" + METRIC + ") or sum by (pod) (" + METRIC + ")"),
            containsInAnyOrder(of("cluster", "a", 3.0), of("cluster", "b", 7.0), of("pod", "p1", 4.0), of("pod", "p2", 6.0))
        );
    }

    /** Elementwise arithmetic applies to every sample and keeps the operand's labels. */
    public void testQueryRangeArithmeticKeepsSeriesLabels() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(rangeSeries(METRIC + " * 2"), containsInAnyOrder(expected(series -> series.withValue(series.value() * 2))));
    }

    /** The ingested fixture mapped through {@code transform}, as an array of matchers-by-equality. */
    private static PromqlSeries[] expected(UnaryOperator<PromqlSeries> transform) {
        return LABELLED_SERIES.stream().map(transform).toArray(PromqlSeries[]::new);
    }

    /** Series returned by a range query spanning the ingested sample, each taken at its last step. */
    private List<PromqlSeries> rangeSeries(String promql) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", promql),
            new BasicNameValuePair("start", "2026-01-01T00:01:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:03:00Z"),
            new BasicNameValuePair("step", "60s")
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("matrix"));
        return PromqlSeries.ofRange(responsePath);
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
