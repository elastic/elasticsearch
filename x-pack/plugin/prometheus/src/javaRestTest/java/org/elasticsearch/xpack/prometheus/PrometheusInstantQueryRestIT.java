/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests for the Prometheus {@code /api/v1/query} instant query endpoint.
 */
public class PrometheusInstantQueryRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "test_gauge_labels_iq";

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

    public void testInstantQueryWithAliasOutsideApiKeyPatternReturnsUnknownIndex() throws Exception {
        ingestTestData("test_gauge_iq");
        createAlias("prometheus-metrics-alias", DEFAULT_DATA_STREAM);

        // Index privileges are resolved against the alias in the request URL, not only the backing data stream.
        ResponseException e = expectThrows(ResponseException.class, () -> executeInstantQuery("prometheus-metrics-alias"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("Unknown index [prometheus-metrics-alias]"));
    }

    public void testInstantQueryWithAliasGrantedByApiKey() throws Exception {
        ingestTestData("test_gauge_iq");
        String alias = "prometheus-metrics-api-key-alias";
        createAlias(alias, DEFAULT_DATA_STREAM);

        String aliasReadApiKey = createPrometheusReadApiKey("prometheus-alias-read-key", alias);
        ObjectPath responsePath = executeInstantQuery("test_gauge_iq{job=\"test_job\"}", "2026-01-01T00:05:00Z", alias, aliasReadApiKey);
        assertMetricResult(responsePath);
    }

    public void testInstantQueryWithAliasMatchingApiKeyPattern() throws Exception {
        ingestTestData("test_gauge_iq");
        createAlias("metrics-prometheus-alias", DEFAULT_DATA_STREAM);

        ObjectPath responsePath = executeInstantQuery("metrics-prometheus-alias");
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

    /**
     * {@code by (...)} must key on the named label whichever side of the {@code labels.} passthrough prefix it sorts
     * on, and the result must expose that label and nothing else.
     */
    public void testInstantQuerySumByEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            valuesOf("sum by (cluster) (" + METRIC + ")"),
            equalTo(Map.of(Map.of("cluster", "a"), "3", Map.of("cluster", "b"), "7"))
        );
        assertThat(valuesOf("sum by (pod) (" + METRIC + ")"), equalTo(Map.of(Map.of("pod", "p1"), "4", Map.of("pod", "p2"), "6")));
        assertThat(valuesOf("sum by (region) (" + METRIC + ")"), equalTo(Map.of(Map.of("region", "r1"), "5", Map.of("region", "r2"), "5")));
        assertThat(valuesOf("sum by (job) (" + METRIC + ")"), equalTo(Map.of(Map.of("job", "test_job"), "10")));
    }

    /**
     * {@code without (...)} must drop the named label and keep the rest. Every series stays distinct after dropping
     * any single label, so the four input values survive unchanged.
     */
    public void testInstantQuerySumWithoutEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        for (String dropped : List.of("cluster", "instance", "job", "pod", "region")) {
            Map<Map<String, String>, String> series = valuesOf("sum without (" + dropped + ") (" + METRIC + ")");
            assertThat("without(" + dropped + ")", series.keySet(), hasSize(4));
            for (Map<String, String> labels : series.keySet()) {
                assertThat("without(" + dropped + ") leaked [" + dropped + "]: " + labels, labels.containsKey(dropped), equalTo(false));
                assertThat("without(" + dropped + ") lost labels: " + labels, labels.keySet(), hasSize(4));
            }
            assertThat(Set.copyOf(series.values()), equalTo(Set.of("1", "2", "3", "4")));
        }
    }

    /** {@code topk} ranks across series and keeps the full labelset of the ones it selects. */
    public void testInstantQueryTopKKeepsSeriesLabels() throws Exception {
        ingestLabelledSeries(METRIC);

        Map<Map<String, String>, String> top = valuesOf("topk(2, " + METRIC + ")");
        assertThat(top.keySet(), hasSize(2));
        assertThat(Set.copyOf(top.values()), equalTo(Set.of("3", "4")));
        for (Map<String, String> labels : top.keySet()) {
            assertThat(labels.keySet(), equalTo(Set.of("cluster", "instance", "job", "pod", "region")));
        }
    }

    /** A comparison against a scalar filters series out and leaves the survivors' labels and values untouched. */
    public void testInstantQueryComparisonFiltersSeries() throws Exception {
        ingestLabelledSeries(METRIC);

        Map<Map<String, String>, String> above = valuesOf(METRIC + " > 1");
        assertThat(above.keySet(), hasSize(3));
        assertThat(Set.copyOf(above.values()), equalTo(Set.of("2", "3", "4")));
    }

    /**
     * Evaluates an instant query inside the ingested lookback window and maps each returned series to its labels
     * (without {@code __name__}) and its value.
     */
    private Map<Map<String, String>, String> valuesOf(String promql) throws Exception {
        ObjectPath path = executeInstantQuery(promql, "2026-01-01T00:05:00Z", null);

        List<Map<String, Object>> result = path.evaluate("data.result");
        Map<Map<String, String>, String> seriesByLabels = new HashMap<>();
        for (Map<String, Object> series : result) {
            // The Prometheus response shape: "metric" is a label map, "value" is a single [epochSeconds, value] pair.
            @SuppressWarnings("unchecked")
            Map<String, String> metric = new HashMap<>((Map<String, String>) series.get("metric"));
            metric.remove("__name__");
            @SuppressWarnings("unchecked")
            List<Object> value = (List<Object>) series.get("value");
            seriesByLabels.put(Map.copyOf(metric), stripTrailingZero((String) value.get(1)));
        }
        return seriesByLabels;
    }

    /** Prometheus renders whole numbers as {@code 3} or {@code 3.0} depending on the path; compare on the integer. */
    private static String stripTrailingZero(String value) {
        return value.endsWith(".0") ? value.substring(0, value.length() - 2) : value;
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

    private ObjectPath executeInstantQuery(String query, String time, String index, String apiKey) throws Exception {
        String path = index == null ? "/_prometheus/api/v1/query" : "/_prometheus/" + index + "/api/v1/query";
        Request request = prometheusGetRequest(path, apiKey, new BasicNameValuePair("query", query), new BasicNameValuePair("time", time));

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }

    private void createAlias(String alias, String dataStream) throws Exception {
        Request request = new Request("POST", "/_aliases");
        request.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "$DATA_STREAM",
                    "alias": "$ALIAS"
                  }
                }
              ]
            }
            """.replace("$DATA_STREAM", dataStream).replace("$ALIAS", alias));
        client().performRequest(request);
    }

}
