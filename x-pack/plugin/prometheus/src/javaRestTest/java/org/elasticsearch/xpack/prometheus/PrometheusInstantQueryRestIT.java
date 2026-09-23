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
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.prometheus.PromqlResponseSeries.of;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

    public void testInstantQuerySumByEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(
            instantSeries("sum by (cluster) (" + METRIC + ")"),
            containsInAnyOrder(of("cluster", "a", 3.0), of("cluster", "b", 7.0))
        );
        assertThat(instantSeries("sum by (pod) (" + METRIC + ")"), containsInAnyOrder(of("pod", "p1", 4.0), of("pod", "p2", 6.0)));
        assertThat(instantSeries("sum by (region) (" + METRIC + ")"), containsInAnyOrder(of("region", "r1", 5.0), of("region", "r2", 5.0)));
        assertThat(instantSeries("sum by (job) (" + METRIC + ")"), contains(of("job", "test_job", 10.0)));
    }

    public void testInstantQuerySumWithoutEachLabel() throws Exception {
        ingestLabelledSeries(METRIC);

        for (String dropped : LABELLED_SERIES_LABELS) {
            assertThat(
                "without(" + dropped + ")",
                instantSeries("sum without (" + dropped + ") (" + METRIC + ")"),
                containsInAnyOrder(LABELLED_SERIES.stream().map(series -> series.without(dropped)).toArray(PromqlResponseSeries[]::new))
            );
        }
    }

    public void testInstantQueryTopKKeepsSeriesLabels() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(instantSeries("topk(2, " + METRIC + ")"), containsInAnyOrder(seriesWithValueAbove(2.0)));
    }

    public void testInstantQueryComparisonFiltersSeries() throws Exception {
        ingestLabelledSeries(METRIC);

        assertThat(instantSeries(METRIC + " > 1"), containsInAnyOrder(seriesWithValueAbove(1.0)));
    }

    private static PromqlResponseSeries[] seriesWithValueAbove(double threshold) {
        return LABELLED_SERIES.stream().filter(series -> series.value() > threshold).toArray(PromqlResponseSeries[]::new);
    }

    private List<PromqlResponseSeries> instantSeries(String promql) throws Exception {
        return PromqlResponseSeries.ofInstant(executeInstantQuery(promql, "2026-01-01T00:05:00Z", null));
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

    // --- tx/rx queries across ingestion paths through the instant query API ---
    // Ingestion helpers live in the base class.

    private static final Instant QUERY_TIME = Instant.parse("2024-05-10T00:00:00Z");

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantRawOperandsMatchAcrossIngestionPaths() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("tx / rx", 5, 10, 3);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("tx / rx", 5, 10, 3);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("tx / rx", 5, 10, 3);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantRawAndPairedOperandsMatchAcrossIngestionPaths() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("tx / (tx + rx)", 5.0 / 6, 10.0 / 11, 3.0 / 4);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("tx / (tx + rx)", 5.0 / 6, 10.0 / 11, 3.0 / 4);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("tx / (tx + rx)", 5.0 / 6, 10.0 / 11, 3.0 / 4);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantSumOverCrossMetricPairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("sum(tx / rx)", 18);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / rx)", 18);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / rx)", 18);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantSumOverSameMetricPairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("sum(tx / tx)", 3);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / tx)", 3);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / tx)", 3);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantSumOverChainedPairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("sum(tx / (tx + rx))", 5.0 / 6 + 10.0 / 11 + 3.0 / 4);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / (tx + rx))", 5.0 / 6 + 10.0 / 11 + 3.0 / 4);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("sum(tx / (tx + rx))", 5.0 / 6 + 10.0 / 11 + 3.0 / 4);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantGroupedSumOverPairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (tx / rx)", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (tx / rx)", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (tx / rx)", "cluster", Map.of("prod", 15.0, "qa", 3.0));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantGroupedSumOverIncreasePairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (increase(tx[1m]) / increase(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (increase(tx[1m]) / increase(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (increase(tx[1m]) / increase(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantGroupedSumOverIratePairing() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (irate(tx[1m]) / irate(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (irate(tx[1m]) / irate(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (cluster) (irate(tx[1m]) / irate(rx[1m]))", "cluster", Map.of("prod", 15.0, "qa", 3.0));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantDefaultMatchingExcludesMetricName() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / sum by (host, __name__) (rx)", "host", txRxRatios());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantIgnoringMatchingExcludesMetricName() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / ignoring () sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / ignoring () sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / ignoring () sum by (host, __name__) (rx)", "host", txRxRatios());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMatchingPreservesBothOperandSelections() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) + bottomk(1, tx)");
        assertBinopInstantValues("bottomk(1, tx) + topk(1, tx)");
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) + bottomk(1, tx)");
        assertBinopInstantValues("bottomk(1, tx) + topk(1, tx)");
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) + bottomk(1, tx)");
        assertBinopInstantValues("bottomk(1, tx) + topk(1, tx)");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMatchingPreservesRightOperandSelection() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("tx / topk(1, rx)", 3);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("tx / topk(1, rx)", 3);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("tx / topk(1, rx)", 3);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMatchingPreservesLeftOperandSelection() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) / rx", 10);
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) / rx", 10);
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantValues("topk(1, tx) / rx", 10);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMixedDuplicateRawMatchKeysAreRejected() throws Exception {
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantDuplicate("tx_dup / rx_dup");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMixedDuplicateLeftExpressionMatchKeysAreRejected() throws Exception {
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantDuplicate("(tx_dup + 0) / rx_dup");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/158610")
    public void testInstantMixedDuplicateRightExpressionMatchKeysAreRejected() throws Exception {
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantDuplicate("tx_dup / (rx_dup + 0)");
    }

    public void testInstantAggregatedOperandsMatchAcrossIngestionPaths() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantAggGroups();
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantAggGroups();
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantAggGroups();
    }

    public void testInstantExplicitOnMatchesRetainedMetricNames() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / on (host) sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / on (host) sum by (host, __name__) (rx)", "host", txRxRatios());
        wipeDefaultStream();
        ingestTestDataUsingRemoteWriteAndBulk(QUERY_TIME);
        assertBinopInstantGroups("sum by (host, __name__) (tx) / on (host) sum by (host, __name__) (rx)", "host", txRxRatios());
    }

    private ObjectPath executeBinopInstantQuery(String expression) throws IOException {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", expression),
            new BasicNameValuePair("time", QUERY_TIME.toString())
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath responsePath = ObjectPath.createFromResponse(response);
        assertThat(responsePath.evaluate("status"), equalTo("success"));
        assertThat(responsePath.evaluate("data.resultType"), equalTo("vector"));
        return responsePath;
    }

    private void assertBinopInstantValues(String expression, double... expected) throws IOException {
        ObjectPath response = executeBinopInstantQuery(expression);
        List<Double> actual = PromqlResponseSeries.ofInstant(response).stream().map(PromqlResponseSeries::value).sorted().toList();
        Arrays.sort(expected);
        assertEquals(expression + ": " + actual, expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertThat(expression, actual.get(i), closeTo(expected[i], 1e-10));
        }
    }

    private void assertBinopInstantGroups(String expression, String group, Map<String, Double> expected) throws IOException {
        ObjectPath response = executeBinopInstantQuery(expression);
        Map<String, Double> actual = new HashMap<>();
        for (PromqlResponseSeries series : PromqlResponseSeries.ofInstant(response)) {
            assertNull("duplicate output group", actual.put(series.labels().get(group), series.value()));
        }
        assertThat(expression, actual.keySet(), equalTo(expected.keySet()));
        expected.forEach((label, value) -> assertThat(expression + " " + label, actual.get(label), closeTo(value, 1e-10)));
    }

    private void assertBinopInstantAggGroups() throws IOException {
        // Default matching exercises folding; explicit matching exercises the join.
        for (String match : List.of("", "on (host)", "ignoring ()")) {
            assertBinopInstantGroups("sum by (host) (tx) / " + match + " sum by (host) (rx)", "host", txRxRatios());
        }
    }

    private void assertBinopInstantDuplicate(String expression) {
        ResponseException error = expectThrows(ResponseException.class, () -> executeBinopInstantQuery(expression));
        assertThat(error.getMessage(), containsString("duplicate"));
    }

}
