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
import static org.hamcrest.Matchers.not;

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

    /**
     * {@code topk} keeps whole series (name included) and the enclosing {@code without} regroups them on every other
     * label: two packings of the same relation, {@code _timeseries} for the ranking and {@code _timeseries$host} for the
     * regroup. Prometheus: {@code topk(1, tx)} is host b (30), summed without host into {@code {cluster="prod"} 30}.
     */
    public void testInstantAggregateWithoutOverTopK() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("sum without (host) (topk(1, tx))", "cluster", Map.of("prod", 30.0));
        assertBinopInstantGroups("count without (cluster) (topk(2, tx))", "host", Map.of("b", 1.0, "c", 1.0));
        assertBinopInstantValues("max without (host, cluster) (bottomk(2, rx))", 3.0);
    }

    /**
     * {@code without} over every dimension the data stream maps leaves the empty label set: one series, not one series
     * per document labelled with the rest of the document.
     */
    public void testInstantAggregateWithoutEveryLabel() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertThat(
            instantSeriesAt("min without (__name__, host, cluster) (tx)", QUERY_TIME),
            contains(new PromqlResponseSeries(Map.of(), 10.0))
        );
        assertThat(
            instantSeriesAt("sum without (__name__, host, cluster) (rx)", QUERY_TIME),
            contains(new PromqlResponseSeries(Map.of(), 9.0))
        );
    }

    private List<PromqlResponseSeries> instantSeriesAt(String promql, Instant time) throws Exception {
        return PromqlResponseSeries.ofInstant(executeInstantQuery(promql, time.toString(), null));
    }

    /**
     * Prometheus drops {@code __name__} from the result of every function but the label functions and last_over_time,
     * also when the name arrived as a {@code by (__name__, ..)} grouping label; the aggregate itself keeps it.
     */
    public void testInstantFunctionOverNamedAggregateDropsTheMetricName() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertThat(metricLabelNames("abs(sum by (__name__, host) (tx))"), equalTo(List.of("host")));
        assertThat(metricLabelNames("sum by (__name__, host) (tx)"), equalTo(List.of("__name__", "host")));
        assertThat(metricLabelNames("sum by (__name__, host) (tx) > 5"), equalTo(List.of("__name__", "host")));
        assertThat(metricLabelNames("last_over_time(tx[5m])"), equalTo(List.of("__name__", "cluster", "host")));
        assertThat(metricLabelNames("rate(tx[5m])"), equalTo(List.of("cluster", "host")));
    }

    /** The sorted label names every result series of {@code promql} carries; the series must all agree. */
    private List<String> metricLabelNames(String promql) throws Exception {
        List<Map<String, Object>> result = executeBinopInstantQuery(promql).evaluate("data.result");
        assertThat(promql, result, not(empty()));
        List<String> names = null;
        for (Map<String, Object> series : result) {
            @SuppressWarnings("unchecked")
            List<String> labels = ((Map<String, Object>) series.get("metric")).keySet().stream().sorted().toList();
            assertTrue(promql + ": label sets differ across series " + result, names == null || names.equals(labels));
            names = labels;
        }
        return names;
    }

    /**
     * A scalar operand ({@code scalar(..)}, an aggregate of one series) applies to every element of the vector operand:
     * the operands join on the step alone, the vector side keeps its labels and loses the metric name.
     */
    public void testInstantScalarOperandBroadcasts() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("scalar(sum(tx)) * rx", 104, 156, 208);
        assertBinopInstantValues("rx / scalar(sum(tx))", 2.0 / 52, 3.0 / 52, 4.0 / 52);
        assertBinopInstantValues("scalar(tx{host=\"a\"}) * rx", 20, 30, 40);
        assertBinopInstantValues("rx * (scalar(tx{host=\"a\"}) + 1)", 22, 33, 44);
        assertBinopInstantGroups("sum by (cluster) (tx) * scalar(max(rx))", "cluster", Map.of("prod", 160.0, "qa", 48.0));
        assertBinopInstantGroups("scalar(min(tx)) - sum by (host) (rx)", "host", Map.of("a", 8.0, "b", 7.0, "c", 6.0));
        assertBinopInstantValues("count(rx * scalar(sum(tx)))", 3);
        assertThat(metricLabelNames("scalar(sum(tx)) * rx"), equalTo(List.of("cluster", "host")));
    }

    /**
     * Default matching between a closed operand (a {@code by} aggregate) and a raw selector pairs one-to-one on the full
     * label set: a pair exists only where the raw series carries exactly the aggregate's labels (and no other), so
     * {@code max by (host, cluster) (tx) * rx} pairs every host while {@code max by (host) (tx) * rx} and
     * {@code sum(tx) / rx} are empty.
     */
    public void testInstantClosedAggregateAgainstRawVectorMatchesOnTheFullLabelSet() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("max by (host, cluster) (tx) * rx", 20, 90, 48);
        assertBinopInstantValues("rx * max by (host, cluster) (tx)", 20, 90, 48);
        assertThat(metricLabelNames("max by (host, cluster) (tx) * rx"), equalTo(List.of("cluster", "host")));
        assertBinopInstantGroups("sum by (host, cluster) (tx) / rx{host!=\"c\"}", "host", Map.of("a", 5.0, "b", 10.0));
        assertBinopInstantValues("count(max by (host, cluster) (tx) * rx)", 3);
        assertBinopInstantValues("max by (host) (tx) * rx");
        assertBinopInstantValues("sum(tx) / rx");
        assertBinopInstantValues("rx / sum(tx)");
        assertBinopInstantValues("stdvar(tx) + rx");
        // a reduction is a finished table over packed series and pairs the same way, on the whole label set
        assertBinopInstantValues("topk(2, tx) * rx", 90, 48);
        assertBinopInstantValues("topk(1, tx) / topk(1, rx)");
        assertBinopInstantValues("bottomk(2, bottomk(1, tx)) * tx", 100);
    }

    /**
     * A series without a partner is dropped by the operator before any enclosing aggregate sees it: Prometheus has no
     * group for it, so {@code count by (cluster) (tx / rx{host!="c"})} has no {@code qa} group at all rather than
     * {@code qa 0}.
     */
    public void testInstantUnmatchedPairsNeverReachTheEnclosingAggregate() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantGroups("count by (cluster) (tx / rx{host!=\"c\"})", "cluster", Map.of("prod", 2.0));
        assertBinopInstantValues("count(tx / rx{host!=\"c\"})", 2);
        assertBinopInstantGroups("sum by (cluster) (tx - rx{host!=\"c\"})", "cluster", Map.of("prod", 35.0));
        assertBinopInstantGroups(
            "count by (host) (sum by (host, cluster) (tx) / sum by (host, cluster) (rx{host!=\"c\"}))",
            "host",
            Map.of("a", 1.0, "b", 1.0)
        );
        assertBinopInstantValues("count(tx / rx{host=~\"nope\"})");
    }

    /**
     * {@code vector(s)} is one series with no labels at every step: in an {@code or} it fills what the other side lacks
     * (whatever sample sits exactly one lookback before the query time), an aggregate over it is itself, it pairs only with
     * another label-less vector, and a comparison filters it like any other vector.
     */
    public void testInstantConstantVector() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME.minusSeconds(300));
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("tx or vector(0)", 10, 30, 12, 0);
        assertBinopInstantValues("vector(0) or tx", 0, 10, 30, 12);
        assertBinopInstantValues("sum(tx) or vector(0)", 52);
        assertBinopInstantValues("sum(tx{host=~\"nope\"}) or vector(0)", 0);
        assertBinopInstantValues("sum(vector(1))", 1);
        assertBinopInstantValues("count(vector(5))", 1);
        assertBinopInstantValues("vector(1) + vector(2)", 3);
        assertBinopInstantValues("sum(tx) + vector(1)", 53);
        assertBinopInstantValues("sum by (cluster) (tx) + vector(1)");
        assertBinopInstantValues("tx * vector(2)");
        assertBinopInstantValues("abs(vector(-1)) * 3", 3);
        assertBinopInstantValues("vector(1) > 2");
        assertBinopInstantValues("vector(3) > 2", 3);
    }

    /** Prometheus converts k with an integer cast: {@code topk(1.5, tx)} keeps one series and {@code topk(0.5, tx)} none. */
    public void testInstantFractionalKIsTruncated() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("topk(1.5, tx)", 30);
        assertBinopInstantValues("bottomk(1.5, tx)", 10);
        assertBinopInstantValues("topk(2.9, tx)", 30, 12);
        assertBinopInstantValues("topk(0.5, tx)");
    }

    /** Prometheus: {@code clamp} with {@code min > max} is the empty vector. */
    public void testInstantClampWithMinAboveMaxIsEmpty() throws Exception {
        ingestTestDataUsingRemoteWrite(QUERY_TIME);
        assertBinopInstantValues("clamp(tx, 60, 40)");
        assertBinopInstantValues("clamp(tx, 20, 25)", 20, 25, 20);
    }
}
