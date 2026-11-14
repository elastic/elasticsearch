/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for the Prometheus {@code /api/v1/query_exemplars} endpoint over exemplars ingested through remote write.
 */
public class PrometheusQueryExemplarsRestIT extends AbstractPrometheusRestIT {

    private static final String METRIC = "request_latency_seconds";
    private static final String OTHER_METRIC = "queue_latency_seconds";

    private String exemplarsReadApiKey;
    private long now;

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("requires the query_exemplars feature flag", PrometheusPlugin.QUERY_EXEMPLARS_FEATURE_FLAG.isEnabled());
    }

    @Before
    public void createExemplarsReadApiKey() throws IOException {
        exemplarsReadApiKey = createApiKey("prometheus-exemplars-read-key", List.of("metrics-*", "exemplars-*"), "read");
        now = System.currentTimeMillis();
    }

    public void testExemplarsOfSelectedSeries() throws Exception {
        ingest(
            series(
                METRIC,
                "a",
                1.0,
                exemplar("trace-a1", "span-a1", 0.1, now - 120_000),
                exemplar("trace-a2", "span-a2", 0.2, now - 60_000)
            ),
            series(METRIC, "b", 2.0, exemplar("trace-b1", "span-b1", 0.3, now - 90_000)),
            series(OTHER_METRIC, "a", 3.0, exemplar("trace-q1", "span-q1", 0.4, now - 30_000))
        );

        ObjectPath path = queryExemplars(METRIC + "{instance=\"a\"}", now - 600_000, now);
        assertThat(path.evaluate("status"), equalTo("success"));
        assertThat(path.evaluate("data"), hasSize(1));
        assertThat(path.evaluate("data.0.seriesLabels"), equalTo(Map.of("__name__", METRIC, "job", "api", "instance", "a")));
        assertThat(path.evaluate("data.0.exemplars"), hasSize(2));
        assertThat(
            path.evaluate("data.0.exemplars.0.labels"),
            equalTo(Map.of("trace_id", "trace-a1", "span_id", "span-a1", "user", "user-trace-a1"))
        );
        assertThat(path.evaluate("data.0.exemplars.0.value"), equalTo("0.1"));
        assertThat(path.evaluate("data.0.exemplars.0.timestamp"), equalTo((now - 120_000) / 1000.0));
        assertThat(path.evaluate("data.0.exemplars.1.labels.trace_id"), equalTo("trace-a2"));
        assertThat(path.evaluate("data.0.exemplars.1.value"), equalTo("0.2"));
    }

    /** Only the selectors of the query select exemplars, whatever the expression around them. */
    public void testExemplarsOfAllSelectorsOfAnExpression() throws Exception {
        ingest(
            series(METRIC, "a", 1.0, exemplar("trace-a1", "span-a1", 0.1, now - 120_000)),
            series(METRIC, "b", 2.0, exemplar("trace-b1", "span-b1", 0.3, now - 90_000)),
            series(OTHER_METRIC, "a", 3.0, exemplar("trace-q1", "span-q1", 0.4, now - 30_000))
        );

        String query = "sum by (job) (avg_over_time(" + METRIC + "[5m])) / sum by (job) (avg_over_time(" + OTHER_METRIC + "[5m]))";
        ObjectPath path = queryExemplars(query, now - 600_000, now);
        List<Object> data = path.evaluate("data");
        List<Object> seriesLabels = new ArrayList<>();
        for (Object series : data) {
            seriesLabels.add(ObjectPath.evaluate(series, "seriesLabels"));
        }
        assertThat(
            seriesLabels,
            containsInAnyOrder(
                Map.of("__name__", OTHER_METRIC, "job", "api", "instance", "a"),
                Map.of("__name__", METRIC, "job", "api", "instance", "a"),
                Map.of("__name__", METRIC, "job", "api", "instance", "b")
            )
        );
    }

    /**
     * The samples a selector with an offset reads lie before the requested time range, and so do its exemplars.
     * This is an accepted deviation from how prometheus does it, but it is arguable more correct:
     * We only show the exemplars that actually influenced the metric computation.
     * */
    public void testOffsetMovesExemplars() throws Exception {
        ingest(series(METRIC, "a", 1.0, exemplar("trace-a1", "span-a1", 0.1, now - 120_000)));

        ObjectPath path = queryExemplars("avg_over_time(" + METRIC + "[5m])", now - 600_000, now);
        assertThat(path.evaluate("data"), hasSize(1));

        path = queryExemplars("avg_over_time(" + METRIC + "[5m] offset 1h)", now - 600_000, now);
        assertThat(path.evaluate("data"), empty());
    }

    /** A range selector whose range is shorter than the time range still selects the exemplars of the whole time range. */
    public void testRangeShorterThanTimeRange() throws Exception {
        ingest(
            series(METRIC, "a", 1.0, exemplar("trace-1", "span-1", 0.1, now - 300_000), exemplar("trace-2", "span-2", 0.2, now - 30_000))
        );

        ObjectPath path = queryExemplars("sum(rate(" + METRIC + "[1m]))", now - 600_000, now);
        assertThat(path.evaluate("data"), hasSize(1));
        assertThat(path.evaluate("data.0.exemplars"), hasSize(2));
        assertThat(path.evaluate("data.0.exemplars.0.labels.trace_id"), equalTo("trace-1"));
        assertThat(path.evaluate("data.0.exemplars.1.labels.trace_id"), equalTo("trace-2"));
    }

    /** Queries that neither the instant nor the range query endpoint accepts are rejected here as well. */
    public void testUnsupportedQueriesAreRejected() throws IOException {
        ingest(series(METRIC, "a", 1.0, exemplar("trace-a1", "span-a1", 0.1, now - 120_000)));
        assertBadData(METRIC + "[5m]", "invalid expression type \"range vector\" for range query");
        assertBadData(METRIC + " @ " + (now / 1000), "@ modifiers are not supported at this time");
    }

    private void assertBadData(String query, String expectedError) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> queryExemplars(query, now - 600_000, now));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        ObjectPath path = ObjectPath.createFromResponse(e.getResponse());
        assertThat(path.evaluate("errorType"), equalTo("bad_data"));
        assertThat(path.evaluate("error"), containsString(expectedError));
    }

    /** The time range selects the exemplars recorded within it, the lookback of the selectors doesn't extend it. */
    public void testExemplarsWithinTimeRange() throws Exception {
        ingest(
            series(
                METRIC,
                "a",
                1.0,
                exemplar("trace-1", "span-1", 0.1, now - 180_000),
                exemplar("trace-2", "span-2", 0.2, now - 120_000),
                exemplar("trace-3", "span-3", 0.3, now - 60_000)
            )
        );

        ObjectPath path = queryExemplars(METRIC, now - 150_000, now - 90_000);
        assertThat(path.evaluate("data"), hasSize(1));
        assertThat(path.evaluate("data.0.exemplars"), hasSize(1));
        assertThat(path.evaluate("data.0.exemplars.0.labels.trace_id"), equalTo("trace-2"));

        path = queryExemplars(METRIC, now - 120_000, now - 120_000);
        assertThat(path.evaluate("data.0.exemplars"), hasSize(1));
        assertThat(path.evaluate("data.0.exemplars.0.labels.trace_id"), equalTo("trace-2"));
    }

    public void testNoExemplarsForUnknownMetric() throws Exception {
        ingest(series(METRIC, "a", 1.0, exemplar("trace-1", "span-1", 0.1, now - 60_000)));
        assertThat(queryExemplars("unknown_metric", now - 600_000, now).evaluate("data"), empty());
    }

    public void testQueryWithoutSelectors() throws Exception {
        assertThat(queryExemplars("1 + 1", now - 600_000, now).evaluate("data"), empty());
        // a string is only accepted by the instant query endpoint
        assertThat(queryExemplars("\"foo\"", now, now).evaluate("data"), empty());
    }

    public void testEndBeforeStart() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> queryExemplars(METRIC, now, now - 1));
        assertBadData(e.getResponse(), "end timestamp must not be before start timestamp");
    }

    public void testMissingQuery() throws IOException {
        Request request = prometheusGetRequest("/_prometheus/api/v1/query_exemplars", exemplarsReadApiKey);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertBadData(e.getResponse(), "required parameter \"query\" is missing");
    }

    public void testMissingTimeRange() throws IOException {
        Request request = prometheusGetRequest("/_prometheus/api/v1/query_exemplars?query=" + METRIC, exemplarsReadApiKey);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertBadData(e.getResponse(), "required parameter \"start\" is missing");

        Request withStart = prometheusGetRequest(
            "/_prometheus/api/v1/query_exemplars?query=" + METRIC + "&start=" + Instant.ofEpochMilli(now),
            exemplarsReadApiKey
        );
        e = expectThrows(ResponseException.class, () -> client().performRequest(withStart));
        assertBadData(e.getResponse(), "required parameter \"end\" is missing");
    }

    private static void assertBadData(Response response, String error) throws IOException {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        ObjectPath path = ObjectPath.createFromResponse(response);
        assertThat(path.evaluate("status"), equalTo("error"));
        assertThat(path.evaluate("errorType"), equalTo("bad_data"));
        assertThat(path.evaluate("error"), equalTo(error));
    }

    private ObjectPath queryExemplars(String query, long start, long end) throws IOException {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_exemplars",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("start", Instant.ofEpochMilli(start).toString()),
            new BasicNameValuePair("end", Instant.ofEpochMilli(end).toString())
        );
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .removeHeader("Authorization")
                .addHeader("Authorization", "ApiKey " + exemplarsReadApiKey)
                .build()
        );
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return ObjectPath.createFromResponse(response);
    }

    private void ingest(RemoteWrite.TimeSeries... series) throws IOException {
        RemoteWrite.WriteRequest.Builder writeRequest = RemoteWrite.WriteRequest.newBuilder();
        for (RemoteWrite.TimeSeries timeSeries : series) {
            writeRequest.addTimeseries(timeSeries);
        }
        Request request = new Request("POST", "/_prometheus/api/v1/write");
        request.setEntity(new ByteArrayEntity(writeRequest.build().toByteArray(), ContentType.create("application/x-protobuf")));
        addWriteAuth(request);
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(204));
        client().performRequest(new Request("POST", "/metrics-*,exemplars-*/_refresh"));
    }

    private RemoteWrite.TimeSeries series(String metric, String instance, double value, RemoteWrite.Exemplar... exemplars) {
        return RemoteWrite.TimeSeries.newBuilder()
            .addLabels(label("__name__", metric))
            .addLabels(label("job", "api"))
            .addLabels(label("instance", instance))
            .addSamples(sample(value, now - 60_000))
            .addAllExemplars(List.of(exemplars))
            .build();
    }

    private static RemoteWrite.Exemplar exemplar(String traceId, String spanId, double value, long timestamp) {
        return RemoteWrite.Exemplar.newBuilder()
            .addLabels(label("trace_id", traceId))
            .addLabels(label("span_id", spanId))
            .addLabels(label("user", "user-" + traceId))
            .setValue(value)
            .setTimestamp(timestamp)
            .build();
    }
}
