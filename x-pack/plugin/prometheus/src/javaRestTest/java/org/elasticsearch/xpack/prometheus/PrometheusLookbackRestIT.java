/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/** Exercises selector lookback through the public HTTP API, including a dynamic cluster default. */
public class PrometheusLookbackRestIT extends AbstractPrometheusRestIT {
    private static final Instant SAMPLE_TIME = Instant.parse("2026-01-01T00:00:00Z");
    private static final String METRIC = "lookback_gauge";
    private static final String SETTING = "esql.query.settings.promql_lookback_delta";

    public void testRangeCarriesSampleUntilLookbackExpires() throws Exception {
        ingestSample();
        var result = range(60, 360, "60", null);
        List<Map<String, Object>> series = result.evaluate("data.result");
        assertThat(series, hasSize(1));
        List<List<Object>> values = result.evaluate("data.result.0.values");
        assertThat(values, hasSize(4));
        for (int i = 0; i < values.size(); i++) {
            assertThat(((Number) values.get(i).get(0)).longValue(), equalTo(SAMPLE_TIME.getEpochSecond() + 60L * (i + 1)));
            assertThat(Double.parseDouble((String) values.get(i).get(1)), equalTo(42.0));
        }
    }

    public void testLatestSampleReplacesCarriedValue() throws Exception {
        ingestTestData(
            RemoteWrite.WriteRequest.newBuilder()
                .addTimeseries(
                    RemoteWrite.TimeSeries.newBuilder()
                        .addLabels(label("__name__", METRIC))
                        .addLabels(label("host", "one"))
                        .addSamples(sample(42, SAMPLE_TIME.toEpochMilli()))
                        .addSamples(sample(84, SAMPLE_TIME.plusSeconds(90).toEpochMilli()))
                )
                .build(),
            SAMPLE_TIME
        );
        List<List<Object>> values = range(60, 240, "60", "2m").evaluate("data.result.0.values");
        assertThat(values, hasSize(3));
        assertThat(Double.parseDouble((String) values.get(0).get(1)), equalTo(42.0));
        assertThat(Double.parseDouble((String) values.get(1).get(1)), equalTo(84.0));
        assertThat(Double.parseDouble((String) values.get(2).get(1)), equalTo(84.0));
    }

    public void testQueryOverrideAndStepLargerThanLookback() throws Exception {
        ingestSample();
        assertThat(range(360, 360, "600", null).<List<?>>evaluate("data.result"), hasSize(0));
        assertThat(range(360, 360, "600", "10m").<List<?>>evaluate("data.result"), hasSize(1));
        assertThat(range(60, 180, "60", "120").<List<?>>evaluate("data.result.0.values"), hasSize(1));
        assertThat(instant(120, "2m").<List<?>>evaluate("data.result"), hasSize(0));
        assertThat(instant(119, "2m").<List<?>>evaluate("data.result"), hasSize(1));
        assertThat(range(119, 119, "17", "120.0").<List<?>>evaluate("data.result"), hasSize(1));
    }

    public void testSubMillisecondLookbackHasAnEmptyWindow() throws Exception {
        ingestSample();
        assertThat(instant(0, "0.0005").<List<?>>evaluate("data.result"), hasSize(0));
        assertThat(range(0, 60, "60", "0.0005").<List<?>>evaluate("data.result"), hasSize(0));
        // Values below one nanosecond truncate to zero and use the default instead.
        assertThat(instant(1, "1e-12").<List<?>>evaluate("data.result"), hasSize(1));
    }

    public void testDynamicDefaultAndCommandOverride() throws Exception {
        ingestSample();
        try {
            updateDefault("2m");
            assertThat(instant(180, null).<List<?>>evaluate("data.result"), hasSize(0));
            assertThat(range(180, 180, "60", null).<List<?>>evaluate("data.result"), hasSize(0));
            assertThat(instant(180, "5m").<List<?>>evaluate("data.result"), hasSize(1));
            assertThat(range(180, 180, "60", "5m").<List<?>>evaluate("data.result"), hasSize(1));
            assertThat(command(null).<List<?>>evaluate("values"), hasSize(0));
            assertThat(command("5m").<List<?>>evaluate("values"), hasSize(1));
            updateDefault("4m");
            assertThat(instant(180, null).<List<?>>evaluate("data.result"), hasSize(1));
        } finally {
            updateDefault(null);
        }
    }

    public void testNonPositiveOverrideUsesConfiguredDefault() throws Exception {
        ingestSample();
        try {
            updateDefault("2m");
            for (String value : List.of("0", "-1", "0ms")) {
                assertThat(instant(180, value).<List<?>>evaluate("data.result"), hasSize(0));
                assertThat(range(180, 180, "60", value).<List<?>>evaluate("data.result"), hasSize(0));
            }
        } finally {
            updateDefault(null);
        }
    }

    public void testInvalidLookbackIsBadData() throws Exception {
        for (String value : List.of("NaN", "Inf", "1e100", "bad")) {
            var error = expectThrows(ResponseException.class, () -> instant(60, value));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(error.getMessage(), containsString("bad_data"));
        }
    }

    private void ingestSample() throws Exception {
        ingestTestData(
            RemoteWrite.WriteRequest.newBuilder()
                .addTimeseries(
                    RemoteWrite.TimeSeries.newBuilder()
                        .addLabels(label("__name__", METRIC))
                        .addLabels(label("host", "one"))
                        .addSamples(sample(42, SAMPLE_TIME.toEpochMilli()))
                )
                .build(),
            SAMPLE_TIME
        );
    }

    private ObjectPath instant(long seconds, String lookback) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", METRIC),
            new BasicNameValuePair("time", SAMPLE_TIME.plusSeconds(seconds).toString())
        );
        if (lookback != null) request.addParameter("lookback_delta", lookback);
        return ObjectPath.createFromResponse(client().performRequest(request));
    }

    private ObjectPath range(long start, long end, String step, String lookback) throws Exception {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", METRIC),
            new BasicNameValuePair("start", SAMPLE_TIME.plusSeconds(start).toString()),
            new BasicNameValuePair("end", SAMPLE_TIME.plusSeconds(end).toString()),
            new BasicNameValuePair("step", step)
        );
        if (lookback != null) request.addParameter("lookback_delta", lookback);
        return ObjectPath.createFromResponse(client().performRequest(request));
    }

    private ObjectPath command(String lookback) throws Exception {
        String query = "PROMQL index="
            + DEFAULT_DATA_STREAM
            + " time=\""
            + SAMPLE_TIME.plusSeconds(180)
            + "\" "
            + (lookback == null ? "" : "lookback_delta=" + lookback + " ")
            + "result=("
            + METRIC
            + ")";
        Request request = new Request("POST", "/_query");
        try (var builder = org.elasticsearch.xcontent.json.JsonXContent.contentBuilder()) {
            builder.startObject().field("query", query).endObject();
            request.setJsonEntity(org.elasticsearch.common.Strings.toString(builder));
        }
        return ObjectPath.createFromResponse(adminClient().performRequest(request));
    }

    private void updateDefault(String value) throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\":{\"" + SETTING + "\":" + (value == null ? "null" : "\"" + value + "\"") + "}}");
        assertThat(adminClient().performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
    }
}
