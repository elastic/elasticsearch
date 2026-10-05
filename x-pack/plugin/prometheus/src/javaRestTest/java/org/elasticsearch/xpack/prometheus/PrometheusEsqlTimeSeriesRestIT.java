/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end test for the {@code _timeseries} ES|QL metadata column when documents are ingested
 * via the Prometheus remote-write pipeline.
 *
 * <p>The remote-write transport action builds {@code _source} with {@link
 * org.elasticsearch.xcontent.XContentFactory#cborBuilder(org.elasticsearch.common.io.stream.StreamOutput)
 * cborBuilder(...)} and the metrics-prometheus@template index template defaults to
 * {@code index.mapping.source.mode: stored} on non-Enterprise licenses (and for trial/enterprise it would
 * be synthetic). With stored source the original CBOR bytes are preserved, which used to leak through as
 * binary content in the {@code _timeseries} keyword column. This test asserts that the column is JSON
 * regardless of how {@code _source} is encoded on disk.
 */
public class PrometheusEsqlTimeSeriesRestIT extends AbstractPrometheusRestIT {

    public void testTimeSeriesIsJsonAfterRemoteWrite() throws Exception {
        long timestamp = System.currentTimeMillis();
        String metricName = "go_gc_cleanups_executed_cleanups_total";

        Map<String, String> labels = Map.of("instance", "localhost:9090", "job", "prometheus");
        sendRemoteWrite(metricName, labels, 0.0, timestamp);

        ObjectPath response = runEsqlQuery(
            "TS metrics-generic.prometheus-default | WHERE @timestamp > NOW() - 1d | STATS LAST_OVER_TIME(" + metricName + ")"
        );

        List<Map<String, String>> columns = response.evaluate("columns");
        assertThat(columns, notNullValue());
        List<String> columnNames = columns.stream().map(c -> c.get("name")).toList();
        assertThat(columnNames, containsInAnyOrder("LAST_OVER_TIME(" + metricName + ")", "_timeseries"));

        List<List<Object>> rows = response.evaluate("values");
        assertThat(rows, hasSize(1));

        int timeseriesIdx = columnNames.indexOf("_timeseries");
        Object timeseriesValue = rows.getFirst().get(timeseriesIdx);
        assertThat(timeseriesValue, notNullValue());

        Map<String, Object> parsed = parseJsonObject(timeseriesValue.toString());

        @SuppressWarnings("unchecked")
        Map<String, Object> parsedLabels = (Map<String, Object>) parsed.get("labels");
        assertThat(parsedLabels, notNullValue());
        assertThat(parsedLabels, hasEntry("__name__", metricName));
        assertThat(parsedLabels, hasEntry("instance", "localhost:9090"));
        assertThat(parsedLabels, hasEntry("job", "prometheus"));
    }

    /**
     * Regression test for PromQL {@code without(<label>)} over Prometheus remote-write data. Labels are stored under a
     * {@code labels} passthrough field, so each dimension surfaces both as a concrete field ({@code labels.pod}) and a
     * short alias ({@code pod}); the {@code _timeseries} block loader excludes a dropped dimension by its concrete
     * field name. A regression in the PromQL translator made label resolution pick whichever name sorted first in the
     * leaf output, so dimensions lexically before {@code "labels"} ({@code cluster}, {@code job}, {@code instance})
     * resolved to the bare alias and leaked back into the output, while later ones ({@code pod}, {@code region}) dropped
     * correctly. Only a real passthrough mapping exercises this; the ES|QL unit fixtures use top-level dimensions.
     */
    public void testWithoutDropsEveryLabelRegardlessOfName() throws Exception {
        long timestamp = System.currentTimeMillis();
        // job/instance are constant, cluster/pod/region vary; the labels span both sides of the "labels" prefix
        // lexically: cluster/instance/job < labels < pod/region.
        sendRemoteWrite("app_mem", series("a", "p1", "r1"), 1.0, timestamp);
        sendRemoteWrite("app_mem", series("a", "p2", "r2"), 2.0, timestamp);
        sendRemoteWrite("app_mem", series("b", "p1", "r2"), 3.0, timestamp);
        sendRemoteWrite("app_mem", series("b", "p2", "r1"), 4.0, timestamp);

        for (String dropped : List.of("pod", "region", "cluster", "job", "instance")) {
            ObjectPath response = rangeQuery("sum without(" + dropped + ") (app_mem)", timestamp);
            List<Map<String, Object>> result = response.evaluate("data.result");
            assertThat("expected at least one output series for without(" + dropped + ")", result.isEmpty(), equalTo(false));
            for (Map<String, Object> seriesResult : result) {
                @SuppressWarnings("unchecked")
                Map<String, Object> metric = (Map<String, Object>) seriesResult.get("metric");
                assertThat(
                    "without(" + dropped + ") must drop [" + dropped + "] but it leaked: " + metric,
                    metric.containsKey(dropped),
                    equalTo(false)
                );
            }
        }
    }

    private static Map<String, String> series(String cluster, String pod, String region) {
        return Map.of("job", "test_job", "instance", "localhost:9090", "cluster", cluster, "pod", pod, "region", region);
    }

    private ObjectPath rangeQuery(String query, long centerTimestamp) throws IOException {
        Request request = prometheusReadRequest(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", query),
            new BasicNameValuePair("start", Instant.ofEpochMilli(centerTimestamp - 60_000L).toString()),
            new BasicNameValuePair("end", Instant.ofEpochMilli(centerTimestamp + 60_000L).toString()),
            new BasicNameValuePair("step", "30s")
        );
        return ObjectPath.createFromResponse(client().performRequest(request));
    }

    private void sendRemoteWrite(String metricName, Map<String, String> labels, double value, long timestamp) throws IOException {
        RemoteWrite.TimeSeries.Builder ts = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metricName));
        labels.forEach((k, v) -> ts.addLabels(label(k, v)));
        ts.addSamples(sample(value, timestamp));
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().addTimeseries(ts.build()).build();

        Request request = new Request("POST", "/_prometheus/api/v1/write");
        request.setEntity(new ByteArrayEntity(snappyEncode(writeRequest.toByteArray()), ContentType.create("application/x-protobuf")));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.CONTENT_ENCODING, "snappy").build());
        addWriteAuth(request);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(204));
        client().performRequest(new Request("POST", "/" + DEFAULT_DATA_STREAM + "/_refresh"));
    }

    private ObjectPath runEsqlQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\":\"" + query.replace("\"", "\\\"") + "\"}");
        return ObjectPath.createFromResponse(client().performRequest(request));
    }

    private static Map<String, Object> parseJsonObject(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }
}
