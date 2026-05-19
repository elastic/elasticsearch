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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;

import java.io.IOException;
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
