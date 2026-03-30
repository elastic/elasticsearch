/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /api/v1/series} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusSeriesRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";
    private static final String DEFAULT_DATA_STREAM = "metrics-generic.prometheus-default";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    // Validation — 400 responses

    public void testMissingMatchSelectorReturnsBadRequest() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/series");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("match[]"));
    }

    public void testInvalidSelectorSyntaxReturnsBadRequest() throws Exception {
        // {not valid!!!} is not valid PromQL
        Request request = seriesRequest("{not valid!!!}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = seriesRequest("up[5m]");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    // Response format — 200 with JSON envelope

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("test_gauge", Map.of());

        Response response = querySeries("test_gauge");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    // Data round-trip — write via remote write, read back via series

    public void testGetReturnsIndexedSeries() throws Exception {
        writeMetric("test_gauge", Map.of("job", "series_test", "instance", "localhost:9090"), 42.0);

        List<Map<String, Object>> data = querySeriesData("test_gauge");

        assertThat(data, hasSize(1));
        Map<String, Object> series = data.getFirst();
        assertThat(series.get("__name__"), equalTo("test_gauge"));
        assertThat(series.get("job"), equalTo("series_test"));
        assertThat(series.get("instance"), equalTo("localhost:9090"));
    }

    public void testGetWithLabelMatcherFiltersCorrectly() throws Exception {
        writeMetric("matched_metric", Map.of("job", "target_job"));
        writeMetric("other_metric", Map.of("job", "other_job"));

        // Query by exact metric name — should only return the matched metric
        List<Map<String, Object>> data = querySeriesData("matched_metric");

        assertThat(data, hasSize(1));
        assertThat(data.getFirst().get("__name__"), equalTo("matched_metric"));
        assertThat(data.getFirst().get("job"), equalTo("target_job"));
    }

    public void testSeriesWithIndexPattern() throws Exception {
        writeMetric("test_gauge_idx", Map.of("job", "index_test"));

        List<Map<String, Object>> data = querySeriesData("metrics-generic.prometheus-*", "test_gauge_idx");

        assertThat(data, hasSize(1));
        assertThat(data.getFirst().get("__name__"), equalTo("test_gauge_idx"));
    }

    // Helpers

    /**
     * Builds a series request with a single {@code match[]} parameter.
     * TODO: support multiple {@code match[]} values once multi-value query param support lands.
     */
    private static Request seriesRequest(String matcher) {
        return seriesRequest(null, matcher);
    }

    private static Request seriesRequest(String index, String matcher) {
        String path = index == null ? "/_prometheus/api/v1/series" : "/_prometheus/" + index + "/api/v1/series";
        Request request = new Request("GET", path);
        request.addParameter("match[]", matcher);
        return request;
    }

    private Response querySeries(String matcher) throws IOException {
        return querySeries(null, matcher);
    }

    private Response querySeries(String index, String matcher) throws IOException {
        return client().performRequest(seriesRequest(index, matcher));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> querySeriesData(String matcher) throws IOException {
        return querySeriesData(null, matcher);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> querySeriesData(String index, String matcher) throws IOException {
        return seriesData(querySeries(index, matcher));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> seriesData(Response response) throws IOException {
        Map<String, Object> body = entityAsMap(response);
        return (List<Map<String, Object>>) body.get("data");
    }

    private void writeMetric(String metricName, Map<String, String> labels) throws IOException {
        writeMetric(metricName, labels, 1.0);
    }

    private void writeMetric(String metricName, Map<String, String> labels, double value) throws IOException {
        RemoteWrite.TimeSeries.Builder ts = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metricName));
        labels.forEach((k, v) -> ts.addLabels(label(k, v)));
        ts.addSamples(sample(value, System.currentTimeMillis()));

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().addTimeseries(ts.build()).build();

        Request request = new Request("POST", "/_prometheus/api/v1/write");
        request.setEntity(new ByteArrayEntity(snappyEncode(writeRequest.toByteArray()), ContentType.create("application/x-protobuf")));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.CONTENT_ENCODING, "snappy"));
        client().performRequest(request);
        client().performRequest(new Request("POST", "/" + DEFAULT_DATA_STREAM + "/_refresh"));
    }

    private static RemoteWrite.Label label(String name, String value) {
        return RemoteWrite.Label.newBuilder().setName(name).setValue(value).build();
    }

    private static RemoteWrite.Sample sample(double value, long timestamp) {
        return RemoteWrite.Sample.newBuilder().setValue(value).setTimestamp(timestamp).build();
    }

    private static byte[] snappyEncode(byte[] input) {
        ByteBuf in = Unpooled.wrappedBuffer(input);
        ByteBuf out = Unpooled.buffer(input.length);
        try {
            new Snappy().encode(in, out, input.length);
            byte[] result = new byte[out.readableBytes()];
            out.readBytes(result);
            return result;
        } finally {
            in.release();
            out.release();
        }
    }
}
