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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /api/v1/labels} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusLabelsRestIT extends ESRestTestCase {

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

    public void testInvalidSelectorSyntaxReturnsBadRequest() throws Exception {
        // {not valid!!!} is not valid PromQL
        Request request = labelsRequest("{not valid!!!}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = labelsRequest("up[5m]");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testGetWithNoMatchSelectorReturnsSuccess() throws Exception {
        // match[] is optional for the labels endpoint (unlike series)
        writeMetric("labels_no_selector_gauge", Map.of());
        Request request = new Request("GET", "/_prometheus/api/v1/labels");
        Response response = client().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("labels_format_gauge", Map.of());

        Response response = queryLabels("labels_format_gauge");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testGetAlwaysReturnsNameLabel() throws Exception {
        writeMetric("labels_name_gauge", Map.of("job", "labels_test"));

        List<String> data = queryLabelsData("labels_name_gauge");

        assertThat(data, containsInAnyOrder("__name__", "job"));
    }

    public void testGetReturnsIndexedLabels() throws Exception {
        writeMetric("labels_round_trip_gauge", Map.of("job", "labels_test", "instance", "localhost:9090"));

        List<String> data = queryLabelsData("labels_round_trip_gauge");

        assertThat(data, containsInAnyOrder("__name__", "instance", "job"));
    }

    public void testGetWithMatchSelectorFiltersToMatchingLabels() throws Exception {
        writeMetric("labels_filtered_gauge", Map.of("unique_label", "only_here"));
        writeMetric("labels_other_gauge", Map.of("other_label", "other_value"));

        // Query by exact metric name — should return labels only from the matched series
        List<String> data = queryLabelsData("labels_filtered_gauge");

        assertThat(data, hasItem("unique_label"));
    }

    @SuppressWarnings("unchecked")
    public void testGetWithMultipleMatchSelectorsReturnsCombinedLabels() throws Exception {
        writeMetric("multi_labels_metric_a", Map.of("label_only_in_a", "value_a"));
        writeMetric("multi_labels_metric_b", Map.of("label_only_in_b", "value_b"));
        writeMetric("multi_labels_metric_c", Map.of("label_only_in_c", "value_c")); // must not appear in results

        // Use URIBuilder to send two match[] selectors in a single request, working around the
        // test client's single-value-per-key restriction on Request.addParameter.
        Request request = new Request(
            "GET",
            new URIBuilder("/_prometheus/api/v1/labels").addParameter("match[]", "multi_labels_metric_a")
                .addParameter("match[]", "multi_labels_metric_b")
                .build()
                .toString()
        );
        List<String> data = (List<String>) entityAsMap(client().performRequest(request)).get("data");

        assertThat(data, containsInAnyOrder("__name__", "label_only_in_a", "label_only_in_b"));
    }

    /** Builds a labels request with optional {@code match[]} parameters. */
    private static Request labelsRequest(String... matchers) {
        Request request = new Request("GET", "/_prometheus/api/v1/labels");
        for (String matcher : matchers) {
            request.addParameter("match[]", matcher);
        }
        return request;
    }

    private Response queryLabels(String... matchers) throws IOException {
        return client().performRequest(labelsRequest(matchers));
    }

    @SuppressWarnings("unchecked")
    private List<String> queryLabelsData(String... matchers) throws IOException {
        Map<String, Object> body = entityAsMap(queryLabels(matchers));
        return (List<String>) body.get("data");
    }

    private void writeMetric(String metricName, Map<String, String> labels) throws IOException {
        RemoteWrite.TimeSeries.Builder ts = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metricName));
        labels.forEach((k, v) -> ts.addLabels(label(k, v)));
        ts.addSamples(sample(1.0, System.currentTimeMillis()));

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
