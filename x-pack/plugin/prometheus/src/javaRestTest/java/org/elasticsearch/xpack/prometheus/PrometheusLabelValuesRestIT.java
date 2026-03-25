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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /_prometheus/api/v1/label/{name}/values} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusLabelValuesRestIT extends ESRestTestCase {

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
        .feature(FeatureFlag.PROMETHEUS_FEATURE_FLAG)
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
        Request request = labelValuesRequest("job", "{not valid!!!}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = labelValuesRequest("job", "up[5m]");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("test_gauge", Map.of("job", "prometheus"));

        Response response = client().performRequest(labelValuesRequest("job"));

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testUnknownLabelReturnsEmptyData() throws Exception {
        Response response = client().performRequest(labelValuesRequest("label_that_does_not_exist_anywhere"));

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        List<String> data = labelValuesData(response);
        assertThat(data.isEmpty(), equalTo(true));
    }

    public void testGetReturnsValuesForRegularLabel() throws Exception {
        writeMetric("roundtrip_gauge", Map.of("job", "node_exporter", "instance", "host1:9100"));
        writeMetric("roundtrip_gauge", Map.of("job", "prometheus", "instance", "host2:9090"));

        List<String> values = labelValuesData(client().performRequest(labelValuesRequest("job")));

        assertThat(values, hasItem("node_exporter"));
        assertThat(values, hasItem("prometheus"));
    }

    public void testGetReturnsValuesForNameLabel() throws Exception {
        writeMetric("name_label_metric_a", Map.of("job", "test"));
        writeMetric("name_label_metric_b", Map.of("job", "test"));

        List<String> values = labelValuesData(client().performRequest(labelValuesRequest("__name__")));

        assertThat(values, hasItem("name_label_metric_a"));
        assertThat(values, hasItem("name_label_metric_b"));
    }

    public void testGetWithMatchSelectorFiltersValues() throws Exception {
        writeMetric("selector_metric", Map.of("job", "filtered_job", "env", "prod"));
        writeMetric("other_metric", Map.of("job", "other_job", "env", "staging"));

        // Only request values for "job" where the metric is selector_metric
        List<String> values = labelValuesData(client().performRequest(labelValuesRequest("job", "selector_metric")));

        assertThat(values, hasItem("filtered_job"));
        assertThat(values, not(hasItem("other_job")));
    }

    public void testGetValuesAreSorted() throws Exception {
        writeMetric("sorted_gauge", Map.of("job", "zebra"));
        writeMetric("sorted_gauge", Map.of("job", "alpha"));
        writeMetric("sorted_gauge", Map.of("job", "middle"));

        List<String> values = labelValuesData(client().performRequest(labelValuesRequest("job")));

        // Extract just the values that we wrote (there may be others from earlier tests)
        List<String> ours = values.stream().filter(v -> List.of("zebra", "alpha", "middle").contains(v)).toList();
        assertThat(ours, equalTo(List.of("alpha", "middle", "zebra")));
    }

    public void testUEncodedLabelNameIsDecoded() throws Exception {
        // U__http_2e_requests decodes to http.requests — which doesn't exist, so we just
        // verify the endpoint is reachable and returns a 200 with an empty data array.
        Response response = client().performRequest(new Request("GET", "/_prometheus/api/v1/label/U__http_2e_requests/values"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(entityAsMap(response).get("status"), equalTo("success"));
    }

    private static Request labelValuesRequest(String labelName, String... matchers) {
        Request request = new Request("GET", "/_prometheus/api/v1/label/" + labelName + "/values");
        for (String matcher : matchers) {
            request.addParameter("match[]", matcher);
        }
        return request;
    }

    @SuppressWarnings("unchecked")
    private List<String> labelValuesData(Response response) throws IOException {
        Map<String, Object> body = entityAsMap(response);
        return (List<String>) body.get("data");
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
