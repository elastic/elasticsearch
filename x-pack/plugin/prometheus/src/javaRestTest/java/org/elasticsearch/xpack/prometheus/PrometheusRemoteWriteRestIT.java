/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class PrometheusRemoteWriteRestIT extends ESRestTestCase {

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

    public void testRemoteWriteEndpointWithEmptyRequest() throws Exception {
        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().build();
        sendAndAssertSuccess(writeRequest);
    }

    public void testRemoteWriteIndexesGaugeMetric() throws Exception {
        long timestamp = System.currentTimeMillis();
        String metricName = "test_gauge_metric";

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(timeSeries(metricName, Map.of("job", "test_job", "instance", "localhost:9090"), sample(42.5, timestamp)))
            .build();

        sendAndAssertSuccess(writeRequest);

        Map<String, Object> source = searchSingleDoc(metricName);

        // Verify document structure
        assertThat(source, hasKey("@timestamp"));
        assertThat(source.get(metricName), equalTo(42.5));

        // Verify labels
        assertLabel(source, "__name__", metricName);
        assertLabel(source, "job", "test_job");
        assertLabel(source, "instance", "localhost:9090");

        // Verify data_stream fields
        assertDataStream(source, "metrics", "generic.prometheus", "default");
    }

    public void testRemoteWriteIndexesCounterMetric() throws Exception {
        long timestamp = System.currentTimeMillis();
        String metricName = "http_requests_total";

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(timeSeries(metricName, Map.of("method", "GET"), sample(1000.0, timestamp)))
            .build();

        sendAndAssertSuccess(writeRequest);

        Map<String, Object> source = searchSingleDoc(metricName);
        assertThat(source.get(metricName), equalTo(1000.0));
        assertLabel(source, "method", "GET");
    }

    public void testRemoteWriteWithMultipleTimeseriesAndSamples() throws Exception {
        long baseTimestamp = System.currentTimeMillis();
        String metric1 = "multi_ts_metric_one";
        String metric2 = "multi_ts_metric_two";

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                timeSeries(
                    metric1,
                    Map.of("job", "test"),
                    sample(1.0, baseTimestamp - 2000),
                    sample(2.0, baseTimestamp - 1000),
                    sample(3.0, baseTimestamp)
                )
            )
            .addTimeseries(timeSeries(metric2, Map.of(), sample(100.0, baseTimestamp)))
            .build();

        sendAndAssertSuccess(writeRequest);

        assertThat(searchDocs(metric1), hasSize(3));

        List<Map<String, Object>> hits2 = searchDocs(metric2);
        assertThat(hits2, hasSize(1));
        assertThat(hits2.get(0), hasKey(metric2));
    }

    public void testRemoteWriteMissingNameLabelReturns400() throws Exception {
        long timestamp = System.currentTimeMillis();

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(
                RemoteWrite.TimeSeries.newBuilder().addLabels(label("job", "test_job")).addSamples(sample(42.0, timestamp)).build()
            )
            .build();

        String body = sendAndAssertBadRequest(writeRequest);
        assertThat(body, containsString("missing __name__ label"));
    }

    public void testRemoteWriteWithCustomDataset() throws Exception {
        String metricName = "custom_dataset_metric";
        sendAndAssertSuccess(simpleWriteRequest(metricName), "/_prometheus/myapp/api/v1/write");

        Map<String, Object> source = searchSingleDoc("metrics-myapp.prometheus-default", metricName);
        assertDataStream(source, "metrics", "myapp.prometheus", "default");
    }

    public void testRemoteWriteWithCustomDatasetAndNamespace() throws Exception {
        String metricName = "custom_ns_metric";
        sendAndAssertSuccess(simpleWriteRequest(metricName), "/_prometheus/myapp/production/api/v1/write");

        Map<String, Object> source = searchSingleDoc("metrics-myapp.prometheus-production", metricName);
        assertDataStream(source, "metrics", "myapp.prometheus", "production");
    }

    public void testRemoteWriteWithInvalidCustomDatasetReturns400() throws Exception {
        String body = sendAndAssertBadRequest(simpleWriteRequest("invalid_dataset_metric"), "/_prometheus/my-app/api/v1/write");
        assertThat(body, containsString("data stream dataset 'my-app' contains disallowed characters, must conform to regex ["));
    }

    public void testRemoteWriteWithInvalidCustomNamespaceReturns400() throws Exception {
        String body = sendAndAssertBadRequest(simpleWriteRequest("invalid_namespace_metric"), "/_prometheus/myapp/foo:bar/api/v1/write");
        assertThat(body, containsString("data stream namespace 'foo:bar' contains disallowed characters, must conform to regex ["));
    }

    // --- helpers ---

    private static RemoteWrite.WriteRequest simpleWriteRequest(String metricName) {
        return RemoteWrite.WriteRequest.newBuilder()
            .addTimeseries(timeSeries(metricName, Map.of(), sample(1.0, System.currentTimeMillis())))
            .build();
    }

    private static RemoteWrite.TimeSeries timeSeries(String metricName, Map<String, String> labels, RemoteWrite.Sample... samples) {
        RemoteWrite.TimeSeries.Builder builder = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metricName));
        labels.forEach((k, v) -> builder.addLabels(label(k, v)));
        for (RemoteWrite.Sample s : samples) {
            builder.addSamples(s);
        }
        return builder.build();
    }

    private static RemoteWrite.Label label(String name, String value) {
        return RemoteWrite.Label.newBuilder().setName(name).setValue(value).build();
    }

    private static RemoteWrite.Sample sample(double value, long timestamp) {
        return RemoteWrite.Sample.newBuilder().setValue(value).setTimestamp(timestamp).build();
    }

    private void sendAndAssertSuccess(RemoteWrite.WriteRequest writeRequest) throws IOException {
        sendAndAssertSuccess(writeRequest, "/_prometheus/api/v1/write");
    }

    private void sendAndAssertSuccess(RemoteWrite.WriteRequest writeRequest, String endpoint) throws IOException {
        Request request = new Request("POST", endpoint);
        request.setEntity(new ByteArrayEntity(writeRequest.toByteArray(), ContentType.create("application/x-protobuf")));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(204));
    }

    private String sendAndAssertBadRequest(RemoteWrite.WriteRequest writeRequest) throws IOException {
        return sendAndAssertBadRequest(writeRequest, "/_prometheus/api/v1/write");
    }

    private String sendAndAssertBadRequest(RemoteWrite.WriteRequest writeRequest, String endpoint) throws IOException {
        Request request = new Request("POST", endpoint);
        request.setEntity(new ByteArrayEntity(writeRequest.toByteArray(), ContentType.create("application/x-protobuf")));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        return EntityUtils.toString(e.getResponse().getEntity());
    }

    /**
     * Searches for all indexed documents matching the given metric name and returns their _source maps.
     */
    private List<Map<String, Object>> searchDocs(String metricName) throws IOException {
        return searchDocs(DEFAULT_DATA_STREAM, metricName);
    }

    private List<Map<String, Object>> searchDocs(String dataStream, String metricName) throws IOException {
        Request refresh = new Request("POST", "/" + dataStream + "/_refresh");
        client().performRequest(refresh);

        Request search = new Request("GET", "/" + dataStream + "/_search");
        search.setJsonEntity(org.elasticsearch.common.Strings.format("""
            {
              "query": {
                "term": {
                  "labels.__name__": "%s"
                }
              }
            }
            """, metricName));
        Response response = client().performRequest(search);
        Map<String, Object> searchResult = entityAsMap(response);

        @SuppressWarnings("unchecked")
        Map<String, Object> hitsWrapper = (Map<String, Object>) searchResult.get("hits");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsWrapper.get("hits");

        return hits.stream().map(hit -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> src = (Map<String, Object>) hit.get("_source");
            return src;
        }).toList();
    }

    /**
     * Convenience method that asserts exactly one document was indexed for the given metric and returns its _source.
     */
    private Map<String, Object> searchSingleDoc(String metricName) throws IOException {
        return searchSingleDoc(DEFAULT_DATA_STREAM, metricName);
    }

    private Map<String, Object> searchSingleDoc(String dataStream, String metricName) throws IOException {
        List<Map<String, Object>> docs = searchDocs(dataStream, metricName);
        assertThat(docs, hasSize(1));
        return docs.get(0);
    }

    @SuppressWarnings("unchecked")
    private static void assertDataStream(Map<String, Object> source, String type, String dataset, String namespace) {
        Map<String, Object> dataStream = (Map<String, Object>) source.get("data_stream");
        assertThat(dataStream.get("type"), equalTo(type));
        assertThat(dataStream.get("dataset"), equalTo(dataset));
        assertThat(dataStream.get("namespace"), equalTo(namespace));
    }

    @SuppressWarnings("unchecked")
    private static void assertLabel(Map<String, Object> source, String labelName, String expectedValue) {
        Map<String, Object> labels = (Map<String, Object>) source.get("labels");
        assertThat(labels.get(labelName), equalTo(expectedValue));
    }
}
