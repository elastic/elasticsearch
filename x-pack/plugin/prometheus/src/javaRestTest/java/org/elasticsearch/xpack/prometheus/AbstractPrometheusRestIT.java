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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Base class for Prometheus REST integration tests.
 *
 * <p>Provides a superuser-authenticated admin client for cluster management operations
 * (index refresh, search, template setup), plus minimal-privilege API keys for
 * authenticating actual Prometheus endpoint calls:
 * <ul>
 *   <li>{@link #writeApiKey} — {@code create_doc} + {@code auto_configure} on {@code metrics-*},
 *       sufficient for {@code /_prometheus/api/v1/write}</li>
 *   <li>{@link #readApiKey} — {@code read} on {@code metrics-*},
 *       sufficient for all query and metadata endpoints</li>
 * </ul>
 */
public abstract class AbstractPrometheusRestIT extends ESRestTestCase {

    protected static final String USER = "test_admin";
    protected static final String PASS = "x-pack-test-password";
    protected static final String DEFAULT_DATA_STREAM = "metrics-generic.prometheus-default";

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

    protected String writeApiKey;
    protected String readApiKey;

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void createApiKeys() throws IOException {
        writeApiKey = createApiKey("prometheus-write-key", "metrics-*", "create_doc", "auto_configure");
        readApiKey = createApiKey("prometheus-read-key", "metrics-*", "read");
    }

    /**
     * Adds the write API key ({@code create_doc} + {@code auto_configure}) to the given request.
     * Use for requests to {@code /_prometheus/api/v1/write}.
     */
    protected void addWriteAuth(Request request) {
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + writeApiKey).build());
    }

    /**
     * Adds the read API key to the given request.
     * Use for requests to all Prometheus query and metadata endpoints.
     */
    protected void addReadAuth(Request request) {
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + readApiKey).build());
    }

    // --- sample data helpers ---

    /**
     * Writes 5 evenly-spaced samples for {@code metricName} (job=test_job, instance=localhost:9090)
     * starting at 2026-01-01T00:00:00Z with a 1-minute step, sets the TSDS start_time accordingly,
     * and asserts zero indexing failures.
     * Suitable as setup data for query-range and instant-query tests.
     */
    protected void ingestTestData(String metricName) throws IOException {
        long baseTimestamp = 1767225600000L; // 2026-01-01T00:00:00Z

        Request putCustomTemplate = new Request("PUT", "/_component_template/metrics-prometheus@custom");
        putCustomTemplate.setJsonEntity("""
            {
              "template": {
                "settings": {
                  "index": {
                    "time_series": {
                      "start_time": "2026-01-01T00:00:00Z"
                    }
                  }
                }
              }
            }
            """);
        client().performRequest(putCustomTemplate);

        RemoteWrite.WriteRequest.Builder writeRequestBuilder = RemoteWrite.WriteRequest.newBuilder();
        for (int i = 0; i < 5; i++) {
            writeRequestBuilder.addTimeseries(
                RemoteWrite.TimeSeries.newBuilder()
                    .addLabels(label("__name__", metricName))
                    .addLabels(label("job", "test_job"))
                    .addLabels(label("instance", "localhost:9090"))
                    .addSamples(sample(i * 10.0, baseTimestamp + i * 60_000L))
                    .build()
            );
        }

        Request writeRequest = new Request("POST", "/_prometheus/api/v1/write");
        writeRequest.setEntity(
            new ByteArrayEntity(writeRequestBuilder.build().toByteArray(), ContentType.create("application/x-protobuf"))
        );
        addWriteAuth(writeRequest);
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), equalTo(204));
        if (writeResponse.getEntity() != null) {
            assertThat(EntityUtils.toString(writeResponse.getEntity()), equalTo(""));
        }

        client().performRequest(new Request("POST", "/" + DEFAULT_DATA_STREAM + "/_refresh"));

        Request searchFailures = new Request("GET", "/" + DEFAULT_DATA_STREAM + "::failures/_search");
        searchFailures.setJsonEntity("""
            {
              "track_total_hits": true,
              "size": 0
            }
            """);
        ObjectPath failuresPath = ObjectPath.createFromResponse(client().performRequest(searchFailures));
        assertThat(((Number) failuresPath.evaluate("hits.total.value")).intValue(), equalTo(0));
    }

    /**
     * Writes a single metric sample via remote write and refreshes {@link #DEFAULT_DATA_STREAM}.
     * Uses the write API key.
     */
    protected void writeMetric(String metricName, Map<String, String> labels) throws IOException {
        writeMetric(metricName, labels, 1.0);
    }

    protected void writeMetric(String metricName, Map<String, String> labels, double value) throws IOException {
        RemoteWrite.TimeSeries.Builder ts = RemoteWrite.TimeSeries.newBuilder().addLabels(label("__name__", metricName));
        labels.forEach((k, v) -> ts.addLabels(label(k, v)));
        ts.addSamples(sample(value, System.currentTimeMillis()));

        RemoteWrite.WriteRequest writeRequest = RemoteWrite.WriteRequest.newBuilder().addTimeseries(ts.build()).build();

        Request request = new Request("POST", "/_prometheus/api/v1/write");
        request.setEntity(new ByteArrayEntity(snappyEncode(writeRequest.toByteArray()), ContentType.create("application/x-protobuf")));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.CONTENT_ENCODING, "snappy"));
        addWriteAuth(request);
        client().performRequest(request);
        client().performRequest(new Request("POST", "/" + DEFAULT_DATA_STREAM + "/_refresh"));
    }

    protected static RemoteWrite.Label label(String name, String value) {
        return RemoteWrite.Label.newBuilder().setName(name).setValue(value).build();
    }

    protected static RemoteWrite.Sample sample(double value, long timestamp) {
        return RemoteWrite.Sample.newBuilder().setValue(value).setTimestamp(timestamp).build();
    }

    protected static byte[] snappyEncode(byte[] input) {
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

    // --- search helpers ---

    /**
     * Searches for all indexed documents matching the given metric name in {@link #DEFAULT_DATA_STREAM}
     * and returns their {@code _source} maps.
     */
    protected List<Map<String, Object>> searchDocs(String metricName) throws IOException {
        return searchDocs(DEFAULT_DATA_STREAM, metricName);
    }

    protected List<Map<String, Object>> searchDocs(String dataStream, String metricName) throws IOException {
        client().performRequest(new Request("POST", "/" + dataStream + "/_refresh"));

        Request search = new Request("GET", "/" + dataStream + "/_search");
        search.setJsonEntity(Strings.format("""
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
     * Asserts exactly one document was indexed for the given metric and returns its {@code _source}.
     */
    protected ObjectPath searchSingleDoc(String metricName) throws IOException {
        return searchSingleDoc(DEFAULT_DATA_STREAM, metricName);
    }

    protected ObjectPath searchSingleDoc(String dataStream, String metricName) throws IOException {
        List<Map<String, Object>> docs = searchDocs(dataStream, metricName);
        assertThat(docs, hasSize(1));
        return new ObjectPath(docs.getFirst());
    }

    protected boolean dataStreamExists(String dataStream) throws IOException {
        try {
            client().performRequest(new Request("GET", "/_data_stream/" + dataStream));
            return true;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    // --- security helpers ---

    protected static String createApiKey(String name, String indexPattern, String... privileges) throws IOException {
        StringBuilder privilegeArray = new StringBuilder();
        for (int i = 0; i < privileges.length; i++) {
            if (i > 0) privilegeArray.append("\", \"");
            privilegeArray.append(privileges[i]);
        }
        Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity("""
            {
              "name": "$NAME",
              "role_descriptors": {
                "role": {
                  "index": [
                    {
                      "names": ["$INDEX_PATTERN"],
                      "privileges": ["$PRIVILEGES"]
                    }
                  ]
                }
              }
            }
            """.replace("$NAME", name).replace("$INDEX_PATTERN", indexPattern).replace("$PRIVILEGES", privilegeArray));
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        return response.evaluate("encoded");
    }
}
