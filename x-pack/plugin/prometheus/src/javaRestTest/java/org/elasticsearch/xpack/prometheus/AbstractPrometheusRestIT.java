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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.prometheus.proto.RemoteWrite;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

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
