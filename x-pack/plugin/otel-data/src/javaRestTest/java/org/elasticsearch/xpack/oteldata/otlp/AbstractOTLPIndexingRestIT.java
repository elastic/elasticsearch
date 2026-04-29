/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractOTLPIndexingRestIT extends ESRestTestCase {

    protected static final String USER = "test_admin";
    protected static final String PASS = "x-pack-test-password";
    protected static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));
    protected static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .feature(FeatureFlag.OTLP_TRACES)
        .feature(FeatureFlag.OTLP_LOGS)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /** The OTLP endpoint path for this signal type, e.g. {@code "/_otlp/v1/metrics"}. */
    protected abstract String otlpEndpointPath();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/traces-otel@template"))));
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/logs-otel@template"))));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * A request body exceeding the default {@code http.max_protobuf_content_length} (8MiB) must be rejected with 413.
     * Uses the main {@link #cluster} where the coordinating limit is not tight, so the upfront reservation
     * of 8MiB succeeds and the body size check is what triggers the rejection.
     */
    public void testOversizedRequestReturns413() throws Exception {
        // 9MiB exceeds the default 8MiB http.max_protobuf_content_length
        byte[] oversizedBody = new byte[9 * 1024 * 1024];
        Request request = new Request("POST", otlpEndpointPath());
        request.setEntity(new ByteArrayEntity(oversizedBody, ContentType.create("application/x-protobuf")));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(413));
    }

    protected static String createApiKey(String indexPattern) throws IOException {
        return createApiKey(new String[] { indexPattern });
    }

    protected static String createApiKey(String... indexPatterns) throws IOException {
        StringBuilder indexPatternsJson = new StringBuilder();
        for (int i = 0; i < indexPatterns.length; i++) {
            if (i > 0) {
                indexPatternsJson.append(", ");
            }
            indexPatternsJson.append('"').append(indexPatterns[i]).append('"');
        }
        Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "otel-test-key",
              "role_descriptors": {
                "writer": {
                  "index": [
                    {
                      "names": [$INDEX_PATTERNS],
                      "privileges": ["create_doc", "auto_configure"]
                    }
                  ]
                }
              }
            }
            """.replace("$INDEX_PATTERNS", indexPatternsJson.toString()));
        ObjectPath createApiKeyResponse = ObjectPath.createFromResponse(client().performRequest(createApiKeyRequest));
        return createApiKeyResponse.evaluate("encoded");
    }

    protected ObjectPath search(String target) throws IOException {
        var response = client().performRequest(new Request("GET", target + "/_search"));
        assertOK(response);
        return ObjectPath.createFromResponse(response);
    }
}
