/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that OTLP endpoints return 400 (no handler found) when {@code xpack.otel_data.enabled}
 * is {@code false}. When disabled the plugin registers no REST handlers, so the node has no route
 * for any {@code /_otlp/*} path. Elasticsearch returns 400 for unmatched routes.
 *
 * This class intentionally does NOT extend {@link AbstractOTLPIndexingRestIT} so that the
 * normal indexing test clusters are never started here.
 */
public class OTLPDisabledRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.otel_data.enabled", "false")
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

    public void testMetricsEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        assertOtlpEndpointNoHandler("/_otlp/v1/metrics");
    }

    public void testLogsEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        assertOtlpEndpointNoHandler("/_otlp/v1/logs");
    }

    public void testTracesEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        assertOtlpEndpointNoHandler("/_otlp/v1/traces");
    }

    private void assertOtlpEndpointNoHandler(String path) throws Exception {
        Request request = new Request("POST", path);
        request.setEntity(new ByteArrayEntity(new byte[0], ContentType.create("application/x-protobuf")));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        // Elasticsearch returns 400 (not 404) for requests with no registered handler
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }
}
