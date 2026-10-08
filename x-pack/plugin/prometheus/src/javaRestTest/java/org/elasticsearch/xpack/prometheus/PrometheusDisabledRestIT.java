/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

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
 * Verifies that Prometheus endpoints return 400 (no handler found) when
 * {@code xpack.prometheus.enabled} is {@code false}. When disabled the plugin registers no REST
 * handlers, so the node has no route for any {@code /_prometheus/*} path. Elasticsearch returns 400
 * for unmatched routes.
 *
 * This class intentionally does NOT extend {@link AbstractPrometheusRestIT} so that the normal
 * test clusters (with SSL and API keys) are never started here.
 */
public class PrometheusDisabledRestIT extends ESRestTestCase {

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
        .setting("xpack.prometheus.enabled", "false")
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

    public void testRemoteWriteEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        Request request = new Request("POST", "/_prometheus/api/v1/write");
        request.setEntity(new ByteArrayEntity(new byte[0], ContentType.create("application/x-protobuf")));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        // Elasticsearch returns 400 (not 404) for requests with no registered handler
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testInstantQueryEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/query");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        // Elasticsearch returns 400 (not 404) for requests with no registered handler
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testStatusBuildInfoEndpointReturnsNoHandlerWhenDisabled() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/status/buildinfo");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        // Elasticsearch returns 400 (not 404) for requests with no registered handler
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }
}
