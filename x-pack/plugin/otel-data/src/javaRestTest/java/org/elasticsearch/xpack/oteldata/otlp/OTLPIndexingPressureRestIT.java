/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.apache.http.HttpHost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.equalTo;

/**
 * Standalone IT for the 429 indexing-pressure rejection. Uses a single cluster whose
 * coordinating limit (4MiB) is below the OTLP handler's upfront reservation of
 * {@code http.max_protobuf_content_length} (8MiB), so every OTLP request is rejected
 * with 429 before a single byte of the body is read.
 *
 * This class intentionally does NOT extend {@link AbstractOTLPIndexingRestIT} so that
 * signal-specific ITs (e.g. {@link OTLPMetricsIndexingRestIT}) never pay the cost of
 * starting this pressure cluster.
 */
public class OTLPIndexingPressureRestIT extends ESRestTestCase {

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
        .setting("indexing_pressure.memory.coordinating.limit", "4mb")
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

    /**
     * Any OTLP request is rejected with 429 because the handler's upfront reservation of
     * {@code http.max_protobuf_content_length} (8MiB) exceeds the 4MiB coordinating limit.
     * The body size is irrelevant — the 429 fires before any bytes are read.
     */
    public void testIndexingPressureLimitReturns429() throws Exception {
        byte[] body = new byte[1024];
        Request request = new Request("POST", "/_otlp/v1/metrics");
        request.setEntity(new ByteArrayEntity(body, ContentType.create("application/x-protobuf")));
        HttpHost[] hosts = parseClusterHosts(cluster.getHttpAddresses()).toArray(HttpHost[]::new);
        try (RestClient pressureClient = buildClient(restClientSettings(), hosts)) {
            ResponseException e = expectThrows(ResponseException.class, () -> pressureClient.performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(429));
        }
    }
}
