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
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that a real OTLP protobuf request is captured by the audit trail as {@code request.raw_body}
 * with {@code request.raw_body_content_type=application/x-protobuf} and no {@code request.raw_body_content_encoding}
 * when the client sends no {@code Content-Encoding} header.
 */
public class OTLPAuditRawBodyIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(1)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.audit.logfile.events.emit_request_body", "true")
        .setting("xpack.security.audit.logfile.events.include", "[ \"authentication_success\" ]")
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

    public void testOtlpMetricsRequestRawBodyIsAudited() throws Exception {
        assertOtlpEndpointAuditsRawBody("/_otlp/v1/metrics");
    }

    public void testOtlpLogsRequestRawBodyIsAudited() throws Exception {
        assertOtlpEndpointAuditsRawBody("/_otlp/v1/logs");
    }

    public void testOtlpTracesRequestRawBodyIsAudited() throws Exception {
        assertOtlpEndpointAuditsRawBody("/_otlp/v1/traces");
    }

    private void assertOtlpEndpointAuditsRawBody(String path) throws Exception {
        byte[] body = randomByteArrayOfLength(randomIntBetween(1, 32));
        Request request = new Request("POST", path);
        request.setEntity(new ByteArrayEntity(body, ContentType.create("application/x-protobuf")));
        try {
            client().performRequest(request);
        } catch (ResponseException ignored) {
            // OTLP handler may reject the arbitrary bytes as invalid protobuf. The audit event still fires
            // in the security interceptor before dispatch, which is what this test verifies.
        }

        String expectedRawBody = Base64.getEncoder().encodeToString(body);
        Map<String, Object> event = findAuthenticationSuccessEventWithRawBody(path, expectedRawBody);
        assertThat(event, hasEntry("request.raw_body_content_type", "application/x-protobuf"));
        assertThat(event, not(hasKey("request.raw_body_content_encoding")));
        assertThat(event, not(hasKey("request.body")));
    }

    private Map<String, Object> findAuthenticationSuccessEventWithRawBody(String path, String expectedRawBody) throws Exception {
        List<Map<String, Object>> matches = new ArrayList<>();
        assertBusy(() -> {
            matches.clear();
            try (var auditLog = cluster.getNodeLog(0, LogType.AUDIT)) {
                for (String line : Streams.readAllLines(auditLog)) {
                    if (line.contains("authentication_success") == false) continue;
                    Map<String, Object> event = XContentHelper.convertToMap(XContentType.JSON.xContent(), line, true);
                    if ("authentication_success".equals(event.get("event.action"))
                        && path.equals(event.get("url.path"))
                        && expectedRawBody.equals(event.get("request.raw_body"))) {
                        matches.add(event);
                    }
                }
                assertThat("expected one authentication_success event with matching raw body for " + path, matches, hasSize(1));
            }
        }, 5, TimeUnit.SECONDS);
        return matches.getFirst();
    }
}
