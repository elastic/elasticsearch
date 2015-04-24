/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests.ShieldSettings.TEST_PASSWORD;
import static org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests.ShieldSettings.TEST_USERNAME;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class WatcherSettingsFilterTests extends AbstractWatcherIntegrationTests {

    private CloseableHttpClient httpClient = HttpClients.createDefault();

    @After
    public void cleanup() throws IOException {
        httpClient.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .put("watcher.actions.email.service.account._email.smtp.host", "host.domain")
                .put("watcher.actions.email.service.account._email.smtp.port", 587)
                .put("watcher.actions.email.service.account._email.smtp.user", "_user")
                .put("watcher.actions.email.service.account._email.smtp.password", "_passwd")
                .build();
    }

    @Test
    public void testGetSettings_SmtpPassword() throws Exception {
        String body = executeRequest("GET", "/_nodes/settings", null, null).getBody();
        Map<String, Object> response = JsonXContent.jsonXContent.createParser(body).map();
        Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
        for (Object node : nodes.values()) {
            Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) node).get("settings");
            assertThat(XContentMapValues.extractValue("watcher.actions.email.service.account._email.smtp.user", settings), is((Object) "_user"));
            if (shieldEnabled()) {
                assertThat(XContentMapValues.extractValue("watcher.actions.email.service.account._email.smtp.password", settings), nullValue());
            } else {
                assertThat(XContentMapValues.extractValue("watcher.actions.email.service.account._email.smtp.password", settings), is((Object) "_passwd"));
            }
        }
    }

    protected HttpResponse executeRequest(String method, String path, String body, Map<String, String> params) throws IOException {
        HttpServerTransport httpServerTransport = getInstanceFromMaster(HttpServerTransport.class);
        HttpRequestBuilder requestBuilder = new HttpRequestBuilder(httpClient)
                .httpTransport(httpServerTransport)
                .method(method)
                .path(path);

        if (params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                requestBuilder.addParam(entry.getKey(), entry.getValue());
            }
        }
        if (body != null) {
            requestBuilder.body(body);
        }
        if (shieldEnabled()) {
            requestBuilder.addHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue(TEST_USERNAME, new SecuredString(TEST_PASSWORD.toCharArray())));
        }
        return requestBuilder.execute();
    }
}
