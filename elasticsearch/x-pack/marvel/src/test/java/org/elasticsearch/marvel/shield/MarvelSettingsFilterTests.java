/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class MarvelSettingsFilterTests extends MarvelIntegTestCase {

    private CloseableHttpClient httpClient = HttpClients.createDefault();

    @After
    public void cleanup() throws IOException {
        httpClient.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.agent.exporters._http.type", "http")
                .put("xpack.monitoring.agent.exporters._http.enabled", false)
                .put("xpack.monitoring.agent.exporters._http.auth.username", "_user")
                .put("xpack.monitoring.agent.exporters._http.auth.password", "_passwd")
                .put("xpack.monitoring.agent.exporters._http.ssl.truststore.path", "/path/to/truststore")
                .put("xpack.monitoring.agent.exporters._http.ssl.truststore.password", "_passwd")
                .put("xpack.monitoring.agent.exporters._http.ssl.hostname_verification", true)
                .build();
    }

    public void testGetSettingsFiltered() throws Exception {
        String body = executeRequest("GET", "/_nodes/settings", null, null).getBody();
        Map<String, Object> response = JsonXContent.jsonXContent.createParser(body).map();
        Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
        for (Object node : nodes.values()) {
            Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) node).get("settings");

            assertThat(extractValue("xpack.monitoring.agent.exporters._http.type", settings), Matchers.<Object>equalTo("http"));
            assertThat(extractValue("xpack.monitoring.agent.exporters._http.enabled", settings), Matchers.<Object>equalTo("false"));
            assertNullSetting(settings, "xpack.monitoring.agent.exporters._http.auth.username");
            assertNullSetting(settings, "xpack.monitoring.agent.exporters._http.auth.password");
            assertNullSetting(settings, "xpack.monitoring.agent.exporters._http.ssl.truststore.path");
            assertNullSetting(settings, "xpack.monitoring.agent.exporters._http.ssl.truststore.password");
            assertNullSetting(settings, "xpack.monitoring.agent.exporters._http.ssl.hostname_verification");
        }
    }

    private void assertNullSetting(Map<String, Object> settings, String setting) {
        assertThat(extractValue(setting, settings), nullValue());
    }

    protected HttpResponse executeRequest(String method, String path, String body, Map<String, String> params) throws IOException {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class,
                internalCluster().getMasterName());
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
        if (shieldEnabled) {
            requestBuilder.addHeader(BASIC_AUTH_HEADER,
                    basicAuthHeaderValue(ShieldSettings.TEST_USERNAME, new SecuredString(ShieldSettings.TEST_PASSWORD.toCharArray())));
        }
        return requestBuilder.execute();
    }
}
