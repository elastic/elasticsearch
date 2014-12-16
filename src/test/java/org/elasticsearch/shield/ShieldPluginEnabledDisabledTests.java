/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.integration.LicensingTests;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.transport.SecuredServerTransportService;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.elasticsearch.test.rest.json.JsonPath;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ClusterScope(scope = SUITE)
public class ShieldPluginEnabledDisabledTests extends ShieldIntegrationTest {

    private static boolean enabled;

    @BeforeClass
    public static void init() {
        enabled = randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        logger.info("******* shield is " + (enabled ? "enabled" : "disabled"));
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("shield.enabled", enabled)
                .put(InternalNode.HTTP_ENABLED, true)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return ImmutableSettings.settingsBuilder()
                .put(super.transportClientSettings())
                .put("shield.enabled", enabled)
                .build();
    }

    @Override
    protected Class<? extends Plugin> licensePluginClass() {
        return LicensingTests.InternalLicensePlugin.class;
    }

    @Override
    protected String licensePluginName() {
        return LicensingTests.InternalLicensePlugin.NAME;
    }

    @Test
    public void testTransportEnabledDisabled() throws Exception {
        for (TransportService service : internalCluster().getInstances(TransportService.class)) {
            Matcher<TransportService> matcher = instanceOf(SecuredServerTransportService.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(service, matcher);
        }
        for (Transport transport : internalCluster().getInstances(Transport.class)) {
            Matcher<Transport> matcher = instanceOf(NettySecuredTransport.class);
            if (!enabled) {
                matcher = not(matcher);
            }
            assertThat(transport, matcher);
        }
    }

    @Test
    public void testShieldInfoStatus() throws IOException {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpResponse response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport).method("GET").path("/_shield").addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                    basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME, new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))).execute();
            assertThat(response.getStatusCode(), is(OK.getStatus()));
            assertThat(new JsonPath(response.getBody()).evaluate("status").toString(), equalTo(enabled ? "enabled" : "disabled"));

            if (enabled) {
                LicensingTests.disableLicensing();
                response = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport).method("GET").path("/_shield").addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME, new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))).execute();
                assertThat(response.getStatusCode(), is(OK.getStatus()));
                assertThat(new JsonPath(response.getBody()).evaluate("status").toString(), equalTo("unlicensed"));
            }
        }
    }
}
