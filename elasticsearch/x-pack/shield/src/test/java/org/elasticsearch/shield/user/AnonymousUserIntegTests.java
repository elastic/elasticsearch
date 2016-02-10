/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.authz.InternalAuthorizationService;
import org.elasticsearch.test.ShieldIntegTestCase;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AnonymousUserIntegTests extends ShieldIntegTestCase {
    private boolean authorizationExceptionsEnabled = randomBoolean();

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous")
                .put(InternalAuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), authorizationExceptionsEnabled)
                .build();
    }

    @Override
    public String configRoles() {
        return super.configRoles() + "\n" +
                "anonymous:\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ READ ]\n";
    }

    public void testAnonymousViaHttp() throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(new HttpGet(getNodeUrl() + "_nodes"))) {
            int statusCode = response.getStatusLine().getStatusCode();
            String data = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
            if (authorizationExceptionsEnabled) {
                assertThat(statusCode, is(403));
                assertThat(response.getFirstHeader("WWW-Authenticate"), nullValue());
                assertThat(data, containsString("security_exception"));
            } else {
                assertThat(statusCode, is(401));
                assertThat(response.getFirstHeader("WWW-Authenticate"), notNullValue());
                assertThat(response.getFirstHeader("WWW-Authenticate").getValue(), containsString("Basic"));
                assertThat(data, containsString("security_exception"));
            }
        }
    }

    private String getNodeUrl() {
        TransportAddress transportAddress =
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses());
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        return String.format(Locale.ROOT, "http://%s:%s/", "localhost", inetSocketTransportAddress.address().getPort());
    }
}
