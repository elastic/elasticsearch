/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;


import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyHttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyTransport;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

@ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, numDataNodes = 1)
public class PkiWithoutClientAuthenticationTests extends SecurityIntegTestCase {
    private TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                @Override
                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
    };

    @Override
    public boolean sslTransportEnabled() {
        return true;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put(SecurityNettyTransport.CLIENT_AUTH_SETTING.getKey(), false)
                .put(SecurityNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(),
                        randomFrom(SSLClientAuth.NO.name(), false, "false", "FALSE", SSLClientAuth.NO.name().toLowerCase(Locale.ROOT)))
                .put("xpack.security.authc.realms.pki1.type", "pki")
                .put("xpack.security.authc.realms.pki1.order", "0")
                .build();
    }

    public void testThatTransportClientWorks() {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }

    public void testThatHttpWorks() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new SecureRandom());
        try (CloseableHttpClient httpClient = HttpClients.custom().setSslcontext(sc).build()) {
            HttpRequestBuilder requestBuilder = new HttpRequestBuilder(httpClient)
                    .host("localhost")
                    .port(((InetSocketTransportAddress)httpServerTransport.boundAddress().publishAddress()).address().getPort())
                    .protocol("https")
                    .method("GET")
                    .path("/_nodes");
            requestBuilder.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                    UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.DEFAULT_USER_NAME,
                            new SecuredString(SecuritySettingsSource.DEFAULT_PASSWORD.toCharArray())));
            HttpResponse response = requestBuilder.execute();
            assertThat(response.getStatusCode(), is(200));
        }
    }
}
