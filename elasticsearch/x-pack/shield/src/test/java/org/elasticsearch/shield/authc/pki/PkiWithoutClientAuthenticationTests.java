/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;


import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

@ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, numDataNodes = 1)
public class PkiWithoutClientAuthenticationTests extends ShieldIntegTestCase {
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
                .put(ShieldNettyTransport.CLIENT_AUTH_SETTING.getKey(), false)
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(),
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
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new SecureRandom());
        CloseableHttpClient httpClient = HttpClients.custom().setSSLContext(sc).build();
        try (RestClient restClient =  createRestClient(httpClient, "https")) {
            try (ElasticsearchResponse response = restClient.performRequest("GET", "/_nodes", Collections.emptyMap(), null,
                    new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                            UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                                    new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))))) {
                assertThat(response.getStatusLine().getStatusCode(), is(200));
            }
        }
    }
}
