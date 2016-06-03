/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ShieldSettingsSource.getSSLSettingsForStore;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SslClientAuthTests extends ShieldIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // invert the require auth settings
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), true)
                .put("transport.profiles.default.xpack.security.ssl.client.auth", false)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    public void testThatHttpFailsWithoutSslClientAuth() throws IOException {
        SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(
                SSLContexts.createDefault(),
                SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

        try (RestClient restClient = restClient(HttpClients.custom().setSSLSocketFactory(socketFactory).build(), "https")) {
            restClient.performRequest("GET", "/", Collections.emptyMap(), null);
            fail("Expected SSLHandshakeException");
        } catch (SSLHandshakeException e) {
            assertThat(e.getMessage(), containsString("unable to find valid certification path to requested target"));
        }
    }

    public void testThatHttpWorksWithSslClientAuth() throws IOException {
        Settings settings = Settings.builder()
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient"))
                .build();
        ClientSSLService sslService = new ClientSSLService(settings, new Global(settings));

        SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(
                sslService.sslContext(),
                SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

        CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(socketFactory).build();

        try (RestClient restClient = restClient(client, "https")) {
            try (ElasticsearchResponse response = restClient.performRequest("GET", "/", Collections.emptyMap(), null,
                    new BasicHeader("Authorization", basicAuthHeaderValue(transportClientUsername(), transportClientPassword())))) {
                assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
                assertThat(EntityUtils.toString(response.getEntity()), containsString("You Know, for Search"));
            }
        }
    }

    public void testThatTransportWorksWithoutSslClientAuth() throws Exception {
        // specify an arbitrary keystore, that does not include the certs needed to connect to the transport protocol
        Path store = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks");

        if (Files.notExists(store)) {
            throw new ElasticsearchException("store path doesn't exist");
        }

        Settings settings = Settings.builder()
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.keystore.path", store)
                .put("xpack.security.ssl.keystore.password", "testclient-client-profile")
                .put("cluster.name", internalCluster().getClusterName())
                .put(Security.USER_SETTING.getKey(),
                        transportClientUsername() + ":" + new String(transportClientPassword().internalChars()))
                .build();
        try (TransportClient client = TransportClient.builder().settings(settings).addPlugin(XPackPlugin.class).build()) {
            Transport transport = internalCluster().getDataNodeInstance(Transport.class);
            TransportAddress transportAddress = transport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);

            assertGreenClusterState(client);
        }
    }
}
