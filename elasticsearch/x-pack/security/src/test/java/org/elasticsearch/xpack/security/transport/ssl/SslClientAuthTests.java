/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.ssl;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackTransportClient;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertPathBuilderException;

import static org.elasticsearch.test.SecuritySettingsSource.getSSLSettingsForStore;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SslClientAuthTests extends SecurityIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // invert the require auth settings
                .put(SecurityNetty3Transport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty3HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), true)
                .put("transport.profiles.default.xpack.security.ssl.client.auth", false)
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    public void testThatHttpFailsWithoutSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(SSLContexts.createDefault(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            restClient.performRequest("GET", "/");
            fail("Expected SSLHandshakeException");
        } catch (SSLHandshakeException e) {
            Throwable t = ExceptionsHelper.unwrap(e, CertPathBuilderException.class);
            assertThat(t, instanceOf(CertPathBuilderException.class));
            assertThat(t.getMessage(), containsString("unable to find valid certification path to requested target"));
        }
    }

    public void testThatHttpWorksWithSslClientAuth() throws IOException {
        Settings settings = Settings.builder()
                .put(getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks", "testclient"))
                .build();
        ClientSSLService sslService = new ClientSSLService(settings, null, new Global(settings), null);
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslService.sslContext(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            Response response = restClient.performRequest("GET", "/",
                    new BasicHeader("Authorization", basicAuthHeaderValue(transportClientUsername(), transportClientPassword())));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            assertThat(EntityUtils.toString(response.getEntity()), containsString("You Know, for Search"));
        }
    }

    public void testThatTransportWorksWithoutSslClientAuth() throws IOException {
        // specify an arbitrary keystore, that does not include the certs needed to connect to the transport protocol
        Path store = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient-client-profile.jks");

        if (Files.notExists(store)) {
            throw new ElasticsearchException("store path doesn't exist");
        }

        Settings settings = Settings.builder()
                .put(SecurityNetty3Transport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.keystore.path", store)
                .put("xpack.security.ssl.keystore.password", "testclient-client-profile")
                .put("cluster.name", internalCluster().getClusterName())
                .put(Security.USER_SETTING.getKey(),
                        transportClientUsername() + ":" + new String(transportClientPassword().internalChars()))
                .build();
        try (TransportClient client = new XPackTransportClient(settings)) {
            Transport transport = internalCluster().getDataNodeInstance(Transport.class);
            TransportAddress transportAddress = transport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);

            assertGreenClusterState(client);
        }
    }
}
