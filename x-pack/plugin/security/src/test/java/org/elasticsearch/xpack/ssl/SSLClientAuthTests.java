/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.TestXPackTransportClient;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertPathBuilderException;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SSLClientAuthTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // invert the require auth settings
                .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
                .put("xpack.security.http.ssl.enabled", true)
                .put("xpack.security.http.ssl.client_authentication", SSLClientAuth.REQUIRED)
                .put("transport.profiles.default.xpack.security.ssl.client_authentication", SSLClientAuth.NONE)
                .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testThatHttpFailsWithoutSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(SSLContexts.createDefault(), NoopHostnameVerifier.INSTANCE);
        try (RestClient restClient = createRestClient(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy), "https")) {
            restClient.performRequest("GET", "/");
            fail("Expected SSLHandshakeException");
        } catch (IOException e) {
            Throwable t = ExceptionsHelper.unwrap(e, CertPathBuilderException.class);
            assertThat(t, instanceOf(CertPathBuilderException.class));
            assertThat(t.getMessage(), containsString("unable to find valid certification path to requested target"));
        }
    }

    public void testThatHttpWorksWithSslClientAuth() throws IOException {
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(getSSLContext(), NoopHostnameVerifier.INSTANCE);
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

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testclient-client-profile");
        Settings settings = Settings.builder()
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.ssl.client_authentication", SSLClientAuth.NONE)
                .put("xpack.ssl.keystore.path", store)
                .setSecureSettings(secureSettings)
                .put("cluster.name", internalCluster().getClusterName())
                .put(SecurityField.USER_SETTING.getKey(),
                        transportClientUsername() + ":" + new String(transportClientPassword().getChars()))
                .build();
        try (TransportClient client = new TestXPackTransportClient(settings, LocalStateSecurity.class)) {
            Transport transport = internalCluster().getDataNodeInstance(Transport.class);
            TransportAddress transportAddress = transport.boundAddress().publishAddress();
            client.addTransportAddress(transportAddress);

            assertGreenClusterState(client);
        }
    }

    private SSLContext getSSLContext() {
        try (InputStream in =
                     Files.newInputStream(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks"))) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, "testclient".toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, "testclient".toCharArray());
            SSLContext context = SSLContext.getInstance("TLSv1.2");
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }
}
