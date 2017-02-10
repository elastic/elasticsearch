/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.xpack.common.socket.SocketAccess;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.ssl.SSLClientAuth;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.TestXPackTransportClient;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Map.Entry;

import static org.elasticsearch.test.SecuritySettingsSource.getSSLSettingsForStore;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Test authentication via PKI on both REST and Transport layers
 */
@ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, numDataNodes = 1)
public class PkiAuthenticationTests extends SecurityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        SSLClientAuth sslClientAuth = randomBoolean() ? SSLClientAuth.REQUIRED : SSLClientAuth.OPTIONAL;
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)

                .put("xpack.security.http.ssl.enabled", true)
                .put("xpack.security.http.ssl.client_authentication", sslClientAuth)
                .put("xpack.security.authc.realms.file.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.file.order", "0")
                .put("xpack.security.authc.realms.pki1.type", PkiRealm.TYPE)
                .put("xpack.security.authc.realms.pki1.order", "1")
                .put("xpack.security.authc.realms.pki1.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.authc.realms.pki1.truststore.password", "truststore-testnode-only")
                .put("xpack.security.authc.realms.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                .build();
    }

    @Override
    protected boolean useGeneratedSSLConfig() {
        return false;
    }

    public void testTransportClientCanAuthenticateViaPki() {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode");
        try (TransportClient client = createTransportClient(settings)) {
            client.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
            IndexResponse response = client.prepareIndex("foo", "bar").setSource("pki", "auth").get();
            assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
        }
    }

    /**
     * Test uses the testclient cert which is trusted by the SSL layer BUT it is not trusted by the PKI authentication
     * realm
     */
    public void testTransportClientAuthenticationFailure() {
        try (TransportClient client = createTransportClient(Settings.EMPTY)) {
            client.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
            client.prepareIndex("foo", "bar").setSource("pki", "auth").get();
            fail("transport client should not have been able to authenticate");
        } catch (NoNodeAvailableException e) {
            assertThat(e.getMessage(), containsString("None of the configured nodes are available: [{#transport#"));
        }
    }

    public void testRestAuthenticationViaPki() throws Exception {
        SSLContext context = getRestSSLContext("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks", "testnode");
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(put))) {
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("\"acknowledged\":true"));
            }
        }
    }

    public void testRestAuthenticationFailure() throws Exception {
        SSLContext context = getRestSSLContext("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks", "testclient");
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = SocketAccess.doPrivileged(() -> client.execute(put))) {
                assertThat(response.getStatusLine().getStatusCode(), is(401));
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("unable to authenticate user [Elasticsearch Test Client]"));
            }
        }
    }

    private SSLContext getRestSSLContext(String keystoreResourcePath, String password) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        Path store = getDataPath(keystoreResourcePath);
        KeyStore ks;
        try (InputStream in = Files.newInputStream(store)) {
            ks = KeyStore.getInstance("jks");
            ks.load(in, password.toCharArray());
        }

        kmf.init(ks, password.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        return context;
    }

    private TransportClient createTransportClient(Settings additionalSettings) {
        Settings clientSettings = transportClientSettings();
        if (additionalSettings.getByPrefix("xpack.ssl.").isEmpty() == false) {
            Settings.Builder builder = Settings.builder();
            for (Entry<String, String> entry : clientSettings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("xpack.ssl.") == false) {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
            clientSettings = builder.build();
        }

        Settings.Builder builder = Settings.builder().put(clientSettings)
                .put(additionalSettings)
                .put("cluster.name", internalCluster().getClusterName());
        builder.remove(Security.USER_SETTING.getKey());
        builder.remove("request.headers.Authorization");
        return new TestXPackTransportClient(builder.build());
    }

    private String getNodeUrl() {
        TransportAddress transportAddress = randomFrom(internalCluster().getInstance(HttpServerTransport.class)
                .boundAddress().boundAddresses());
        return String.format(Locale.ROOT, "https://localhost:%s/", transportAddress.address().getPort());
    }
}
