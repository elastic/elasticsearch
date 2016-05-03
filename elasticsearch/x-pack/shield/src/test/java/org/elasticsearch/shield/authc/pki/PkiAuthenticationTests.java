/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.netty.ShieldNettyHttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;

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

import static org.elasticsearch.test.ShieldSettingsSource.getSSLSettingsForStore;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Test authentication via PKI on both REST and Transport layers
 */
@ClusterScope(numClientNodes = 0, numDataNodes = 1)
public class PkiAuthenticationTests extends ShieldIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        SSLClientAuth sslClientAuth = randomBoolean() ? SSLClientAuth.REQUIRED : SSLClientAuth.OPTIONAL;
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)

                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), sslClientAuth)
                .put("xpack.security.authc.realms.file.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.file.order", "0")
                .put("xpack.security.authc.realms.pki1.type", PkiRealm.TYPE)
                .put("xpack.security.authc.realms.pki1.order", "1")
                .put("xpack.security.authc.realms.pki1.truststore.path",
                        getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                .put("xpack.security.authc.realms.pki1.truststore.password", "truststore-testnode-only")
                .put("xpack.security.authc.realms.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected boolean autoSSLEnabled() {
        return false;
    }

    public void testTransportClientCanAuthenticateViaPki() {
        Settings settings = getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode");
        try (TransportClient client = createTransportClient(settings)) {
            client.addTransportAddress(randomFrom(internalCluster().getInstance(Transport.class).boundAddress().boundAddresses()));
            IndexResponse response = client.prepareIndex("foo", "bar").setSource("pki", "auth").get();
            assertThat(response.isCreated(), is(true));
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
        SSLContext context = getRestSSLContext("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode");
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = client.execute(put)) {
                String body = EntityUtils.toString(response.getEntity());
                assertThat(body, containsString("\"acknowledged\":true"));
            }
        }
    }

    public void testRestAuthenticationFailure() throws Exception {
        SSLContext context = getRestSSLContext("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient");
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(context).build()) {
            HttpPut put = new HttpPut(getNodeUrl() + "foo");
            try (CloseableHttpResponse response = client.execute(put)) {
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
        if (additionalSettings.getByPrefix("xpack.security.ssl.").isEmpty() == false) {
            Settings.Builder builder = Settings.builder();
            for (Entry<String, String> entry : clientSettings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("xpack.security.ssl.") == false) {
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
        return TransportClient.builder().settings(builder).addPlugin(XPackPlugin.class).build();
    }

    private String getNodeUrl() {
        TransportAddress transportAddress = randomFrom(internalCluster().getInstance(HttpServerTransport.class)
                .boundAddress().boundAddresses());
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        return String.format(Locale.ROOT, "https://localhost:%s/", inetSocketTransportAddress.address().getPort());
    }
}
