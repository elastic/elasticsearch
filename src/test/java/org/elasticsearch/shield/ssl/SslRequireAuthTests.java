/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.net.InetAddresses;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.plugin.SecurityPlugin;
import org.elasticsearch.shield.ssl.netty.NettySSLHttpServerTransportModule;
import org.elasticsearch.shield.ssl.netty.NettySSLTransportModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportModule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Locale;

import static org.hamcrest.Matchers.*;

/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1, transportClientRatio = 0.0, numClientNodes = 0)
public class SslRequireAuthTests extends ElasticsearchIntegrationTest {

    public static final HostnameVerifier HOSTNAME_VERIFIER = new HostnameVerifier() {
        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    };

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static File ipFilterFile;

    @BeforeClass
    public static void writeAllowAllIpFilterFile() throws Exception {
        ipFilterFile = temporaryFolder.newFile();
        Files.write("allow: all\n".getBytes(com.google.common.base.Charsets.UTF_8), ipFilterFile);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        File testnodeStore;
        try {
            testnodeStore = new File(getClass().getResource("/certs/simple/testnode.jks").toURI());
            assertThat(testnodeStore.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                // prevents exception until parsing has been fixed in PR
                .put("shield.authz.file.roles", "not/existing")
                // needed to ensure that netty transport is started
                .put("node.mode", "network")
                .put("shield.transport.ssl", true)
                .put("shield.transport.ssl.require.client.auth", true)
                .put("shield.transport.ssl.keystore", testnodeStore.getPath())
                .put("shield.transport.ssl.keystore_password", "testnode")
                .put("shield.transport.ssl.truststore", testnodeStore.getPath())
                .put("shield.transport.ssl.truststore_password", "testnode")
                .put("shield.http.ssl", true)
                .put("shield.http.ssl.require.client.auth", true)
                .put("shield.http.ssl.keystore", testnodeStore.getPath())
                .put("shield.http.ssl.keystore_password", "testnode")
                .put("shield.http.ssl.truststore", testnodeStore.getPath())
                .put("shield.http.ssl.truststore_password", "testnode")
                // SSL SETUP
                .put("http.type", NettySSLHttpServerTransportModule.class.getName())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySSLTransportModule.class.getName())
                .put("plugin.types", SecurityPlugin.class.getName())
                .put("shield.n2n.file", ipFilterFile.getPath());

        if (OsUtils.MAC) {
            builder.put("network.host", randomBoolean() ? "127.0.0.1" : "::1");
        }

        return builder.build();
    }


    @Test(expected = SSLHandshakeException.class)
    public void testThatRequireClientAuthRejectsWithoutCert() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        setupTrustManagers(trustAllCerts);

        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        String url = String.format(Locale.ROOT, "https://%s:%s/", InetAddresses.toUriString(inetSocketTransportAddress.address().getAddress()), inetSocketTransportAddress.address().getPort());

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.connect();
    }

    @Test
    @TestLogging("_root:DEBUG")
    public void testThatConnectionToHTTPWorks() throws Exception {
        File store = new File(getClass().getResource("/certs/simple/testnode.jks").toURI());

        KeyStore ks;
        KeyManagerFactory kmf;
        try (FileInputStream in = new FileInputStream(store)){
            // Load KeyStore
            ks = KeyStore.getInstance("jks");
            ks.load(in, "testnode".toCharArray());

            // Initialize KeyManagerFactory
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, "testnode".toCharArray());
        }

        TrustManagerFactory trustFactory;
        try (FileInputStream in = new FileInputStream(store)) {
            // Load TrustStore
            ks.load(in, "testnode".toCharArray());

            // Initialize a trust manager factory with the trusted store
            trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(ks);

            // Retrieve the trust managers from the factory
        }
        setupTrustManagers(kmf.getKeyManagers(), trustFactory.getTrustManagers());

        TransportAddress transportAddress = internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        String url = String.format(Locale.ROOT, "https://%s:%s/", InetAddresses.toUriString(inetSocketTransportAddress.address().getAddress()), inetSocketTransportAddress.address().getPort());

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.connect();

        assertThat(connection.getResponseCode(), is(200));
        String data = Streams.copyToString(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
        assertThat(data, containsString("You Know, for Search"));
    }

    private void setupTrustManagers(KeyManager[] keyManagers, TrustManager[] trustManagers) throws Exception {
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(keyManagers, trustManagers, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // totally secure
        HttpsURLConnection.setDefaultHostnameVerifier(HOSTNAME_VERIFIER);
    }

    private void setupTrustManagers(TrustManager[] trustManagers) throws Exception {
        setupTrustManagers(null, trustManagers);
    }
}
