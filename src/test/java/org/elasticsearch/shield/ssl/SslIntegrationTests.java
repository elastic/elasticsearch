/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import com.google.common.net.InetAddresses;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.shield.plugin.SecurityPlugin;
import org.elasticsearch.shield.ssl.netty.NettySSLHttpServerTransportModule;
import org.elasticsearch.shield.ssl.netty.NettySSLTransportModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.*;

/**
 * Created a testnode cert and a test client cert, which is imported into the keystore
 *
 * keytool -genkeypair -alias testnode -keystore testnode.jks -keyalg RSA -storepass testnode -keypass testnode -dname "cn=Elasticsearch Test Node, ou=elasticsearch, o=org"
 * keytool -export -alias testnode -keystore testnode.jks -rfc -file testnode.cert -storepass testnode
 *
 * keytool -genkeypair -alias testclient -keystore testclient.jks -keyalg RSA -storepass testclient -keypass testclient -dname "cn=Elasticsearch Test Client, ou=elasticsearch, o=org"
 * keytool -export -alias testclient -keystore testclient.jks -rfc -file testclient.cert -storepass testclient
 *
 * keytool -import -trustcacerts -alias testclient -file testclient.cert -keystore testnode.jks -storepass testnode -noprompt
 * keytool -import -trustcacerts -alias testnode -file testnode.cert -keystore testclient.jks -storepass testclient -noprompt
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, transportClientRatio = 0.0, numClientNodes = 0)
public class SslIntegrationTests extends ElasticsearchIntegrationTest {

    /*
    # transport.tcp.ssl.keystore: /path/to/the/keystore
    # transport.tcp.ssl.keystore_password: password
    # transport.tcp.ssl.keystore_algorithm: SunX509
    #
    # transport.tcp.ssl.truststore: /path/to/the/truststore
    # transport.tcp.ssl.truststore_password: password
    # transport.tcp.ssl.truststore_algorithm: PKIX
         */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        File testnodeStore;
        try {
            testnodeStore = new File(getClass().getResource("/certs/simple/testnode.jks").toURI());
            assertThat(testnodeStore.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                // needed to ensure that netty transport is started
                .put("node.mode", "network")
                .put("transport.tcp.ssl", true)
                .put("transport.tcp.ssl.keystore", testnodeStore.getPath())
                .put("transport.tcp.ssl.keystore_password", "testnode")
                .put("transport.tcp.ssl.truststore", testnodeStore.getPath())
                .put("transport.tcp.ssl.truststore_password", "testnode")
                .put("http.ssl", true)
                .put("http.ssl.keystore", testnodeStore.getPath())
                .put("http.ssl.keystore_password", "testnode")
                .put("http.ssl.truststore", testnodeStore.getPath())
                .put("http.ssl.truststore_password", "testnode")
                // SSL SETUP
                .put("http.type", NettySSLHttpServerTransportModule.class.getName())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySSLTransportModule.class.getName())
                .put("plugin.types", SecurityPlugin.class.getName())
                .build();
    }

    @Before
    public void setup() {
        System.setProperty("javax.net.debug", "all");
    }

    @After
    public void teardown() {
        System.clearProperty("javax.net.debug");
    }

    @Test
    @TestLogging("_root:INFO,org.elasticsearch.test:TRACE, org.elasticsearch.client.transport:DEBUG,org.elasticsearch.shield:TRACE")
    public void testThatTransportClientCanConnectToNodeViaSsl() throws Exception {
        TransportClient transportClient = new TransportClient(getSettings("transport_client").build(), false);
        TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
        transportClient.addTransportAddress(transportAddress);

        assertGreenClusterState(transportClient);
    }

    @Test(expected = ElasticsearchSSLException.class)
    @TestLogging("_root:INFO,org.elasticsearch.client.transport:DEBUG")
    public void testThatUnconfiguredCipchersAreRejected() {
        // some randomly taken ciphers from SSLContext.getDefault().getSocketFactory().getSupportedCipherSuites()
        // could be really randomized
        Settings customSettings = getSettings("transport_client").put("transport.tcp.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();

        TransportClient transportClient = new TransportClient(customSettings);

        TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
        transportClient.addTransportAddress(transportAddress);

        transportClient.admin().cluster().prepareHealth().get();
    }

    @Test
    public void testConnectNodeWorks() throws Exception {
        try (Node node = NodeBuilder.nodeBuilder().settings(getSettings("ssl_node")).node().start()) {
            try (Client client = node.client()) {
                assertGreenClusterState(client);
            }
        }
    }

    @Test
    public void testConnectNodeClientWorks() throws Exception {
        // no multicast, good old discovery
        TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
        assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        Settings.Builder settingsBuilder = getSettings("node_client")
                .put("node.client", true)
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", inetSocketTransportAddress.address().getHostString() + ":" + inetSocketTransportAddress.address().getPort());

        try (Node node = NodeBuilder.nodeBuilder().settings(settingsBuilder).node().start()) {
            try (Client client = node.client()) {
                assertGreenClusterState(client);
            }
        }
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testConnectNodeFailsWithWrongCipher() throws Exception {
        Settings customSettings = getSettings("ssl_node").put("transport.tcp.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();
        NodeBuilder.nodeBuilder().settings(customSettings).node().start();
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testConnectNodeClientFailsWithWrongCipher() throws Exception {
        Settings customSettings = getSettings("ssl_node").put("node.client", true).put("transport.tcp.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();
        NodeBuilder.nodeBuilder().settings(customSettings).node().start();
    }

    @Test
    public void testThatConnectionToHTTPWorks() throws Exception {
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

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        // totally secure
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });

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

    private ImmutableSettings.Builder getSettings(String name) {
        File testClientKeyStore;
        File testClientTrustStore;
        try {
            testClientKeyStore = new File(getClass().getResource("/certs/simple/testclient.jks").toURI());
            testClientTrustStore = new File(getClass().getResource("/certs/simple/testclient.jks").toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(testClientKeyStore.exists(), is(true));
        assertThat(testClientTrustStore.exists(), is(true));

        return ImmutableSettings.settingsBuilder()
                .put("node.name", name)
                .put("transport.tcp.ssl", true)
                .put("transport.tcp.ssl.keystore", testClientKeyStore.getPath())
                .put("transport.tcp.ssl.keystore_password", "testclient")
                .put("transport.tcp.ssl.truststore", testClientTrustStore .getPath())
                .put("transport.tcp.ssl.truststore_password", "testclient")
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySSLTransportModule.class.getName())
                //.put("plugin.types", SecurityPlugin.class.getName())
                .put("cluster.name", internalCluster().getClusterName());
    }

    private void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }
}
