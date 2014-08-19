/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.net.InetAddresses;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.os.OsUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.shield.n2n.N2NPlugin;
import org.elasticsearch.shield.transport.netty.NettySecuredHttpServerTransportModule;
import org.elasticsearch.shield.transport.netty.NettySecuredTransportModule;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, transportClientRatio = 0.0, numClientNodes = 0)
public class SslIntegrationTests extends ElasticsearchIntegrationTest {

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
            testnodeStore = new File(getClass().getResource("certs/simple/testnode.jks").toURI());
            assertThat(testnodeStore.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                //
                .put("shield.authz.file.roles", "not/existing")
                // needed to ensure that netty transport is started
                .put("node.mode", "network")
                .put("shield.transport.ssl", true)
                .put("shield.transport.ssl.keystore", testnodeStore.getPath())
                .put("shield.transport.ssl.keystore_password", "testnode")
                .put("shield.transport.ssl.truststore", testnodeStore.getPath())
                .put("shield.transport.ssl.truststore_password", "testnode")
                .put("shield.http.ssl", true)
                .put("shield.http.ssl.require.client.auth", false)
                .put("shield.http.ssl.keystore", testnodeStore.getPath())
                .put("shield.http.ssl.keystore_password", "testnode")
                .put("shield.http.ssl.truststore", testnodeStore.getPath())
                .put("shield.http.ssl.truststore_password", "testnode")
                // SSL SETUP
                .put("http.type", NettySecuredHttpServerTransportModule.class.getName())
                .put("plugin.types", N2NPlugin.class.getName())
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransportModule.class.getName())
                .put("shield.n2n.file", ipFilterFile.getPath());

        if (OsUtils.MAC) {
            builder.put("network.host", randomBoolean() ? "127.0.0.1" : "::1");
        }
        return builder.build();
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
        Settings customSettings = getSettings("transport_client").put("shield.transport.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();

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
        Settings customSettings = getSettings("ssl_node").put("shield.transport.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();
        NodeBuilder.nodeBuilder().settings(customSettings).node().start();
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testConnectNodeClientFailsWithWrongCipher() throws Exception {
        Settings customSettings = getSettings("ssl_node").put("node.client", true).put("shield.transport.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"}).build();
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
            testClientKeyStore = new File(getClass().getResource("certs/simple/testclient.jks").toURI());
            testClientTrustStore = new File(getClass().getResource("certs/simple/testclient.jks").toURI());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(testClientKeyStore.exists(), is(true));
        assertThat(testClientTrustStore.exists(), is(true));

        return ImmutableSettings.settingsBuilder()
                .put("node.name", name)
                .put("plugins.load_classpath_plugins", false)
                .put("shield.transport.ssl", true)
                .put("shield.transport.ssl.keystore", testClientKeyStore.getPath())
                .put("shield.transport.ssl.keystore_password", "testclient")
                .put("shield.transport.ssl.truststore", testClientTrustStore .getPath())
                .put("shield.transport.ssl.truststore_password", "testclient")
                .put("discovery.zen.ping.multicast.ping.enabled", false)
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransportModule.class.getName())
                .put("shield.n2n.file", ipFilterFile.getPath())
                .put("cluster.name", internalCluster().getClusterName());
    }

    private void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }

}
