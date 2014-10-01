/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import com.google.common.base.Charsets;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportModule;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.*;

public class SslIntegrationTests extends ShieldIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true).build();
    }

    @Test
    public void testThatInternallyCreatedTransportClientCanConnect() throws Exception {
        Client transportClient = internalCluster().transportClient();
        assertGreenClusterState(transportClient);
    }

    @Test
    public void testThatProgrammaticallyCreatedTransportClientCanConnect() throws Exception {
        Settings settings = settingsBuilder()
                .put(transportClientSettings())
                .put("name", "programmatic_transport_client_")
                .put("cluster.name", internalCluster().getClusterName())
                .build();

        try (TransportClient transportClient = new TransportClient(settings, false)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            assertGreenClusterState(transportClient);
        }

        try (TransportClient transportClient = new TransportClient(settings, true)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            assertGreenClusterState(transportClient);
        }
    }

    // no SSL exception as this is the exception is returned when connecting
    @Test(expected = NoNodeAvailableException.class)
    public void testThatUnconfiguredCipchersAreRejected() {
        TransportClient transportClient = new TransportClient(settingsBuilder()
                .put(transportClientSettings())
                .put("name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .putArray("shield.transport.ssl.ciphers", new String[]{"TLS_ECDH_anon_WITH_RC4_128_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"})
                .build());

        TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
        transportClient.addTransportAddress(transportAddress);

        transportClient.admin().cluster().prepareHealth().get();
    }

    @Test
    public void testConnectNodeWorks() throws Exception {
        Settings settings = settingsBuilder()
                .put("name", "programmatic_node")
                .put("cluster.name", internalCluster().getClusterName())

                .put("request.headers.Authorization", basicAuthHeaderValue(getClientUsername(), getClientPassword()))
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransport.class.getName())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)

                .put(getSSLSettingsForStore("certs/simple/testclient.jks", "testclient"))
                .build();

        try (Node node = NodeBuilder.nodeBuilder().settings(settings).node()) {
            try (Client client = node.client()) {
                assertGreenClusterState(client);
            }
        }
    }

    @Test
    public void testConnectNodeClientWorks() throws Exception {
        Settings settings = settingsBuilder()
                .put("name", "programmatic_node_client")
                .put("cluster.name", internalCluster().getClusterName())
                .put("node.mode", "network")

                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.type", "zen")
                .putArray("discovery.zen.ping.unicast.hosts", getUnicastHostAddress())

                .put("request.headers.Authorization", basicAuthHeaderValue(getClientUsername(), getClientPassword()))
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettySecuredTransport.class.getName())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .put("shield.transport.n2n.ip_filter.file", writeFile(newFolder(), "ip_filter.yml", ShieldIntegrationTest.CONFIG_IPFILTER_ALLOW_ALL))

                .put(getSSLSettingsForStore("certs/simple/testclient.jks", "testclient"))
                .build();

        try (Node node = NodeBuilder.nodeBuilder().settings(settings).client(true).node()) {
            try (Client client = node.client()) {
                assertGreenClusterState(client);
            }
        }
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
        connection.setRequestProperty("Authorization", UsernamePasswordToken.basicAuthHeaderValue(getClientUsername(), getClientPassword()));
        connection.connect();

        assertThat(connection.getResponseCode(), is(200));
        String data = Streams.copyToString(new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
        assertThat(data, containsString("You Know, for Search"));
    }

    private void assertGreenClusterState(Client client) {
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().get();
        assertNoTimeout(clusterHealthResponse);
        assertThat(clusterHealthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
    }
}
