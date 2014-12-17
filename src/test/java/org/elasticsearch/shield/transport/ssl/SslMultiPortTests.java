/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.ssl;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.CoreMatchers.is;

@ClusterScope(scope = Scope.SUITE)
public class SslMultiPortTests extends ShieldIntegrationTest {

    private static int randomClientPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);

        File store;
        try {
            store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks").toURI());
            assertThat(store.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                // client set up here
                .put("transport.profiles.client.port", randomClientPortRange)
                .put("transport.profiles.client.bind_host", "localhost") // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.shield.truststore.path", store.getAbsolutePath()) // settings for client truststore
                .put("transport.profiles.client.shield.truststore.password", "testnode-client-profile")
                .build();
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    private TransportClient createTransportClient(Settings additionalSettings) {
        Settings settings = ImmutableSettings.builder().put(transportClientSettings())
                .put("name", "programmatic_transport_client")
                .put("cluster.name", internalCluster().getClusterName())
                .put(additionalSettings)
                .build();
        return new TransportClient(settings, false);
    }

    @Test
    public void testThatStandardTransportClientCanConnectToDefaultProfile() throws Exception {
        assertGreenClusterState(internalCluster().transportClient());
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatStandardTransportClientCannotConnectToClientProfile() throws Exception {
        try(TransportClient transportClient = createTransportClient(ImmutableSettings.EMPTY)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getClientProfilePort()));
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    @Test
    public void testThatProfileTransportClientCanConnectToClientProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", getClientProfilePort()));
            assertGreenClusterState(transportClient);
        }
    }

    @Test(expected = NoNodeAvailableException.class)
    public void testThatProfileTransportClientCannotConnectToDefaultProfile() throws Exception {
        Settings settings = ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient-client-profile.jks", "testclient-client-profile");
        try (TransportClient transportClient = createTransportClient(settings)) {
            TransportAddress transportAddress = internalCluster().getInstance(Transport.class).boundAddress().boundAddress();
            transportClient.addTransportAddress(transportAddress);
            transportClient.admin().cluster().prepareHealth().get();
        }
    }

    /*
     * Gets the actual port that the client profile in this test environment is listening on as the randomClientPort
     * may actually be bound by some other node
     */
    private int getClientProfilePort() throws Exception {
        NettyTransport transport = (NettyTransport) internalCluster().getInstance(Transport.class);
        Field channels = NettyTransport.class.getDeclaredField("serverChannels");
        channels.setAccessible(true);
        Map<String, Channel> serverChannels = (Map<String, Channel>) channels.get(transport);
        Channel clientProfileChannel = serverChannels.get("client");
        return ((InetSocketAddress) clientProfileChannel.getLocalAddress()).getPort();
    }
}
