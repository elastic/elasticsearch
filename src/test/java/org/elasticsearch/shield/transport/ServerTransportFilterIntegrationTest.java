/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.shield.signature.InternalSignatureService;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

@ClusterScope(scope = Scope.SUITE)
public class ServerTransportFilterIntegrationTest extends ShieldIntegrationTest {

    private static int randomClientPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
    }

    @Override
    protected boolean sslTransportEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder settingsBuilder = settingsBuilder();
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);

        File store;
        try {
            store = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
            assertThat(store.exists(), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (sslTransportEnabled()) {
            settingsBuilder.put("transport.profiles.client.shield.truststore.path", store.getAbsolutePath()) // settings for client truststore
                           .put("transport.profiles.client.shield.truststore.password", "testnode")
                           .put("shield.transport.ssl", true);
        }

        return settingsBuilder
                .put(super.nodeSettings(nodeOrdinal))
                .put("transport.profiles.default.shield.type", "node")
                .put("transport.profiles.client.shield.type", "client")
                .put("transport.profiles.client.port", randomClientPortRange)
                .put("transport.profiles.client.bind_host", "localhost") // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("shield.audit.enabled", false)
                .build();
    }

    @Test
    public void testThatConnectionToServerTypeConnectionWorks() {
        Settings dataNodeSettings = internalCluster().getDataNodeInstance(Settings.class);
        String systemKeyFile = dataNodeSettings.get(InternalSignatureService.FILE_SETTING);

        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        assertThat(transportAddress, instanceOf(InetSocketTransportAddress.class));
        InetSocketAddress inetSocketAddress = ((InetSocketTransportAddress) transportAddress).address();
        String unicastHost = inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort();

        // test that starting up a node works
        Settings nodeSettings = settingsBuilder()
                .put(ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                .put("node.mode", "network")
                .put("node.name", "my-test-node")
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", unicastHost)
                .put("shield.transport.ssl", sslTransportEnabled())
                .put("shield.audit.enabled", false)
                .put(InternalNode.HTTP_ENABLED, false)
                .put(InternalSignatureService.FILE_SETTING, systemKeyFile)
                .build();
        try (Node node = nodeBuilder().client(true).settings(nodeSettings).node()) {
            assertGreenClusterState(node.client());
        }
    }

    @Test
    public void testThatConnectionToClientTypeConnectionIsRejected() {
        Settings dataNodeSettings = internalCluster().getDataNodeInstance(Settings.class);
        String systemKeyFile = dataNodeSettings.get(InternalSignatureService.FILE_SETTING);

        // test that starting up a node works
        Settings nodeSettings = settingsBuilder()
                .put(ShieldSettingsSource.getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                .put("node.mode", "network")
                .put("node.name", "my-test-node")
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("discovery.zen.ping.unicast.hosts", "localhost:" + randomClientPort)
                .put("shield.transport.ssl", sslTransportEnabled())
                .put("shield.audit.enabled", false)
                .put(InternalNode.HTTP_ENABLED, false)
                .put(InternalSignatureService.FILE_SETTING, systemKeyFile)
                .put("discovery.initial_state_timeout", "2s")
                .build();
        try (Node node = nodeBuilder().client(true).settings(nodeSettings).build()) {
            node.start();

            // assert that node is not connected by waiting for the timeout
            try {
                node.client().admin().cluster().prepareHealth().get("1s");
                fail("Expected timeout exception due to node unable to connect");
            } catch (ElasticsearchTimeoutException e) {}
        }
    }
}
