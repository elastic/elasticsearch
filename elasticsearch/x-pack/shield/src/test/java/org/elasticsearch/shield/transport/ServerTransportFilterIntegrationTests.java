/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.shield.authc.file.FileRealm;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.shield.test.ShieldTestUtils.createFolder;
import static org.elasticsearch.shield.test.ShieldTestUtils.writeFile;
import static org.elasticsearch.test.ShieldSettingsSource.getSSLSettingsForStore;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

public class ServerTransportFilterIntegrationTests extends ShieldIntegTestCase {
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
    protected boolean autoSSLEnabled() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder();
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);

        Path store;
        try {
            store = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
            assertThat(Files.exists(store), is(true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (sslTransportEnabled()) {
            settingsBuilder.put("transport.profiles.client.shield.truststore.path", store) // settings for client truststore
                           .put("transport.profiles.client.shield.truststore.password", "testnode")
                           .put(ShieldNettyTransport.SSL_SETTING.getKey(), true);
        }

        return settingsBuilder
                .put(super.nodeSettings(nodeOrdinal))
                .put("transport.profiles.default.shield.type", "node")
                .put("transport.profiles.client.shield.type", "client")
                .put("transport.profiles.client.port", randomClientPortRange)
                // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
                .put("transport.profiles.client.bind_host", "localhost")
                .put("xpack.security.audit.enabled", false)
                .build();
    }

    public void testThatConnectionToServerTypeConnectionWorks() throws IOException {
        Settings dataNodeSettings = internalCluster().getDataNodeInstance(Settings.class);
        String systemKeyFile = InternalCryptoService.FILE_SETTING.get(dataNodeSettings);

        Transport transport = internalCluster().getDataNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        assertThat(transportAddress, instanceOf(InetSocketTransportAddress.class));
        InetSocketAddress inetSocketAddress = ((InetSocketTransportAddress) transportAddress).address();
        String unicastHost = NetworkAddress.format(inetSocketAddress);

        // test that starting up a node works
        Settings nodeSettings = Settings.builder()
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                .put("node.mode", "network")
                .put("node.name", "my-test-node")
                .put("network.host", "localhost")
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.unicast.hosts", unicastHost)
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), sslTransportEnabled())
                .put("xpack.security.audit.enabled", false)
                .put("path.home", createTempDir())
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(InternalCryptoService.FILE_SETTING.getKey(), systemKeyFile)
                .build();
        try (Node node = new MockNode(nodeSettings, Version.CURRENT, Collections.singletonList(XPackPlugin.class))) {
            node.start();
            assertGreenClusterState(node.client());
        }
    }

    public void testThatConnectionToClientTypeConnectionIsRejected() throws IOException {
        Settings dataNodeSettings = internalCluster().getDataNodeInstance(Settings.class);
        String systemKeyFile = InternalCryptoService.FILE_SETTING.get(dataNodeSettings);

        Path folder = createFolder(createTempDir(), getClass().getSimpleName() + "-" + randomAsciiOfLength(10));

        // test that starting up a node works
        Settings nodeSettings = Settings.builder()
                .put("xpack.security.authc.realms.file.type", FileRealm.TYPE)
                .put("xpack.security.authc.realms.file.order", 0)
                .put("xpack.security.authc.realms.file.files.users", writeFile(folder, "users", configUsers()))
                .put("xpack.security.authc.realms.file.files.users_roles", writeFile(folder, "users_roles", configUsersRoles()))
                .put(FileRolesStore.ROLES_FILE_SETTING.getKey(), writeFile(folder, "roles.yml", configRoles()))
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks", "testnode"))
                .put("node.mode", "network")
                .put("node.name", "my-test-node")
                .put(Security.USER_SETTING.getKey(), "test_user:changeme")
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.unicast.hosts", "localhost:" + randomClientPort)
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), sslTransportEnabled())
                .put("xpack.security.audit.enabled", false)
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(InternalCryptoService.FILE_SETTING.getKey(), systemKeyFile)
                .put("discovery.initial_state_timeout", "2s")
                .put("path.home", createTempDir())
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .build();
        try (Node node = new MockNode(nodeSettings, Version.CURRENT, Collections.singletonList(XPackPlugin.class))) {
            node.start();

            // assert that node is not connected by waiting for the timeout
            try {
                // updating cluster settings requires a master. since the node should not be able to
                // connect to the cluster, there should be no master, and therefore this
                // operation should fail. we can't use cluster health/stats here to and
                // wait for a timeout, because as long as the node is not connected to the cluster
                // the license is disabled and therefore blocking health & stats calls.
                node.client().admin().cluster().prepareUpdateSettings()
                        .setTransientSettings(singletonMap("key", "value"))
                        .setMasterNodeTimeout(TimeValue.timeValueSeconds(2))
                        .get();
                fail("Expected to fail update settings as the node should not be able to connect to the cluster, cause there should be no" +
                        " master");
            } catch (MasterNotDiscoveredException e) {
                // expected
                logger.error("expected exception", e);
            }
        }
    }

}
