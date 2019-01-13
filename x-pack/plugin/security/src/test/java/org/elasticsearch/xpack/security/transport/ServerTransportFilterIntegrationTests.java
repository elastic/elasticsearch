/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForPEMFiles;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ServerTransportFilterIntegrationTests extends SecurityIntegTestCase {
    private static int randomClientPort;

    @BeforeClass
    public static void getRandomPort() {
        randomClientPort = randomIntBetween(49000, 65500); // ephemeral port
    }

    @Override
    public boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder();
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort+100);
        Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        settingsBuilder.put(super.nodeSettings(nodeOrdinal))
            .putList("transport.profiles.client.xpack.security.ssl.certificate_authorities",
                Arrays.asList(certPath.toString())) // settings for client truststore
            .put("xpack.ssl.client_authentication", SSLClientAuth.REQUIRED)
            .put("transport.profiles.client.xpack.security.type", "client")
            .put("transport.profiles.client.port", randomClientPortRange)
            // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
            .put("transport.profiles.client.bind_host", "localhost")
            .put("xpack.security.audit.enabled", false)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false);
        if (randomBoolean()) {
            settingsBuilder.put("transport.profiles.default.xpack.security.type", "node"); // this is default lets set it randomly
        }

        SecuritySettingsSource.addSecureSettings(settingsBuilder, secureSettings ->
            secureSettings.setString("transport.profiles.client.xpack.security.ssl.truststore.secure_password", "testnode"));
        return settingsBuilder.build();
    }

    public void testThatConnectionToServerTypeConnectionWorks() throws IOException, NodeValidationException {
        Path home = createTempDir();
        Path xpackConf = home.resolve("config");
        Files.createDirectories(xpackConf);

        Transport transport = internalCluster().getMasterNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        String unicastHost = NetworkAddress.format(transportAddress.address());

        // test that starting up a node works
        Settings.Builder nodeSettings = Settings.builder()
                .put("node.name", "my-test-node")
                .put("network.host", "localhost")
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.unicast.hosts", unicastHost)
                .put("discovery.zen.minimum_master_nodes",
                        internalCluster().getInstance(Settings.class).get("discovery.zen.minimum_master_nodes"))
                .put("xpack.security.enabled", true)
                .put("xpack.security.audit.enabled", false)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put("path.home", home)
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(TestZenDiscovery.USE_ZEN2.getKey(), getUseZen2())
                .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false);
                //.put("xpack.ml.autodetect_process", false);
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(
            LocalStateSecurity.class, TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class);
        addSSLSettingsForPEMFiles(
            nodeSettings,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        try (Node node = new MockNode(nodeSettings.build(), mockPlugins)) {
            node.start();
            ensureStableCluster(cluster().size() + 1);
        }
    }

    public void testThatConnectionToClientTypeConnectionIsRejected() throws IOException, NodeValidationException, InterruptedException {
        Path home = createTempDir();
        Path xpackConf = home.resolve("config");
        Files.createDirectories(xpackConf);
        writeFile(xpackConf, "users", configUsers());
        writeFile(xpackConf, "users_roles", configUsersRoles());
        writeFile(xpackConf, "roles.yml", configRoles());

        Transport transport = internalCluster().getMasterNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.profileBoundAddresses().get("client").publishAddress();
        String unicastHost = NetworkAddress.format(transportAddress.address());

        // test that starting up a node works
        Settings.Builder nodeSettings = Settings.builder()
                .put("xpack.security.authc.realms.file.file.order", 0)
                .put("node.name", "my-test-node")
                .put(SecurityField.USER_SETTING.getKey(), "test_user:" + SecuritySettingsSourceField.TEST_PASSWORD)
                .put("cluster.name", internalCluster().getClusterName())
                .put("discovery.zen.ping.unicast.hosts", unicastHost)
                .put("discovery.zen.minimum_master_nodes",
                        internalCluster().getInstance(Settings.class).get("discovery.zen.minimum_master_nodes"))
                .put("xpack.security.enabled", true)
                .put("xpack.security.audit.enabled", false)
                .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put("discovery.initial_state_timeout", "0s")
                .put("path.home", home)
                .put(Node.NODE_MASTER_SETTING.getKey(), false)
                .put(TestZenDiscovery.USE_ZEN2.getKey(), getUseZen2())
                .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false);
                //.put("xpack.ml.autodetect_process", false);
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(
            LocalStateSecurity.class, TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class);
        addSSLSettingsForPEMFiles(
            nodeSettings,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        try (Node node = new MockNode(nodeSettings.build(), mockPlugins)) {
            node.start();
            TransportService instance = node.injector().getInstance(TransportService.class);
            try (Transport.Connection connection = instance.openConnection(new DiscoveryNode("theNode", transportAddress, Version.CURRENT),
                    ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG))) {
                // handshake should be ok
                final DiscoveryNode handshake = instance.handshake(connection, 10000);
                assertEquals(transport.boundAddress().publishAddress(), handshake.getAddress());
                CountDownLatch latch = new CountDownLatch(1);
                instance.sendRequest(connection, NodeMappingRefreshAction.ACTION_NAME,
                        new NodeMappingRefreshAction.NodeMappingRefreshRequest("foo", "bar", "baz"),
                        TransportRequestOptions.EMPTY,
                        new TransportResponseHandler<TransportResponse>() {
                    @Override
                    public TransportResponse read(StreamInput in) {
                        try {
                            fail("never get that far");
                        } finally {
                            latch.countDown();
                        }
                        return null;
                    }

                    @Override
                    public void handleResponse(TransportResponse response) {
                        try {
                            fail("never get that far");
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        try {
                            assertThat(exp.getCause(), instanceOf(ElasticsearchSecurityException.class));
                            assertThat(exp.getCause().getMessage(),
                                    equalTo("executing internal/shard actions is considered malicious and forbidden"));
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
                latch.await();
            }
        }
    }

}
