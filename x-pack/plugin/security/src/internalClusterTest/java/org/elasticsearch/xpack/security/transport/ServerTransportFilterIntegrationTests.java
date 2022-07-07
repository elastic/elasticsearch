/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.TransportNodesListGatewayMetaState;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
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
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
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
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settingsBuilder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        String randomClientPortRange = randomClientPort + "-" + (randomClientPort + 100);
        addSSLSettingsForNodePEMFiles(settingsBuilder, "transport.profiles.client.xpack.security.", true);
        Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        settingsBuilder.putList(
            "transport.profiles.client.xpack.security.ssl.certificate_authorities",
            Collections.singletonList(certPath.toString())
        ) // settings for client truststore
            .put("transport.profiles.client.xpack.security.type", "client")
            .put("transport.profiles.client.port", randomClientPortRange)
            // make sure this is "localhost", no matter if ipv4 or ipv6, but be consistent
            .put("transport.profiles.client.bind_host", "localhost")
            .put("xpack.security.audit.enabled", false)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        if (randomBoolean()) {
            settingsBuilder.put("transport.profiles.default.xpack.security.type", "node"); // this is default lets set it randomly
        }

        return settingsBuilder.build();
    }

    public void testThatConnectionToServerTypeConnectionWorks() throws IOException, NodeValidationException {
        Path home = createTempDir();
        Path xpackConf = home.resolve("config");
        Files.createDirectories(xpackConf);

        Transport transport = internalCluster().getAnyMasterNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.boundAddress().publishAddress();
        String unicastHost = NetworkAddress.format(transportAddress.address());

        // test that starting up a node works
        Settings.Builder nodeSettings = getSettingsBuilder().put("node.name", "my-test-node")
            .put("network.host", "localhost")
            .put("cluster.name", internalCluster().getClusterName())
            .put(DISCOVERY_SEED_HOSTS_SETTING.getKey(), unicastHost)
            .put("xpack.security.enabled", true)
            .put("xpack.security.audit.enabled", false)
            .put("xpack.security.transport.ssl.enabled", true)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
            .put("path.home", home)
            .put(nonMasterNode());
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(LocalStateSecurity.class, MockHttpTransport.TestPlugin.class);
        addSSLSettingsForPEMFiles(
            nodeSettings,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList(
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"
            )
        );
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

        Transport transport = internalCluster().getAnyMasterNodeInstance(Transport.class);
        TransportAddress transportAddress = transport.profileBoundAddresses().get("client").publishAddress();
        String unicastHost = NetworkAddress.format(transportAddress.address());

        // test that starting up a node works
        Settings.Builder nodeSettings = getSettingsBuilder().put("xpack.security.authc.realms.file.file.order", 0)
            .put("node.name", "my-test-node")
            .put(SecurityField.USER_SETTING.getKey(), "test_user:" + SecuritySettingsSourceField.TEST_PASSWORD)
            .put("cluster.name", internalCluster().getClusterName())
            .put(DISCOVERY_SEED_HOSTS_SETTING.getKey(), unicastHost)
            .put("xpack.security.enabled", true)
            .put("xpack.security.audit.enabled", false)
            .put("xpack.security.transport.ssl.enabled", true)
            .put(XPackSettings.WATCHER_ENABLED.getKey(), false)
            .put("discovery.initial_state_timeout", "0s")
            .put("path.home", home)
            .put(nonMasterNode());
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(LocalStateSecurity.class, MockHttpTransport.TestPlugin.class);
        addSSLSettingsForPEMFiles(
            nodeSettings,
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
            "testnode",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
            Arrays.asList(
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_ec.crt"
            )
        );
        try (Node node = new MockNode(nodeSettings.build(), mockPlugins)) {
            node.start();
            TransportService instance = node.injector().getInstance(TransportService.class);
            try (
                Transport.Connection connection = instance.openConnection(
                    new DiscoveryNode("theNode", transportAddress, Version.CURRENT),
                    ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG)
                )
            ) {
                // handshake should be ok
                final DiscoveryNode handshake = PlainActionFuture.get(
                    fut -> instance.handshake(connection, TimeValue.timeValueSeconds(10), fut)
                );
                assertEquals(transport.boundAddress().publishAddress(), handshake.getAddress());
                CountDownLatch latch = new CountDownLatch(1);
                instance.sendRequest(
                    connection,
                    TransportNodesListGatewayMetaState.ACTION_NAME,
                    new TransportNodesListGatewayMetaState.Request("foo", "bar", "baz"),
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
                                assertThat(
                                    exp.getCause().getMessage(),
                                    equalTo("executing internal/shard actions is considered malicious and forbidden")
                                );
                            } finally {
                                latch.countDown();
                            }
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
                );
                latch.await();
            }
        }
    }

    private Settings.Builder getSettingsBuilder() {
        Settings.Builder builder = Settings.builder();
        if (inFipsJvm()) {
            builder.put(XPackSettings.DIAGNOSE_TRUST_EXCEPTIONS_SETTING.getKey(), false);
        }
        return builder;
    }

}
