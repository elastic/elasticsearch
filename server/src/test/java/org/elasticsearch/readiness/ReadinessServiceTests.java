/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.elasticsearch.Version;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.spy;

public class ReadinessServiceTests extends ESTestCase {
    private ClusterService clusterService;
    private ReadinessService readinessService;
    private ThreadPool threadpool;
    private Environment env;
    private FakeHttpTransport httpTransport;

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        final DiscoveryNode node;

        FakeHttpTransport() {
            node = new DiscoveryNode(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE),
                Version.CURRENT
            );
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}

        @Override
        public BoundTransportAddress boundAddress() {
            return new BoundTransportAddress(new TransportAddress[] { node.getAddress() }, node.getAddress());
        }

        @Override
        public HttpInfo info() {
            return null;
        }

        @Override
        public HttpStats stats() {
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("readiness_service_tests");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool
        );
        Settings settings = Settings.builder().put(ReadinessService.ENABLED_SETTING.getKey(), true).build();
        env = newEnvironment(settings);

        httpTransport = new FakeHttpTransport();
        readinessService = new ReadinessService(clusterService, env, httpTransport);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testBasicSetup() {
        assertTrue(readinessService.enabled());
        Path socketPath = readinessService.getSocketPath();
        assertTrue(socketPath.startsWith(env.logsFile()));
        assertTrue(socketPath.toString().endsWith(ReadinessService.SOCKET_NAME));
    }

    @SuppressForbidden(reason = "Must use Default System FS")
    public void testSocketChannelCreation() throws Exception {
        // Unix domain sockets need real file system, we can't use the Lucene Filter FS
        Path truePath = Files.createTempFile("readiness", ".socket");
        try (ServerSocketChannel channel = readinessService.setupUnixDomainSocket(truePath)) {
            assertTrue(channel.isOpen());
            assertTrue(Files.exists(truePath));
        }
    }

    @SuppressForbidden(reason = "Must use Default System FS")
    public void testStartStop() throws Exception {
        ReadinessService spied = spy(readinessService);
        Path tempPath = Files.createTempFile("readiness", ".socket");
        Mockito.doReturn(tempPath).when(spied).getSocketPath();
        spied.start();
        assertNotNull(spied.serverChannel());
        spied.stop();
        assertTrue(Files.exists(tempPath));
        assertFalse(spied.ready());
        spied.close();
        assertFalse(Files.exists(tempPath));
    }

    @SuppressForbidden(reason = "Intentional socket open")
    public void testSendStatus() throws Exception {
        ReadinessService spied = spy(readinessService);
        Path tempPath = Files.createTempFile("readiness", ".socket");
        Mockito.doReturn(tempPath).when(spied).getSocketPath();
        spied.start();

        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(tempPath);

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    channel.connect(socketAddress);
                    BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
                    String message = reader.readLine();
                    assertNotNull(message);
                    assertEquals("false," + httpTransport.boundAddress().publishAddress().getPort(), message);

                    ClusterState previousState = ClusterState.builder(new ClusterName("cluster"))
                        .nodes(
                            DiscoveryNodes.builder()
                                .add(
                                    DiscoveryNode.createLocal(
                                        Settings.EMPTY,
                                        new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                                        "node2"
                                    )
                                )
                        )
                        .build();

                    ClusterState newState = ClusterState.builder(previousState)
                        .nodes(
                            DiscoveryNodes.builder(previousState.nodes())
                                .add(httpTransport.node)
                                .masterNodeId(httpTransport.node.getId())
                                .localNodeId(httpTransport.node.getId())
                        )
                        .build();
                    ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);
                    spied.clusterChanged(event);
                    assertTrue(spied.ready());
                } catch (IOException e) {
                    fail("Shouldn't reach here");
                }

                return null;
            });
        }
        spied.stop();
        spied.close();
    }

    @SuppressForbidden(reason = "Intentional socket open")
    public void testStatusChange() throws Exception {
        ReadinessService spied = spy(readinessService);
        Path tempPath = Files.createTempFile("readiness", ".socket");
        Mockito.doReturn(tempPath).when(spied).getSocketPath();
        spied.start();

        assertFalse(spied.ready());

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2"))
            )
            .build();

        ClusterState newState = ClusterState.builder(previousState)
            .nodes(
                DiscoveryNodes.builder(previousState.nodes())
                    .add(httpTransport.node)
                    .masterNodeId(httpTransport.node.getId())
                    .localNodeId(httpTransport.node.getId())
            )
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);
        spied.clusterChanged(event);
        assertTrue(spied.ready());

        previousState = newState;

        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(tempPath);

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {

            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    channel.connect(socketAddress);
                    BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
                    String message = reader.readLine();
                    assertNotNull(message);
                    assertEquals("true," + httpTransport.boundAddress().publishAddress().getPort(), message);
                } catch (IOException e) {
                    fail("Shouldn't reach here");
                }

                return null;
            });
        }

        newState = ClusterState.builder(previousState)
            .metadata(
                Metadata.builder(previousState.metadata())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                httpTransport.node.getId(),
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(httpTransport.node.getId())
                                    .setReason("testing")
                                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .build()
                            )
                        )
                    )
                    .build()
            )
            .build();

        event = new ClusterChangedEvent("test", newState, previousState);
        spied.clusterChanged(event);
        assertFalse(spied.ready());

        spied.stop();
        spied.close();
    }

}
