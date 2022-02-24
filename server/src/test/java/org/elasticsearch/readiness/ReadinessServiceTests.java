/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.apache.lucene.tests.util.LuceneTestCase;
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
import org.elasticsearch.core.PathUtils;
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

@LuceneTestCase.SuppressFileSystems("*")
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
        Path socketFilePath = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("readiness.socket");
        Settings settings = Settings.builder()
            .put(Environment.READINESS_SOCKET_FILE.getKey(), socketFilePath.normalize().toAbsolutePath())
            .build();
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
        assertEquals(env.readinessSocketFile(), readinessService.getSocketPath());
    }

    public void testSocketChannelCreation() throws Exception {
        // Unix domain sockets need real file system, we can't use the Lucene Filter FS
        try (ServerSocketChannel channel = readinessService.setupUnixDomainSocket(readinessService.getSocketPath())) {
            assertTrue(channel.isOpen());
            assertTrue(Files.exists(readinessService.getSocketPath()));
        }
    }

    public void testStartStop() {
        readinessService.start();
        assertNotNull(readinessService.serverChannel());
        readinessService.stop();
        assertTrue(Files.exists(readinessService.getSocketPath()));
        assertFalse(readinessService.ready());
        readinessService.close();
        assertFalse(Files.exists(readinessService.getSocketPath()));
    }

    public void testLongSocketPathName() {
        ReadinessService spied = spy(readinessService);
        Path tempPath = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve(randomAlphaOfLength(108));
        Mockito.doReturn(tempPath).when(spied).getSocketPath();
        assertEquals("Unix domain path too long", expectThrows(IllegalStateException.class, () -> spied.start()).getCause().getMessage());
    }

    @SuppressForbidden(reason = "Intentional socket open")
    public void testSendStatus() throws Exception {
        readinessService.start();

        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(readinessService.getSocketPath());

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
                    readinessService.clusterChanged(event);
                    assertTrue(readinessService.ready());
                } catch (IOException e) {
                    fail("Shouldn't reach here");
                }

                return null;
            });
        }
        readinessService.stop();
        readinessService.close();
    }

    @SuppressForbidden(reason = "Intentional socket open")
    public void testStatusChange() throws Exception {
        readinessService.start();

        assertFalse(readinessService.ready());

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
        readinessService.clusterChanged(event);
        assertTrue(readinessService.ready());

        previousState = newState;

        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(readinessService.getSocketPath());

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

        ClusterState noMasterState = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId(null))
            .build();
        event = new ClusterChangedEvent("test", noMasterState, previousState);
        readinessService.clusterChanged(event);
        assertFalse(readinessService.ready());

        event = new ClusterChangedEvent("test", previousState, noMasterState);
        readinessService.clusterChanged(event);
        assertTrue(readinessService.ready());

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
        readinessService.clusterChanged(event);
        assertFalse(readinessService.ready());

        readinessService.stop();
        readinessService.close();
    }
}
