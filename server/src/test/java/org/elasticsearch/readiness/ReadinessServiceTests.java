/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
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
import org.elasticsearch.test.readiness.ReadinessClientProbe;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class ReadinessServiceTests extends ESTestCase implements ReadinessClientProbe {
    private ClusterService clusterService;
    private ReadinessService readinessService;
    private ThreadPool threadpool;
    private Environment env;
    private FakeHttpTransport httpTransport;

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        final DiscoveryNode node;

        FakeHttpTransport() {
            node = TestDiscoveryNode.create(
                "local",
                "local",
                buildNewFakeTransportAddress(),
                emptyMap(),
                Set.of(DiscoveryNodeRole.MASTER_ROLE)
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
            threadpool,
            null
        );
        env = newEnvironment(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());

        httpTransport = new FakeHttpTransport();
        readinessService = new ReadinessService(clusterService, env);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testEphemeralSocket() throws Exception {
        Environment tempEnv = newEnvironment(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        ReadinessService tempService = new ReadinessService(clusterService, tempEnv);
        try (ServerSocketChannel channel = tempService.setupSocket()) {
            assertTrue(channel.isOpen());
            assertTrue(tempService.boundAddress().publishAddress().getPort() > 0);
        }
    }

    public void testBoundPortDoesntChange() throws Exception {
        Environment tempEnv = newEnvironment(Settings.builder().put(ReadinessService.PORT.getKey(), 0).build());
        ReadinessService tempService = new ReadinessService(clusterService, tempEnv);
        tempService.start();
        tempService.startListener();

        ServerSocketChannel channel = tempService.serverChannel();
        assertTrue(channel.isOpen());
        assertTrue(tempService.boundAddress().publishAddress().getPort() > 0);

        int port = tempService.boundAddress().publishAddress().getPort();

        tempService.stop();
        assertNull(tempService.serverChannel());
        tempService.start();
        tempService.startListener();
        assertEquals(port, ((InetSocketAddress) tempService.serverChannel().getLocalAddress()).getPort());
        tempService.stop();
        tempService.close();
    }

    public void testSocketChannelCreation() throws Exception {
        try (ServerSocketChannel channel = readinessService.setupSocket()) {
            assertTrue(channel.isOpen());
        }
    }

    public void testSocketAddress() throws UnknownHostException {
        InetSocketAddress socketAddress = readinessService.socketAddress(InetAddress.getByName("localhost"), 123);
        assertEquals(123, socketAddress.getPort());
        assertEquals("localhost", socketAddress.getHostString());
    }

    public void testEnabled() {
        Environment environment = newEnvironment(Settings.EMPTY);
        assertFalse(ReadinessService.enabled(environment));
        assertTrue(ReadinessService.enabled(env));
    }

    public void testStartStop() {
        assertTrue(ReadinessService.enabled(env));
        readinessService.start();
        readinessService.startListener();
        assertNotNull(readinessService.serverChannel());
        readinessService.stop();
        assertFalse(readinessService.ready());
        readinessService.close();
    }

    public void testStopWithoutStart() {
        assertTrue(ReadinessService.enabled(env));
        assertFalse(readinessService.ready());
        assertNull(readinessService.serverChannel());
        readinessService.stop();
        assertFalse(readinessService.ready());
        readinessService.close();
    }

    public void testTCPProbe() throws Exception {
        readinessService.start();
        // manually starting the listener, no Elasticsearch booted
        readinessService.startListener();

        // try to see if TCP probe succeeds
        tcpReadinessProbeTrue(readinessService);

        // mocking a cluster change event, with a master down
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
                    .masterNodeId(null) // No master node should cause the readiness service to kill the socket listener
                    .localNodeId(httpTransport.node.getId())
            )
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);
        readinessService.clusterChanged(event);

        // without master the service is not ready
        assertFalse(readinessService.ready());

        // test that we cannot connect to the socket anymore
        tcpReadinessProbeFalse(readinessService);

        readinessService.stop();
        readinessService.close();
    }

    public void testStatusChange() throws Exception {
        readinessService.start();

        // initially the service isn't ready
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
        readinessService.watchedFileChanged();

        // sending a cluster state with active master should bring up the service
        assertTrue(readinessService.ready());

        previousState = newState;
        tcpReadinessProbeTrue(readinessService);

        ClusterState noMasterState = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId(null))
            .build();
        event = new ClusterChangedEvent("test", noMasterState, previousState);
        readinessService.clusterChanged(event);
        assertFalse(readinessService.ready());
        tcpReadinessProbeFalse(readinessService);

        event = new ClusterChangedEvent("test", previousState, noMasterState);
        readinessService.clusterChanged(event);
        assertTrue(readinessService.ready());
        tcpReadinessProbeTrue(readinessService);

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
        tcpReadinessProbeFalse(readinessService);

        readinessService.stop();
        readinessService.close();
    }
}
