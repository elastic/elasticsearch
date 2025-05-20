/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.readiness;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
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
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata.ErrorKind.TRANSIENT;
import static org.elasticsearch.cluster.metadata.ReservedStateMetadata.EMPTY_VERSION;

public class ReadinessServiceTests extends ESTestCase implements ReadinessClientProbe {
    private ClusterService clusterService;
    private ReadinessService readinessService;
    private ThreadPool threadpool;
    private Environment env;
    private FakeHttpTransport httpTransport;

    private static Metadata emptyReservedStateMetadata;
    static {
        var fileSettingsState = new ReservedStateMetadata.Builder(FileSettingsService.NAMESPACE).version(EMPTY_VERSION);
        emptyReservedStateMetadata = new Metadata.Builder().put(fileSettingsState.build()).build();
    }

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        final DiscoveryNode node;

        FakeHttpTransport() {
            node = DiscoveryNodeUtils.builder("local").name("local").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
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

    public void testCloseWithoutStart() {
        assertTrue(ReadinessService.enabled(env));
        assertFalse(readinessService.ready());
        assertNull(readinessService.serverChannel());
        readinessService.close();
        assertFalse(readinessService.ready());
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
                DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node2", new TransportAddress(TransportAddress.META_ADDRESS, 9201)))
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

        ClusterState noFileSettingsState = noFileSettingsState();
        ClusterChangedEvent event = new ClusterChangedEvent("test", noFileSettingsState, emptyState());
        readinessService.clusterChanged(event);

        // sending a cluster state with active master should not yet bring up the service, file settings still are not applied
        assertFalse(readinessService.ready());

        ClusterState completeState = ClusterState.builder(noFileSettingsState).metadata(emptyReservedStateMetadata).build();
        event = new ClusterChangedEvent("test", completeState, noFileSettingsState);
        readinessService.clusterChanged(event);

        // sending a cluster state with active master and file settings applied should bring up the service
        assertTrue(readinessService.ready());
        tcpReadinessProbeTrue(readinessService);

        ClusterState noMasterState = ClusterState.builder(completeState).nodes(completeState.nodes().withMasterNodeId(null)).build();
        event = new ClusterChangedEvent("test", noMasterState, completeState);
        readinessService.clusterChanged(event);
        assertFalse(readinessService.ready());
        tcpReadinessProbeFalse(readinessService);

        event = new ClusterChangedEvent("test", completeState, noMasterState);
        readinessService.clusterChanged(event);
        assertTrue(readinessService.ready());
        tcpReadinessProbeTrue(readinessService);

        // shutting down flips back to not ready
        ClusterState nodeShuttingDownState = ClusterState.builder(completeState)
            .metadata(
                Metadata.builder(completeState.metadata())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                httpTransport.node.getId(),
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(httpTransport.node.getId())
                                    .setNodeEphemeralId(httpTransport.node.getEphemeralId())
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
        event = new ClusterChangedEvent("test", nodeShuttingDownState, completeState);
        try (var mockLog = MockLog.capture(ReadinessService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "node shutting down logged",
                    ReadinessService.class.getCanonicalName(),
                    Level.INFO,
                    "marking node as not ready because it's shutting down"
                )
            );
            readinessService.clusterChanged(event);
            mockLog.assertAllExpectationsMatched();

            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "node shutting down not logged twice",
                    ReadinessService.class.getCanonicalName(),
                    Level.INFO,
                    "marking node as not ready because it's shutting down"
                )
            );
            readinessService.clusterChanged(event);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse(readinessService.ready());
        tcpReadinessProbeFalse(readinessService);

        readinessService.stop();
        readinessService.close();
    }

    public void testFileSettingsUpdateError() throws Exception {
        // ensure an update to file settings that results in an error state doesn't cause readiness to fail
        // since the current file settings already applied cleanly
        readinessService.start();

        // initially the service isn't ready because initial cluster state has not been applied yet
        assertFalse(readinessService.ready());

        var fileSettingsState = new ReservedStateMetadata.Builder(FileSettingsService.NAMESPACE).version(21L)
            .errorMetadata(new ReservedStateErrorMetadata(22L, TRANSIENT, List.of("dummy error")));
        ClusterState state = ClusterState.builder(noFileSettingsState())
            .metadata(new Metadata.Builder().put(fileSettingsState.build()))
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE);
        readinessService.clusterChanged(event);
        assertTrue(readinessService.ready());

        readinessService.stop();
        readinessService.close();
    }

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node2", new TransportAddress(TransportAddress.META_ADDRESS, 9201)))
            )
            .build();
    }

    private ClusterState noFileSettingsState() {
        ClusterState emptyState = emptyState();
        return ClusterState.builder(emptyState)
            .nodes(
                DiscoveryNodes.builder(emptyState.nodes())
                    .add(httpTransport.node)
                    .masterNodeId(httpTransport.node.getId())
                    .localNodeId(httpTransport.node.getId())
            )
            .build();
    }
}
