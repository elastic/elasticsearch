/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.discovery.zen.NodesFaultDetection;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.nio.MockNioTransport;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ZenFaultDetectionTests extends ESTestCase {
    protected ThreadPool threadPool;
    private CircuitBreakerService circuitBreakerService;

    protected static final Version version0 = Version.fromId(/*0*/99);
    protected DiscoveryNode nodeA;
    protected MockTransportService serviceA;
    private Settings settingsA;

    protected static final Version version1 = Version.fromId(199);
    protected DiscoveryNode nodeB;
    protected MockTransportService serviceB;
    private Settings settingsB;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), new ByteSizeValue(0))
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        threadPool = new TestThreadPool(getClass().getName());
        circuitBreakerService = new HierarchyCircuitBreakerService(settings, Collections.emptyList(), clusterSettings);
        settingsA = Settings.builder().put("node.name", "TS_A").put(settings).build();
        serviceA = build(settingsA, version0);
        nodeA = serviceA.getLocalDiscoNode();
        settingsB = Settings.builder().put("node.name", "TS_B").put(settings).build();
        serviceB = build(settingsB, version1);
        nodeB = serviceB.getLocalDiscoNode();

        // wait till all nodes are properly connected and the event has been sent, so tests in this class
        // will not get this callback called on the connections done in this setup
        final CountDownLatch latch = new CountDownLatch(2);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                fail("disconnect should not be called " + node);
            }
        };
        serviceA.addConnectionListener(waitForConnection);
        serviceB.addConnectionListener(waitForConnection);

        serviceA.connectToNode(nodeB);
        serviceA.connectToNode(nodeA);
        serviceB.connectToNode(nodeA);
        serviceB.connectToNode(nodeB);

        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        serviceA.removeConnectionListener(waitForConnection);
        serviceB.removeConnectionListener(waitForConnection);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        serviceA.close();
        serviceB.close();
        terminate(threadPool);
    }

    protected MockTransportService build(Settings settings, Version version) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        MockTransportService transportService =
            new MockTransportService(
                Settings.builder()
                    .put(settings)
                    // trace zenfd actions but keep the default otherwise
                    .putList(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.getKey(), TransportLivenessAction.NAME)
                    .build(),
                new MockNioTransport(settings, version, threadPool, new NetworkService(Collections.emptyList()),
                    PageCacheRecycler.NON_RECYCLING_INSTANCE, namedWriteableRegistry, circuitBreakerService),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                (boundAddress) ->
                    new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), boundAddress.publishAddress(),
                        Node.NODE_ATTRIBUTES.getAsMap(settings), DiscoveryNode.getRolesFromSettings(settings), version),
                null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    private DiscoveryNodes buildNodesForA(boolean master) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(nodeA);
        builder.add(nodeB);
        builder.localNodeId(nodeA.getId());
        builder.masterNodeId(master ? nodeA.getId() : nodeB.getId());
        return builder.build();
    }

    private DiscoveryNodes buildNodesForB(boolean master) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        builder.add(nodeA);
        builder.add(nodeB);
        builder.localNodeId(nodeB.getId());
        builder.masterNodeId(master ? nodeB.getId() : nodeA.getId());
        return builder.build();
    }

    public void testNodesFaultDetectionConnectOnDisconnect() throws InterruptedException {
        boolean shouldRetry = randomBoolean();
        // make sure we don't ping again after the initial ping
        final Settings pingSettings = Settings.builder()
            .put(FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING.getKey(), shouldRetry)
            .put(FaultDetection.PING_INTERVAL_SETTING.getKey(), "5m").build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).version(randomNonNegativeLong())
            .nodes(buildNodesForA(true)).build();
        NodesFaultDetection nodesFDA = new NodesFaultDetection(Settings.builder().put(settingsA).put(pingSettings).build(),
            threadPool, serviceA, () -> clusterState, clusterState.getClusterName());
        nodesFDA.setLocalNode(nodeA);
        NodesFaultDetection nodesFDB = new NodesFaultDetection(Settings.builder().put(settingsB).put(pingSettings).build(),
            threadPool, serviceB, () -> clusterState, clusterState.getClusterName());
        nodesFDB.setLocalNode(nodeB);
        final CountDownLatch pingSent = new CountDownLatch(1);
        nodesFDB.addListener(new NodesFaultDetection.Listener() {
            @Override
            public void onPingReceived(NodesFaultDetection.PingRequest pingRequest) {
                assertThat(pingRequest.clusterStateVersion(), equalTo(clusterState.version()));
                pingSent.countDown();
            }
        });
        nodesFDA.updateNodesAndPing(clusterState);

        // wait for the first ping to go out, so we will really respond to a disconnect event rather then
        // the ping failing
        pingSent.await(30, TimeUnit.SECONDS);

        final String[] failureReason = new String[1];
        final DiscoveryNode[] failureNode = new DiscoveryNode[1];
        final CountDownLatch notified = new CountDownLatch(1);
        nodesFDA.addListener(new NodesFaultDetection.Listener() {
            @Override
            public void onNodeFailure(DiscoveryNode node, String reason) {
                failureNode[0] = node;
                failureReason[0] = reason;
                notified.countDown();
            }
        });
        // will raise a disconnect on A
        serviceB.stop();
        notified.await(30, TimeUnit.SECONDS);

        CircuitBreaker inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        assertThat(inFlightRequestsBreaker.getTrippedCount(), equalTo(0L));

        assertEquals(nodeB, failureNode[0]);
        Matcher<String> matcher = Matchers.containsString("verified");
        if (shouldRetry == false) {
            matcher = Matchers.not(matcher);
        }

        assertThat(failureReason[0], matcher);

        assertWarnings(
            "[discovery.zen.fd.connect_on_network_disconnect] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.",
            "[discovery.zen.fd.ping_interval] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.");
    }

    public void testMasterFaultDetectionConnectOnDisconnect() throws InterruptedException {
        Settings.Builder settings = Settings.builder();
        boolean shouldRetry = randomBoolean();
        ClusterName clusterName = new ClusterName(randomAlphaOfLengthBetween(3, 20));

        // make sure we don't ping
        settings.put(FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING.getKey(), shouldRetry)
                .put(FaultDetection.PING_INTERVAL_SETTING.getKey(), "5m").put("cluster.name", clusterName.value());

        final ClusterState state = ClusterState.builder(clusterName).nodes(buildNodesForA(false)).build();
        AtomicReference<ClusterState> clusterStateSupplier = new AtomicReference<>(state);
        MasterFaultDetection masterFD = new MasterFaultDetection(settings.build(), threadPool, serviceA,
            clusterStateSupplier::get, null, clusterName);
        masterFD.restart(nodeB, "test");

        final String[] failureReason = new String[1];
        final DiscoveryNode[] failureNode = new DiscoveryNode[1];
        final CountDownLatch notified = new CountDownLatch(1);
        masterFD.addListener((masterNode, cause, reason) -> {
            failureNode[0] = masterNode;
            failureReason[0] = reason;
            notified.countDown();
        });
        // will raise a disconnect on A
        serviceB.stop();
        notified.await(30, TimeUnit.SECONDS);

        CircuitBreaker inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        assertThat(inFlightRequestsBreaker.getTrippedCount(), equalTo(0L));

        assertEquals(nodeB, failureNode[0]);
        Matcher<String> matcher = Matchers.containsString("verified");
        if (shouldRetry == false) {
            matcher = Matchers.not(matcher);
        }

        assertThat(failureReason[0], matcher);

        assertWarnings(
            "[discovery.zen.fd.connect_on_network_disconnect] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.",
            "[discovery.zen.fd.ping_interval] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.");
    }

    public void testMasterFaultDetectionNotSizeLimited() throws InterruptedException {
        boolean shouldRetry = randomBoolean();
        ClusterName clusterName = new ClusterName(randomAlphaOfLengthBetween(3, 20));
        final Settings settings = Settings.builder()
            .put(FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING.getKey(), shouldRetry)
            .put(FaultDetection.PING_INTERVAL_SETTING.getKey(), "1s")
            .put("cluster.name", clusterName.value()).build();
        final ClusterState stateNodeA = ClusterState.builder(clusterName).nodes(buildNodesForA(false)).build();
        AtomicReference<ClusterState> clusterStateSupplierA = new AtomicReference<>(stateNodeA);

        int minExpectedPings = 2;

        PingProbe pingProbeA = new PingProbe(minExpectedPings);
        PingProbe pingProbeB = new PingProbe(minExpectedPings);

        serviceA.addMessageListener(pingProbeA);
        serviceB.addMessageListener(pingProbeB);

        MasterFaultDetection masterFDNodeA = new MasterFaultDetection(Settings.builder().put(settingsA).put(settings).build(),
            threadPool, serviceA, clusterStateSupplierA::get, null, clusterName);
        masterFDNodeA.restart(nodeB, "test");

        final ClusterState stateNodeB = ClusterState.builder(clusterName).nodes(buildNodesForB(true)).build();
        AtomicReference<ClusterState> clusterStateSupplierB = new AtomicReference<>(stateNodeB);

        MasterFaultDetection masterFDNodeB = new MasterFaultDetection(Settings.builder().put(settingsB).put(settings).build(),
            threadPool, serviceB, clusterStateSupplierB::get, null, clusterName);
        masterFDNodeB.restart(nodeB, "test");

        // let's do a few pings
        pingProbeA.awaitMinCompletedPings();
        pingProbeB.awaitMinCompletedPings();

        CircuitBreaker inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        assertThat(inFlightRequestsBreaker.getTrippedCount(), equalTo(0L));
        assertThat(pingProbeA.completedPings(), greaterThanOrEqualTo(minExpectedPings));
        assertThat(pingProbeB.completedPings(), greaterThanOrEqualTo(minExpectedPings));

        assertWarnings(
            "[discovery.zen.fd.connect_on_network_disconnect] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.",
            "[discovery.zen.fd.ping_interval] setting was deprecated in Elasticsearch and will be removed in a future " +
                "release! See the breaking changes documentation for the next major version.");
    }

    private static class PingProbe implements TransportMessageListener {
        private final Set<Tuple<DiscoveryNode, Long>> inflightPings = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final Set<Tuple<DiscoveryNode, Long>> completedPings = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final CountDownLatch waitForPings;

        PingProbe(int minCompletedPings) {
            this.waitForPings = new CountDownLatch(minCompletedPings);
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                  TransportRequestOptions options) {
            if (MasterFaultDetection.MASTER_PING_ACTION_NAME.equals(action)) {
                inflightPings.add(Tuple.tuple(node, requestId));
            }
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void onResponseReceived(long requestId, Transport.ResponseContext context) {
            if (MasterFaultDetection.MASTER_PING_ACTION_NAME.equals(context.action())) {
                Tuple<DiscoveryNode, Long> ping = Tuple.tuple(context.connection().getNode(), requestId);
                if (inflightPings.remove(ping)) {
                    completedPings.add(ping);
                    waitForPings.countDown();
                }
            }
        }

        public int completedPings() {
            return completedPings.size();
        }

        public void awaitMinCompletedPings() throws InterruptedException {
            waitForPings.await();
        }
    }
}
