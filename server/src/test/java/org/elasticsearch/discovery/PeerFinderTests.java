/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.CapturingTransport.CapturedRequest;
import org.elasticsearch.test.transport.StubbableConnectionManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.ConnectionManager;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.discovery.PeerFinder.REQUEST_PEERS_ACTION_NAME;
import static org.elasticsearch.discovery.PeerFinder.VERBOSITY_INCREASE_TIMEOUT_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PeerFinderTests extends ESTestCase {

    private CapturingTransport capturingTransport;
    private DeterministicTaskQueue deterministicTaskQueue;
    private DiscoveryNode localNode;
    private MockTransportAddressConnector transportAddressConnector;
    private TestPeerFinder peerFinder;
    private List<TransportAddress> providedAddresses;
    private long addressResolveDelay; // -1 means address resolution fails

    private final Set<DiscoveryNode> disconnectedNodes = new HashSet<>();
    private final Set<DiscoveryNode> connectedNodes = new HashSet<>();
    private DiscoveryNodes lastAcceptedNodes;
    private TransportService transportService;
    private Iterable<DiscoveryNode> foundPeersFromNotification;

    private static final long CONNECTION_TIMEOUT_MILLIS = 30000;

    class MockTransportAddressConnector implements TransportAddressConnector {
        final Map<TransportAddress, DiscoveryNode> reachableNodes = new HashMap<>();
        final Set<TransportAddress> unreachableAddresses = new HashSet<>();
        final Set<TransportAddress> slowAddresses = new HashSet<>();
        final Set<TransportAddress> inFlightConnectionAttempts = new HashSet<>();

        void addReachableNode(DiscoveryNode node) {
            reachableNodes.put(node.getAddress(), node);
        }

        @Override
        public void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<ProbeConnectionResult> listener) {
            assert localNode.getAddress().equals(transportAddress) == false : "should not probe local node";

            final boolean isNotInFlight = inFlightConnectionAttempts.add(transportAddress);
            assertTrue(isNotInFlight);

            final long connectResultTime = deterministicTaskQueue.getCurrentTimeMillis() + (slowAddresses.contains(transportAddress)
                ? CONNECTION_TIMEOUT_MILLIS
                : 0);

            deterministicTaskQueue.scheduleAt(connectResultTime, new Runnable() {
                @Override
                public void run() {
                    if (unreachableAddresses.contains(transportAddress)) {
                        assertTrue(inFlightConnectionAttempts.remove(transportAddress));
                        listener.onFailure(new IOException("cannot connect to " + transportAddress));
                        return;
                    }

                    for (final Map.Entry<TransportAddress, DiscoveryNode> addressAndNode : reachableNodes.entrySet()) {
                        if (addressAndNode.getKey().equals(transportAddress)) {
                            final DiscoveryNode discoveryNode = addressAndNode.getValue();
                            if (discoveryNode.isMasterNode()) {
                                disconnectedNodes.remove(discoveryNode);
                                connectedNodes.add(discoveryNode);
                                assertTrue(inFlightConnectionAttempts.remove(transportAddress));
                                listener.onResponse(new ProbeConnectionResult(discoveryNode, () -> {
                                    if (connectedNodes.remove(discoveryNode)) {
                                        disconnectedNodes.add(discoveryNode);
                                    }
                                }));
                                return;
                            } else {
                                listener.onFailure(new ElasticsearchException("non-master node " + discoveryNode));
                                return;
                            }
                        }
                    }

                    throw new AssertionError(transportAddress + " unknown");
                }

                @Override
                public String toString() {
                    return "connection attempt to " + transportAddress;
                }
            });
        }
    }

    class TestPeerFinder extends PeerFinder {
        DiscoveryNode discoveredMasterNode;
        OptionalLong discoveredMasterTerm = OptionalLong.empty();

        TestPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector) {
            super(settings, transportService, transportAddressConnector, PeerFinderTests.this::resolveConfiguredHosts);
        }

        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            assert holdsLock() == false : "PeerFinder lock held in error";
            assertThat(discoveredMasterNode, nullValue());
            assertFalse(discoveredMasterTerm.isPresent());
            discoveredMasterNode = masterNode;
            discoveredMasterTerm = OptionalLong.of(term);
        }

        @Override
        protected void onFoundPeersUpdated() {
            assert holdsLock() == false : "PeerFinder lock held in error";
            foundPeersFromNotification = getFoundPeers();
            logger.trace("onFoundPeersUpdated({})", foundPeersFromNotification);
        }
    }

    private void resolveConfiguredHosts(Consumer<List<TransportAddress>> onResult) {
        if (addressResolveDelay >= 0) {
            deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + addressResolveDelay, new Runnable() {
                @Override
                public void run() {
                    onResult.accept(providedAddresses);
                }

                @Override
                public String toString() {
                    return "PeerFinderTests#resolveConfiguredHosts";
                }
            });
        } else {
            assertThat(addressResolveDelay, is(-1L));
        }
    }

    private void updateLastAcceptedNodes(Consumer<DiscoveryNodes.Builder> onBuilder) {
        final Builder builder = DiscoveryNodes.builder(lastAcceptedNodes);
        onBuilder.accept(builder);
        lastAcceptedNodes = builder.build();
    }

    @Before
    public void setup() {
        capturingTransport = new CapturingTransport();
        transportAddressConnector = new MockTransportAddressConnector();
        providedAddresses = new ArrayList<>();
        addressResolveDelay = 0L;

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue();

        localNode = newDiscoveryNode("local-node");

        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ConnectionManager innerConnectionManager = new ClusterConnectionManager(
            settings,
            capturingTransport,
            threadPool.getThreadContext()
        );
        StubbableConnectionManager connectionManager = new StubbableConnectionManager(innerConnectionManager);
        connectionManager.setDefaultNodeConnectedBehavior((cm, discoveryNode) -> {
            final boolean isConnected = connectedNodes.contains(discoveryNode);
            final boolean isDisconnected = disconnectedNodes.contains(discoveryNode);
            assert isConnected != isDisconnected : discoveryNode + ": isConnected=" + isConnected + ", isDisconnected=" + isDisconnected;
            return isConnected;
        });
        connectionManager.setDefaultGetConnectionBehavior((cm, discoveryNode) -> capturingTransport.createConnection(discoveryNode));
        transportService = new TransportService(
            settings,
            capturingTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            connectionManager,
            new TaskManager(settings, threadPool, emptySet()),
            Tracer.NOOP
        );

        transportService.start();
        transportService.acceptIncomingRequests();

        lastAcceptedNodes = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build();

        peerFinder = new TestPeerFinder(settings, transportService, transportAddressConnector);
        foundPeersFromNotification = emptyList();
    }

    @After
    public void deactivateAndRunRemainingTasks() {
        peerFinder.deactivate(localNode);
        peerFinder.closePeers();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(connectedNodes, empty());
    }

    public void testAddsReachableNodesFromUnicastHostsList() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDoesNotReturnDuplicateNodesWithDistinctAddresses() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        final TransportAddress alternativeAddress = buildNewFakeTransportAddress();

        providedAddresses.add(otherNode.getAddress());
        providedAddresses.add(alternativeAddress);
        transportAddressConnector.addReachableNode(otherNode);
        transportAddressConnector.reachableNodes.put(alternativeAddress, otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testAddsReachableNodesFromUnicastHostsListProvidedLater() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);
        addressResolveDelay = 10000;

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();

        final long successTime = addressResolveDelay + PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        while (deterministicTaskQueue.getCurrentTimeMillis() < successTime) {
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        assertFoundPeers(otherNode);
    }

    public void testDoesNotRequireAddressResolutionToSucceed() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);
        addressResolveDelay = -1;

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();

        final long successTime = 10000 + PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        while (deterministicTaskQueue.getCurrentTimeMillis() < successTime) {
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        assertFoundPeers();
    }

    public void testDoesNotAddUnreachableNodesFromUnicastHostsList() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers();
    }

    public void testDoesNotAddNonMasterEligibleNodesFromUnicastHostsList() {
        final DiscoveryNode nonMasterNode = DiscoveryNodeUtils.builder("node-from-hosts-list").roles(emptySet()).build();

        providedAddresses.add(nonMasterNode.getAddress());
        transportAddressConnector.addReachableNode(nonMasterNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers();

        assertThat(capturingTransport.capturedRequests(), emptyArray());
    }

    public void testChecksUnicastHostsForChanges() {
        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();

        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDeactivationRetainsPastKnowledgeUntilClosed() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        peerFinder.deactivate(localNode);
        assertFoundPeers();
        assertEquals(Set.of(otherNode), connectedNodes);

        peerFinder.activate(DiscoveryNodes.EMPTY_NODES);
        assertFoundPeers(otherNode);

        peerFinder.deactivate(localNode);
        assertFoundPeers();
        assertEquals(Set.of(otherNode), connectedNodes);
        peerFinder.closePeers();
        assertThat(connectedNodes, empty());

        providedAddresses.clear();
        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();
    }

    public void testDeactivationDuringConnectionAttemptReleasesPeer() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        deterministicTaskQueue.setExecutionDelayVariabilityMillis(10000);
        peerFinder.activate(lastAcceptedNodes);
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + 1, () -> peerFinder.deactivate(localNode));
        do {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks(); // mainly verifying that this doesn't trip an assertion
        } while (deterministicTaskQueue.hasDeferredTasks());
        assertFoundPeers();

        peerFinder.closePeers();
        assertThat(connectedNodes, empty());
    }

    public void testAddsReachableNodesFromClusterState() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-in-cluster-state");
        updateLastAcceptedNodes(b -> b.add(otherNode));
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDoesNotAddUnreachableNodesFromClusterState() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-in-cluster-state");
        updateLastAcceptedNodes(b -> b.add(otherNode));
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();
    }

    public void testAddsReachableNodesFromIncomingRequests() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        final DiscoveryNode otherKnownNode = newDiscoveryNode("other-known-node");

        transportAddressConnector.addReachableNode(sourceNode);
        transportAddressConnector.addReachableNode(otherKnownNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(sourceNode, otherKnownNode);
    }

    public void testDoesNotAddReachableNonMasterEligibleNodesFromIncomingRequests() {
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder("request-source").roles(emptySet()).build();
        final DiscoveryNode otherKnownNode = newDiscoveryNode("other-known-node");

        transportAddressConnector.addReachableNode(otherKnownNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(otherKnownNode);
    }

    public void testDoesNotAddUnreachableNodesFromIncomingRequests() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        final DiscoveryNode otherKnownNode = newDiscoveryNode("other-known-node");

        transportAddressConnector.addReachableNode(sourceNode);
        transportAddressConnector.unreachableAddresses.add(otherKnownNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(sourceNode);
    }

    public void testDoesNotAddUnreachableSourceNodeFromIncomingRequests() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        final DiscoveryNode otherKnownNode = newDiscoveryNode("other-known-node");

        transportAddressConnector.unreachableAddresses.add(sourceNode.getAddress());
        transportAddressConnector.addReachableNode(otherKnownNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(otherKnownNode);
    }

    public void testRespondsToRequestWhenActive() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");

        transportAddressConnector.addReachableNode(sourceNode);

        peerFinder.activate(lastAcceptedNodes);
        final PeersResponse peersResponse1 = peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.emptyList()));
        assertFalse(peersResponse1.getMasterNode().isPresent());
        assertThat(peersResponse1.getKnownPeers(), empty()); // sourceNode is not yet known
        assertThat(peersResponse1.getTerm(), is(0L));

        runAllRunnableTasks();

        assertFoundPeers(sourceNode);

        final long updatedTerm = randomNonNegativeLong();
        peerFinder.setCurrentTerm(updatedTerm);
        final PeersResponse peersResponse2 = peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.emptyList()));
        assertFalse(peersResponse2.getMasterNode().isPresent());
        assertThat(peersResponse2.getKnownPeers(), contains(sourceNode));
        assertThat(peersResponse2.getTerm(), is(updatedTerm));
    }

    public void testDelegatesRequestHandlingWhenInactive() {
        final DiscoveryNode masterNode = newDiscoveryNode("master-node");
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        transportAddressConnector.addReachableNode(sourceNode);

        peerFinder.activate(DiscoveryNodes.EMPTY_NODES);

        final long term = randomNonNegativeLong();
        peerFinder.setCurrentTerm(term);
        peerFinder.deactivate(masterNode);
        assertThat(connectedNodes, empty());

        final PeersResponse expectedResponse = new PeersResponse(Optional.of(masterNode), Collections.emptyList(), term);
        final PeersResponse peersResponse = peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.emptyList()));
        assertThat(peersResponse, equalTo(expectedResponse));
    }

    public void testReceivesRequestsFromTransportService() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");

        transportAddressConnector.addReachableNode(sourceNode);

        peerFinder.activate(lastAcceptedNodes);

        final AtomicBoolean responseReceived = new AtomicBoolean();

        transportService.sendRequest(
            localNode,
            REQUEST_PEERS_ACTION_NAME,
            new PeersRequest(sourceNode, Collections.emptyList()),
            new TransportResponseHandler<PeersResponse>() {
                @Override
                public PeersResponse read(StreamInput in) throws IOException {
                    return new PeersResponse(in);
                }

                @Override
                public Executor executor() {
                    return TransportResponseHandler.TRANSPORT_WORKER;
                }

                @Override
                public void handleResponse(PeersResponse response) {
                    assertTrue(responseReceived.compareAndSet(false, true));
                    assertFalse(response.getMasterNode().isPresent());
                    assertThat(response.getKnownPeers(), empty()); // sourceNode is not yet known
                    assertThat(response.getTerm(), is(0L));
                }

                @Override
                public void handleException(TransportException exp) {
                    fail(exp);
                }
            }
        );

        runAllRunnableTasks();
        assertTrue(responseReceived.get());
        assertFoundPeers(sourceNode);
    }

    public void testRequestsPeersIncludingKnownPeersInRequest() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        final CapturedRequest[] capturedRequests = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, is(1));
        final PeersRequest peersRequest = (PeersRequest) capturedRequests[0].request();
        assertThat(peersRequest.getKnownPeers(), contains(otherNode));
    }

    public void testAddsReachablePeersFromResponse() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        final DiscoveryNode discoveredNode = newDiscoveryNode("discovered-node");
        transportAddressConnector.addReachableNode(discoveredNode);
        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.empty(), singletonList(discoveredNode), randomNonNegativeLong());
        });

        runAllRunnableTasks();
        assertFoundPeers(otherNode, discoveredNode);
    }

    public void testAddsReachableMasterFromResponse() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
        final DiscoveryNode discoveredMaster = newDiscoveryNode("discovered-master");

        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.of(discoveredMaster), emptyList(), randomNonNegativeLong());
        });

        transportAddressConnector.addReachableNode(discoveredMaster);
        runAllRunnableTasks();
        assertFoundPeers(otherNode, discoveredMaster);
        assertThat(peerFinder.discoveredMasterNode, nullValue());
        assertFalse(peerFinder.discoveredMasterTerm.isPresent());
    }

    public void testHandlesDiscoveryOfMasterFromResponseFromMaster() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        final long term = randomNonNegativeLong();
        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.of(otherNode), emptyList(), term);
        });

        runAllRunnableTasks();
        assertFoundPeers(otherNode);
        assertThat(peerFinder.discoveredMasterNode, is(otherNode));
        assertThat(peerFinder.discoveredMasterTerm, is(OptionalLong.of(term)));
    }

    public void testOnlyRequestsPeersOncePerRoundButDoesRetryNextRound() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        transportAddressConnector.addReachableNode(sourceNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, emptyList()));
        runAllRunnableTasks();
        assertFoundPeers(sourceNode);

        respondToRequests(node -> {
            assertThat(node, is(sourceNode));
            return new PeersResponse(Optional.empty(), singletonList(sourceNode), randomNonNegativeLong());
        });

        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, emptyList()));
        runAllRunnableTasks();
        respondToRequests(node -> { throw new AssertionError("there should have been no further requests"); });

        final DiscoveryNode otherNode = newDiscoveryNode("otherNode");
        transportAddressConnector.addReachableNode(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();
        respondToRequests(node -> {
            assertThat(node, is(sourceNode));
            return new PeersResponse(Optional.empty(), singletonList(otherNode), randomNonNegativeLong());
        });
        runAllRunnableTasks();
        assertFoundPeers(sourceNode, otherNode);
    }

    public void testDoesNotReconnectToNodesOnceConnected() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        transportAddressConnector.reachableNodes.clear();
        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDiscardsDisconnectedNodes() {
        final DiscoveryNode otherNode = newDiscoveryNode("original-node");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        transportAddressConnector.reachableNodes.clear();
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());
        connectedNodes.remove(otherNode);
        disconnectedNodes.add(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();
        assertFoundPeers();
    }

    public void testDoesNotMakeMultipleConcurrentConnectionAttemptsToOneAddress() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());
        transportAddressConnector.slowAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks(); // MockTransportAddressConnector verifies no multiple connection attempts
        assertFoundPeers();

        transportAddressConnector.slowAddresses.clear();
        transportAddressConnector.unreachableAddresses.clear();
        transportAddressConnector.addReachableNode(otherNode);

        while (deterministicTaskQueue.getCurrentTimeMillis() < CONNECTION_TIMEOUT_MILLIS) {
            assertFoundPeers();
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        // need to wait for the connection to timeout, then for another wakeup, before discovering the peer
        final long expectedTime = CONNECTION_TIMEOUT_MILLIS + PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(Settings.EMPTY).millis();

        while (deterministicTaskQueue.getCurrentTimeMillis() < expectedTime) {
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        assertFoundPeers(otherNode);
    }

    public void testTimesOutAndRetriesConnectionsToBlackholedNodes() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        final DiscoveryNode nodeToFind = newDiscoveryNode("node-to-find");

        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);
        transportAddressConnector.addReachableNode(nodeToFind);

        peerFinder.activate(lastAcceptedNodes);

        while (true) {
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks(); // MockTransportAddressConnector verifies no multiple connection attempts
            if (capturingTransport.getCapturedRequestsAndClear().length > 0) {
                break;
            }
        }

        final long timeoutAtMillis = deterministicTaskQueue.getCurrentTimeMillis() + PeerFinder.DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING.get(
            Settings.EMPTY
        ).millis();
        while (deterministicTaskQueue.getCurrentTimeMillis() < timeoutAtMillis) {
            assertFoundPeers(otherNode);
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        // need to wait for the connection to timeout, then for another wakeup, before discovering the peer
        final long expectedTime = timeoutAtMillis + PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(Settings.EMPTY).millis();

        while (deterministicTaskQueue.getCurrentTimeMillis() < expectedTime) {
            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
        }

        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.empty(), singletonList(nodeToFind), randomNonNegativeLong());
        });

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(nodeToFind, otherNode);
    }

    @TestLogging(reason = "testing logging at WARN level", value = "org.elasticsearch.discovery:WARN")
    public void testLogsWarningsIfActiveForLongEnough() throws IllegalAccessException {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");

        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + VERBOSITY_INCREASE_TIMEOUT_SETTING.get(Settings.EMPTY)
            .millis();

        try (var mockLog = MockLog.capture(PeerFinder.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "discovery result",
                    "org.elasticsearch.discovery.PeerFinder",
                    Level.WARN,
                    "address [" + otherNode.getAddress() + "]* discovery result: cannot connect to*"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getThrown() == null; // no stack trace at this log level
                    }
                }
            );
            while (deterministicTaskQueue.getCurrentTimeMillis() <= endTime) {
                deterministicTaskQueue.advanceTime();
                runAllRunnableTasks();
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(reason = "testing logging at DEBUG level", value = "org.elasticsearch.discovery:DEBUG")
    public void testLogsStackTraceInDiscoveryResultMessages() throws IllegalAccessException {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");

        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + VERBOSITY_INCREASE_TIMEOUT_SETTING.get(Settings.EMPTY)
            .millis();

        try (var mockLog = MockLog.capture(PeerFinder.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "discovery result",
                    "org.elasticsearch.discovery.PeerFinder",
                    Level.DEBUG,
                    "address [" + otherNode.getAddress() + "]* discovery result*"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getThrown() instanceof IOException && event.getThrown().getMessage().startsWith("cannot connect to");
                    }
                }
            );

            deterministicTaskQueue.advanceTime();
            runAllRunnableTasks();
            mockLog.assertAllExpectationsMatched();

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "discovery result",
                    "org.elasticsearch.discovery.PeerFinder",
                    Level.WARN,
                    "address [" + otherNode.getAddress() + "]* discovery result*"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getThrown() instanceof IOException && event.getThrown().getMessage().startsWith("cannot connect to");
                    }
                }
            );
            while (deterministicTaskQueue.getCurrentTimeMillis() <= endTime) {
                deterministicTaskQueue.advanceTime();
                runAllRunnableTasks();
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(reason = "testing logging at WARN level", value = "org.elasticsearch.discovery:WARN")
    public void testEventuallyLogsIfReturnedMasterIsUnreachable() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + VERBOSITY_INCREASE_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
            + PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(Settings.EMPTY).millis();

        runAllRunnableTasks();

        assertFoundPeers(otherNode);
        final DiscoveryNode unreachableMaster = newDiscoveryNode("unreachable-master");
        transportAddressConnector.unreachableAddresses.add(unreachableMaster.getAddress());

        MockLog.assertThatLogger(() -> {
            while (deterministicTaskQueue.getCurrentTimeMillis() <= endTime) {
                deterministicTaskQueue.advanceTime();
                runAllRunnableTasks();
                respondToRequests(node -> {
                    assertThat(node, is(otherNode));
                    return new PeersResponse(Optional.of(unreachableMaster), emptyList(), randomNonNegativeLong());
                });
            }
        },
            PeerFinder.class,
            new MockLog.SeenEventExpectation(
                "discovery result",
                "org.elasticsearch.discovery.PeerFinder",
                Level.WARN,
                "address ["
                    + unreachableMaster.getAddress()
                    + "]* [current master according to *node-from-hosts-list*ClusterFormationFailureHelper*discovery-troubleshooting*"
            )
        );

        assertFoundPeers(otherNode);
        assertThat(peerFinder.discoveredMasterNode, nullValue());
        assertFalse(peerFinder.discoveredMasterTerm.isPresent());
    }

    public void testReconnectsToDisconnectedNodes() {
        final DiscoveryNode otherNode = newDiscoveryNode("original-node");
        providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.addReachableNode(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        transportAddressConnector.reachableNodes.clear();
        final DiscoveryNode rebootedOtherNode = DiscoveryNodeUtils.create("rebooted-node", otherNode.getAddress());
        transportAddressConnector.addReachableNode(rebootedOtherNode);

        connectedNodes.remove(otherNode);
        disconnectedNodes.add(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(rebootedOtherNode);
    }

    private void respondToRequests(Function<DiscoveryNode, PeersResponse> responseFactory) {
        final CapturedRequest[] capturedRequests = capturingTransport.getCapturedRequestsAndClear();
        for (final CapturedRequest capturedRequest : capturedRequests) {
            assertThat(capturedRequest.action(), is(REQUEST_PEERS_ACTION_NAME));
            assertThat(capturedRequest.request(), instanceOf(PeersRequest.class));
            final PeersRequest peersRequest = (PeersRequest) capturedRequest.request();
            assertThat(peersRequest.getSourceNode(), is(localNode));
            capturingTransport.handleResponse(capturedRequests[0].requestId(), responseFactory.apply(capturedRequest.node()));
        }
    }

    private void assertFoundPeers(DiscoveryNode... expectedNodesArray) {
        final Set<DiscoveryNode> expectedNodes = Arrays.stream(expectedNodesArray).collect(Collectors.toSet());
        final List<DiscoveryNode> actualNodesList = StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false).toList();
        final HashSet<DiscoveryNode> actualNodesSet = new HashSet<>(actualNodesList);
        assertThat(actualNodesSet, equalTo(expectedNodes));
        assertTrue("no duplicates in " + actualNodesList, actualNodesSet.size() == actualNodesList.size());
        assertNotifiedOfAllUpdates();
    }

    private void assertNotifiedOfAllUpdates() {
        final Stream<DiscoveryNode> actualNodes = StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false);
        final Stream<DiscoveryNode> notifiedNodes = StreamSupport.stream(foundPeersFromNotification.spliterator(), false);
        assertThat(notifiedNodes.collect(Collectors.toSet()), equalTo(actualNodes.collect(Collectors.toSet())));
    }

    private DiscoveryNode newDiscoveryNode(String nodeId) {
        return DiscoveryNodeUtils.create(nodeId);
    }

    private void runAllRunnableTasks() {
        deterministicTaskQueue.scheduleNow(new Runnable() {
            @Override
            public void run() {
                PeerFinderTests.this.assertNotifiedOfAllUpdates();
            }

            @Override
            public String toString() {
                return "assertNotifiedOfAllUpdates";
            }
        });
        deterministicTaskQueue.runAllRunnableTasks();
        assertNotifiedOfAllUpdates();
    }
}
