/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.PeerFinder.TransportAddressConnector;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.CapturingTransport.CapturedRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.coordination.PeerFinder.REQUEST_PEERS_ACTION_NAME;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PeerFinderTests extends ESTestCase {

    private CapturingTransport capturingTransport;
    private DeterministicTaskQueue deterministicTaskQueue;
    private DiscoveryNode localNode;
    private MockUnicastHostsProvider unicastHostsProvider;
    private MockTransportAddressConnector transportAddressConnector;
    private TestPeerFinder peerFinder;

    private Set<DiscoveryNode> disconnectedNodes = new HashSet<>();
    private Set<DiscoveryNode> connectedNodes = new HashSet<>();
    private DiscoveryNodes lastAcceptedNodes;


    class MockTransportAddressConnector implements TransportAddressConnector {
        final Set<DiscoveryNode> reachableNodes = new HashSet<>();
        final Set<TransportAddress> unreachableAddresses = new HashSet<>();

        @Override
        public DiscoveryNode connectTo(TransportAddress transportAddress) throws IOException {
            assert localNode.getAddress().equals(transportAddress) == false : "should not probe local node";

            if (unreachableAddresses.contains(transportAddress)) {
                throw new IOException("cannot connect to " + transportAddress);
            }

            for (final DiscoveryNode discoveryNode : reachableNodes) {
                if (discoveryNode.getAddress().equals(transportAddress)) {
                    disconnectedNodes.remove(discoveryNode);
                    connectedNodes.add(discoveryNode);
                    return discoveryNode;
                }
            }

            throw new AssertionError(transportAddress + " unknown");
        }
    }

    static class MockUnicastHostsProvider implements UnicastHostsProvider {
        final List<TransportAddress> providedAddresses = new ArrayList<>();

        @Override
        public List<TransportAddress> buildDynamicHosts(HostsResolver hostsResolver) {
            return providedAddresses;
        }
    }

    static class TestPeerFinder extends PeerFinder {
        DiscoveryNode discoveredMasterNode;

        TestPeerFinder(Settings settings, TransportService transportService, UnicastHostsProvider hostsProvider,
                       FutureExecutor futureExecutor, TransportAddressConnector transportAddressConnector,
                       Supplier<ExecutorService> executorServiceFactory) {
            super(settings, transportService, hostsProvider, futureExecutor, transportAddressConnector, executorServiceFactory);
        }

        @Override
        protected void onMasterFoundByProbe(DiscoveryNode masterNode, long term) {
            assert holdsLock() == false : "PeerFinder lock held in error";
            assertThat(discoveredMasterNode, nullValue());
            discoveredMasterNode = masterNode;
        }
    }

    private void updateLastAcceptedNodes(Consumer<DiscoveryNodes.Builder> onBuilder) {
        final Builder builder = DiscoveryNodes.builder(lastAcceptedNodes);
        onBuilder.accept(builder);
        lastAcceptedNodes = builder.build();
    }

    @Before
    public void setup() {
        capturingTransport = new CapturingTransport() {
            @Override
            public boolean nodeConnected(DiscoveryNode node) {
                final boolean isConnected = connectedNodes.contains(node);
                final boolean isDisconnected = disconnectedNodes.contains(node);
                assert isConnected != isDisconnected : node + ": isConnected=" + isConnected + ", isDisconnected=" + isDisconnected;
                return isConnected;
            }
        };
        unicastHostsProvider = new MockUnicastHostsProvider();
        transportAddressConnector = new MockTransportAddressConnector();

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings);

        localNode = newDiscoveryNode("local-node");
        final TransportService transportService = new TransportService(settings, capturingTransport,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        lastAcceptedNodes = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build();

        peerFinder = new TestPeerFinder(settings, transportService, unicastHostsProvider,
            deterministicTaskQueue.getFutureExecutor(), transportAddressConnector, deterministicTaskQueue::getExecutorService);

        peerFinder.start();
    }

    @After
    public void deactivateAndRunRemainingTasks() {
        peerFinder.deactivate();
        deterministicTaskQueue.runAllTasks(); // termination ensures that everything is properly cleaned up
    }

    public void testActivationAndDeactivation() {
        peerFinder.activate(lastAcceptedNodes);
        assertFoundPeers();
        assertTrue(peerFinder.isActive());

        peerFinder.deactivate();
        assertFalse(peerFinder.isActive());
    }

    public void testAddsReachableNodesFromUnicastHostsList() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDoesNotAddUnreachableNodesFromUnicastHostsList() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.unreachableAddresses.add(otherNode.getAddress());

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers();
    }

    public void testDoesNotAddNonMasterEligibleNodesFromUnicastHostsList() {
        final DiscoveryNode nonMasterNode = new DiscoveryNode("node-from-hosts-list", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);

        unicastHostsProvider.providedAddresses.add(nonMasterNode.getAddress());
        transportAddressConnector.reachableNodes.add(nonMasterNode);

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
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testDeactivationClearsPastKnowledge() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        peerFinder.deactivate();

        unicastHostsProvider.providedAddresses.clear();
        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();
        assertFoundPeers();
    }

    public void testAddsReachableNodesFromClusterState() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-in-cluster-state");
        updateLastAcceptedNodes(b -> b.add(otherNode));
        transportAddressConnector.reachableNodes.add(otherNode);

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

        transportAddressConnector.reachableNodes.add(sourceNode);
        transportAddressConnector.reachableNodes.add(otherKnownNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(sourceNode, otherKnownNode);
    }

    public void testDoesNotAddUnreachableNodesFromIncomingRequests() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        final DiscoveryNode otherKnownNode = newDiscoveryNode("other-known-node");

        transportAddressConnector.reachableNodes.add(sourceNode);
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
        transportAddressConnector.reachableNodes.add(otherKnownNode);

        peerFinder.activate(lastAcceptedNodes);
        peerFinder.handlePeersRequest(new PeersRequest(sourceNode, Collections.singletonList(otherKnownNode)));
        runAllRunnableTasks();

        assertFoundPeers(otherKnownNode);
    }

    public void testRequestsPeersIncludingKnownPeersInRequest() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        final CapturedRequest[] capturedRequests = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, is(1));
        final PeersRequest peersRequest = (PeersRequest) capturedRequests[0].request;
        assertThat(peersRequest.getKnownPeers(), contains(otherNode));
    }

    public void testAddsReachablePeersFromResponse() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        final DiscoveryNode discoveredNode = newDiscoveryNode("discovered-node");
        transportAddressConnector.reachableNodes.add(discoveredNode);
        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.empty(), singletonList(discoveredNode), randomNonNegativeLong());
        });

        runAllRunnableTasks();
        assertFoundPeers(otherNode, discoveredNode);
    }

    public void testAddsReachableMasterFromResponse() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
        final DiscoveryNode discoveredMaster = newDiscoveryNode("discovered-master");

        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.of(discoveredMaster), emptyList(), randomNonNegativeLong());
        });

        transportAddressConnector.reachableNodes.add(discoveredMaster);
        runAllRunnableTasks();
        assertFoundPeers(otherNode, discoveredMaster);
        assertThat(peerFinder.discoveredMasterNode, nullValue());
    }

    public void testHandlesDiscoveryOfMasterFromResponseFromMaster() {
        final DiscoveryNode otherNode = newDiscoveryNode("node-from-hosts-list");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        respondToRequests(node -> {
            assertThat(node, is(otherNode));
            return new PeersResponse(Optional.of(otherNode), emptyList(), randomNonNegativeLong());
        });

        runAllRunnableTasks();
        assertFoundPeers(otherNode);
        assertThat(peerFinder.discoveredMasterNode, is(otherNode));
    }

    public void testOnlyRequestsPeersOncePerRoundButDoesRetryNextRound() {
        final DiscoveryNode sourceNode = newDiscoveryNode("request-source");
        transportAddressConnector.reachableNodes.add(sourceNode);

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
        respondToRequests(node -> {
            throw new AssertionError("there should have been no further requests");
        });

        final DiscoveryNode otherNode = newDiscoveryNode("otherNode");
        transportAddressConnector.reachableNodes.add(otherNode);

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
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        transportAddressConnector.reachableNodes.clear();
        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(otherNode);
    }

    public void testReconnectsToDisconnectedNodes() {
        final DiscoveryNode otherNode = newDiscoveryNode("original-node");
        unicastHostsProvider.providedAddresses.add(otherNode.getAddress());
        transportAddressConnector.reachableNodes.add(otherNode);

        peerFinder.activate(lastAcceptedNodes);
        runAllRunnableTasks();

        assertFoundPeers(otherNode);

        transportAddressConnector.reachableNodes.clear();
        final DiscoveryNode rebootedOtherNode = new DiscoveryNode("rebooted-node", otherNode.getAddress(), Version.CURRENT);
        transportAddressConnector.reachableNodes.add(rebootedOtherNode);

        connectedNodes.remove(otherNode);
        disconnectedNodes.add(otherNode);

        deterministicTaskQueue.advanceTime();
        runAllRunnableTasks();

        assertFoundPeers(otherNode, rebootedOtherNode);
    }

    private void respondToRequests(Function<DiscoveryNode, PeersResponse> responseFactory) {
        final CapturedRequest[] capturedRequests = capturingTransport.getCapturedRequestsAndClear();
        for (final CapturedRequest capturedRequest : capturedRequests) {
            assertThat(capturedRequest.action, is(REQUEST_PEERS_ACTION_NAME));
            assertThat(capturedRequest.request, instanceOf(PeersRequest.class));
            final PeersRequest peersRequest = (PeersRequest) capturedRequest.request;
            assertThat(peersRequest.getSourceNode(), is(localNode));
            capturingTransport.handleResponse(capturedRequests[0].requestId, responseFactory.apply(capturedRequest.node));
        }
    }

    private void assertFoundPeers(DiscoveryNode... expectedNodes) {
        assertThat(peerFinder.getFoundPeers(),
            equalTo(Arrays.stream(expectedNodes).collect(Collectors.toSet())));
    }

    private DiscoveryNode newDiscoveryNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private void runAllRunnableTasks() {
        deterministicTaskQueue.runAllRunnableTasks(random());
    }
}

