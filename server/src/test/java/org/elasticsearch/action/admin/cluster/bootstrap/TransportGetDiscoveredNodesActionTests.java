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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.NoOpClusterApplier;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.PeersRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportService.HandshakeResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.discovery.PeerFinder.REQUEST_PEERS_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class TransportGetDiscoveredNodesActionTests extends ESTestCase {
    public void testHandlesNonstandardDiscoveryImplementation() {
        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final DiscoveryNode discoveryNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());
        final Discovery discovery = new Discovery() {
            @Override
            public DiscoveryStats stats() {
                throw new AssertionError("should not be called");
            }

            @Override
            public void startInitialJoin() {
                throw new AssertionError("should not be called");
            }

            @Override
            public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
                throw new AssertionError("should not be called");
            }

            @Override
            public State lifecycleState() {
                throw new AssertionError("should not be called");
            }

            @Override
            public void addLifecycleListener(LifecycleListener listener) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void removeLifecycleListener(LifecycleListener listener) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void start() {
                throw new AssertionError("should not be called");
            }

            @Override
            public void stop() {
                throw new AssertionError("should not be called");
            }

            @Override
            public void close() {
                throw new AssertionError("should not be called");
            }
        };
        final TransportGetDiscoveredNodesAction transportGetDiscoveredNodesAction
            = new TransportGetDiscoveredNodesAction(Settings.EMPTY, mock(ActionFilters.class), transportService, discovery);

        final ActionListener<GetDiscoveredNodesResponse> listener = new ActionListener<GetDiscoveredNodesResponse>() {
            @Override
            public void onResponse(GetDiscoveredNodesResponse getDiscoveredNodesResponse) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called");
            }
        };

        assertThat(expectThrows(IllegalStateException.class,
            () -> transportGetDiscoveredNodesAction.doExecute(mock(Task.class), new GetDiscoveredNodesRequest(), listener))
            .getMessage(), equalTo("cannot execute a Zen2 action if not using Zen2"));

        threadPool.shutdown();
    }

    public void testFailsOnNonMasterEligibleNodes() {
        final DiscoveryNode discoveryNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> discoveryNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), random());
        coordinator.start();

        final TransportGetDiscoveredNodesAction transportGetDiscoveredNodesAction
            = new TransportGetDiscoveredNodesAction(Settings.EMPTY, mock(ActionFilters.class), transportService, coordinator);

        final ActionListener<GetDiscoveredNodesResponse> listener = new ActionListener<GetDiscoveredNodesResponse>() {
            @Override
            public void onResponse(GetDiscoveredNodesResponse getDiscoveredNodesResponse) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("should not be called");
            }
        };

        assertThat(expectThrows(ElasticsearchException.class, () -> transportGetDiscoveredNodesAction.doExecute(mock(Task.class),
            new GetDiscoveredNodesRequest(), listener)).getMessage(), equalTo("this node is not master-eligible"));

        threadPool.shutdown();
    }

    public void testFailsImmediatelyWithNoTimeout() {
        final DiscoveryNode localNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER), Version.CURRENT);

        final MockTransport transport = new MockTransport();
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), random());
        coordinator.start();
        coordinator.startInitialJoin();

        final TransportGetDiscoveredNodesAction transportGetDiscoveredNodesAction
            = new TransportGetDiscoveredNodesAction(Settings.EMPTY, mock(ActionFilters.class), transportService, coordinator);

        final AtomicBoolean responseReceived = new AtomicBoolean();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setWaitForNodes(2);
        transportGetDiscoveredNodesAction.doExecute(mock(Task.class), getDiscoveredNodesRequest,
            new ActionListener<GetDiscoveredNodesResponse>() {
                @Override
                public void onResponse(GetDiscoveredNodesResponse getDiscoveredNodesResponse) {
                    throw new AssertionError("should not be called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e.getMessage(), startsWith("timed out while waiting for "));
                    responseReceived.set(true);
                }
            });
        assertTrue(responseReceived.get());

        threadPool.shutdown();
    }

    @TestLogging("org.elasticsearch.cluster.coordination:TRACE")
    public void testGetsDiscoveredNodes() {
        final DiscoveryNode localNode
            = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER), Version.CURRENT);
        final DiscoveryNode otherNode
            = new DiscoveryNode("other", buildNewFakeTransportAddress(), emptyMap(), singleton(Role.MASTER), Version.CURRENT);
        final String clusterName = randomAlphaOfLength(10);

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(HANDSHAKE_ACTION_NAME) && node.getAddress().equals(otherNode.getAddress())) {
                    handleResponse(requestId, new HandshakeResponse(otherNode, new ClusterName(clusterName), Version.CURRENT));
                }
            }
        };
        final ThreadPool threadPool = new TestThreadPool("test", Settings.EMPTY);
        final TransportService transportService = transport.createTransportService(
            Settings.builder().put(CLUSTER_NAME_SETTING.getKey(), clusterName).build(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ClusterState state = ClusterState.builder(new ClusterName(clusterName)).build();
        final Coordinator coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, state), r -> emptyList(),
            new NoOpClusterApplier(), random());
        coordinator.start();
        coordinator.startInitialJoin();

        final TransportGetDiscoveredNodesAction transportGetDiscoveredNodesAction
            = new TransportGetDiscoveredNodesAction(Settings.EMPTY, mock(ActionFilters.class), transportService, coordinator);

        final AtomicBoolean responseReceived = new AtomicBoolean();

        threadPool.generic().execute(() ->
            transportService.sendRequest(localNode, REQUEST_PEERS_ACTION_NAME, new PeersRequest(otherNode, emptyList()),
                new TransportResponseHandler<PeersResponse>() {
                    @Override
                    public PeersResponse read(StreamInput in) throws IOException {
                        return new PeersResponse(in);
                    }

                    @Override
                    public void handleResponse(PeersResponse response) {
                    }

                    @Override
                    public void handleException(TransportException exp) {
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                }));

        final GetDiscoveredNodesRequest getDiscoveredNodesRequestWithTimeout = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequestWithTimeout.setTimeout(TimeValue.timeValueSeconds(60));
        getDiscoveredNodesRequestWithTimeout.setWaitForNodes(2);
        transportGetDiscoveredNodesAction.doExecute(mock(Task.class), getDiscoveredNodesRequestWithTimeout,
            new ActionListener<GetDiscoveredNodesResponse>() {
                @Override
                public void onResponse(GetDiscoveredNodesResponse getDiscoveredNodesResponse) {
                    assertThat(getDiscoveredNodesResponse.getNodes(), containsInAnyOrder(localNode, otherNode));
                    responseReceived.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("should not be called");
                }
            });
        assertTrue(responseReceived.get());

        responseReceived.set(false);
        final GetDiscoveredNodesRequest getDiscoveredNodesRequestWithoutTimeout = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequestWithoutTimeout.setWaitForNodes(2);
        transportGetDiscoveredNodesAction.doExecute(mock(Task.class), getDiscoveredNodesRequestWithoutTimeout,
            new ActionListener<GetDiscoveredNodesResponse>() {
                @Override
                public void onResponse(GetDiscoveredNodesResponse getDiscoveredNodesResponse) {
                    assertThat(getDiscoveredNodesResponse.getNodes(), containsInAnyOrder(localNode, otherNode));
                    responseReceived.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("should not be called");
                }
            });
        assertTrue(responseReceived.get());

        threadPool.shutdown();
    }
}
