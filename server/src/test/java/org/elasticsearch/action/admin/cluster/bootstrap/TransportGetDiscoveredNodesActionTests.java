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

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.NoOpClusterApplier;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.PeersRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportService.HandshakeResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.discovery.PeerFinder.REQUEST_PEERS_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportGetDiscoveredNodesActionTests extends ESTestCase {

    private static final ActionFilters EMPTY_FILTERS = new ActionFilters(emptySet());

    private static ThreadPool threadPool;
    private DiscoveryNode localNode;
    private String clusterName;
    private TransportService transportService;
    private Coordinator coordinator;
    private DiscoveryNode otherNode;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
    }

    @AfterClass
    public static void shutdownThreadPool() {
        threadPool.shutdown();
    }

    @Before
    public void setupTest() {
        clusterName = randomAlphaOfLength(10);
        localNode = new DiscoveryNode(
            "node1", "local", buildNewFakeTransportAddress(), emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
        otherNode = new DiscoveryNode(
            "node2", "other", buildNewFakeTransportAddress(), emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(HANDSHAKE_ACTION_NAME) && node.getAddress().equals(otherNode.getAddress())) {
                    handleResponse(requestId, new HandshakeResponse(otherNode, new ClusterName(clusterName), Version.CURRENT));
                }
            }
        };
        transportService = transport.createTransportService(
            Settings.builder().put(CLUSTER_NAME_SETTING.getKey(), clusterName).build(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        coordinator = new Coordinator("local", Settings.EMPTY, clusterSettings, transportService, writableRegistry(),
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            new MasterService("local", Settings.EMPTY, threadPool),
            () -> new InMemoryPersistedState(0, ClusterState.builder(new ClusterName(clusterName)).build()), r -> emptyList(),
            new NoOpClusterApplier(), new Random(random().nextLong()));
    }

    public void testHandlesNonstandardDiscoveryImplementation() throws InterruptedException {
        final Discovery discovery = mock(Discovery.class);
        verifyZeroInteractions(discovery);

        new TransportGetDiscoveredNodesAction(Settings.EMPTY, EMPTY_FILTERS, transportService, discovery); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, new GetDiscoveredNodesRequest(), new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                final Throwable rootCause = exp.getRootCause();
                assertThat(rootCause, instanceOf(IllegalArgumentException.class));
                assertThat(rootCause.getMessage(), equalTo("discovered nodes are not exposed by discovery type [zen]"));
                countDownLatch.countDown();
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    public void testFailsOnNonMasterEligibleNodes() throws InterruptedException {
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        // transport service only picks up local node when started, so we can change it here ^

        new TransportGetDiscoveredNodesAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, new GetDiscoveredNodesRequest(), new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                final Throwable rootCause = exp.getRootCause();
                assertThat(rootCause, instanceOf(IllegalArgumentException.class));
                assertThat(rootCause.getMessage(),
                    equalTo("this node is not master-eligible, but discovered nodes are only exposed by master-eligible nodes"));
                countDownLatch.countDown();
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    public void testFailsQuicklyWithZeroTimeoutAndAcceptsNullTimeout() throws InterruptedException {
        new TransportGetDiscoveredNodesAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();
        coordinator.startInitialJoin();

        {
            final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
            getDiscoveredNodesRequest.setWaitForNodes(2);
            getDiscoveredNodesRequest.setTimeout(null);
            transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
                @Override
                public void handleResponse(GetDiscoveredNodesResponse response) {
                    throw new AssertionError("should not be called");
                }

                @Override
                public void handleException(TransportException exp) {
                    throw new AssertionError("should not be called", exp);
                }
            });
        }

        {
            final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
            getDiscoveredNodesRequest.setWaitForNodes(2);
            getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);

            final CountDownLatch countDownLatch = new CountDownLatch(1);
            transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
                @Override
                public void handleResponse(GetDiscoveredNodesResponse response) {
                    throw new AssertionError("should not be called");
                }

                @Override
                public void handleException(TransportException exp) {
                    final Throwable rootCause = exp.getRootCause();
                    assertThat(rootCause, instanceOf(ElasticsearchTimeoutException.class));
                    assertThat(rootCause.getMessage(), startsWith("timed out while waiting for GetDiscoveredNodesRequest{"));
                    countDownLatch.countDown();
                }
            });

            assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        }
    }

    public void testGetsDiscoveredNodesWithZeroTimeout() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setWaitForNodes(2);
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionMet(getDiscoveredNodesRequest);
    }

    public void testGetsDiscoveredNodesByAddress() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setRequiredNodes(Arrays.asList(localNode.getAddress().toString(), otherNode.getAddress().toString()));
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionMet(getDiscoveredNodesRequest);
    }

    public void testGetsDiscoveredNodesByName() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setRequiredNodes(Arrays.asList(localNode.getName(), otherNode.getName()));
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionMet(getDiscoveredNodesRequest);
    }

    public void testGetsDiscoveredNodesByIP() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        String ip = localNode.getAddress().getAddress();
        getDiscoveredNodesRequest.setRequiredNodes(Collections.singletonList(ip));
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionFailedOnDuplicate(getDiscoveredNodesRequest, '[' + ip + "] matches [");
    }

    public void testGetsDiscoveredNodesDuplicateName() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        String name = localNode.getName();
        getDiscoveredNodesRequest.setRequiredNodes(Arrays.asList(name, name));
        getDiscoveredNodesRequest.setWaitForNodes(1);
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionFailedOnDuplicate(getDiscoveredNodesRequest, "[" + localNode + "] matches [" + name + ", " + name + ']');
    }

    public void testGetsDiscoveredNodesWithDuplicateMatchNameAndAddress() throws InterruptedException {
        setupGetDiscoveredNodesAction();
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setRequiredNodes(Arrays.asList(localNode.getAddress().toString(), localNode.getName()));
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        assertWaitConditionFailedOnDuplicate(getDiscoveredNodesRequest, "[" + localNode + "] matches [");
    }

    public void testGetsDiscoveredNodesTimeoutOnMissing() throws InterruptedException {
        setupGetDiscoveredNodesAction();

        final CountDownLatch latch = new CountDownLatch(1);
        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setRequiredNodes(Arrays.asList(localNode.getAddress().toString(), "_missing"));
        getDiscoveredNodesRequest.setWaitForNodes(1);
        getDiscoveredNodesRequest.setTimeout(TimeValue.ZERO);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                assertThat(exp.getRootCause(), instanceOf(ElasticsearchTimeoutException.class));
                latch.countDown();
            }
        });

        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testThrowsExceptionIfDuplicateDiscoveredLater() throws InterruptedException {
        new TransportGetDiscoveredNodesAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();
        coordinator.startInitialJoin();

        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        final String ip = localNode.getAddress().getAddress();
        getDiscoveredNodesRequest.setRequiredNodes(Collections.singletonList(ip));
        getDiscoveredNodesRequest.setWaitForNodes(2);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                Throwable t = exp.getRootCause();
                assertThat(t, instanceOf(IllegalArgumentException.class));
                assertThat(t.getMessage(), startsWith('[' + ip + "] matches ["));
                countDownLatch.countDown();
            }
        });

        executeRequestPeersAction();
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    private void executeRequestPeersAction() {
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
    }

    private void setupGetDiscoveredNodesAction() throws InterruptedException {
        new TransportGetDiscoveredNodesAction(Settings.EMPTY, EMPTY_FILTERS, transportService, coordinator); // registers action
        transportService.start();
        transportService.acceptIncomingRequests();
        coordinator.start();
        coordinator.startInitialJoin();

        executeRequestPeersAction();

        final GetDiscoveredNodesRequest getDiscoveredNodesRequest = new GetDiscoveredNodesRequest();
        getDiscoveredNodesRequest.setWaitForNodes(2);
        assertWaitConditionMet(getDiscoveredNodesRequest);
    }

    private void assertWaitConditionMet(GetDiscoveredNodesRequest getDiscoveredNodesRequest) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                assertThat(response.getNodes(), containsInAnyOrder(localNode, otherNode));
                countDownLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                throw new AssertionError("should not be called", exp);
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    private void assertWaitConditionFailedOnDuplicate(GetDiscoveredNodesRequest getDiscoveredNodesRequest, String message)
        throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(localNode, GetDiscoveredNodesAction.NAME, getDiscoveredNodesRequest, new ResponseHandler() {
            @Override
            public void handleResponse(GetDiscoveredNodesResponse response) {
                throw new AssertionError("should not be called");
            }

            @Override
            public void handleException(TransportException exp) {
                Throwable t = exp.getRootCause();
                assertThat(t, instanceOf(IllegalArgumentException.class));
                assertThat(t.getMessage(), startsWith(message));
                countDownLatch.countDown();
            }
        });

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }

    private abstract class ResponseHandler implements TransportResponseHandler<GetDiscoveredNodesResponse> {
        @Override
        public String executor() {
            return Names.SAME;
        }

        @Override
        public GetDiscoveredNodesResponse read(StreamInput in) throws IOException {
            return new GetDiscoveredNodesResponse(in);
        }
    }
}
