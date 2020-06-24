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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.coordination.LeaderChecker.LeaderCheckRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.nullValue;

public class LeaderCheckerTests extends ESTestCase {

    public void testFollowerBehaviour() {
        final DiscoveryNode leader1 = new DiscoveryNode("leader-1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode leader2
            = randomBoolean() ? leader1 : new DiscoveryNode("leader-2", buildNewFakeTransportAddress(), Version.CURRENT);

        final DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        Settings.Builder settingsBuilder = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getId());

        final long leaderCheckIntervalMillis;
        if (randomBoolean()) {
            leaderCheckIntervalMillis = randomLongBetween(1000, 60000);
            settingsBuilder.put(LEADER_CHECK_INTERVAL_SETTING.getKey(), leaderCheckIntervalMillis + "ms");
        } else {
            leaderCheckIntervalMillis = LEADER_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        }

        final long leaderCheckTimeoutMillis;
        if (randomBoolean()) {
            leaderCheckTimeoutMillis = randomLongBetween(1, 60000);
            settingsBuilder.put(LEADER_CHECK_TIMEOUT_SETTING.getKey(), leaderCheckTimeoutMillis + "ms");
        } else {
            leaderCheckTimeoutMillis = LEADER_CHECK_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        }

        final int leaderCheckRetryCount;
        if (randomBoolean()) {
            leaderCheckRetryCount = randomIntBetween(1, 10);
            settingsBuilder.put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), leaderCheckRetryCount);
        } else {
            leaderCheckRetryCount = LEADER_CHECK_RETRY_COUNT_SETTING.get(Settings.EMPTY);
        }

        final AtomicLong checkCount = new AtomicLong();
        final AtomicBoolean allResponsesFail = new AtomicBoolean();

        final Settings settings = settingsBuilder.build();
        logger.info("--> using {}", settings);

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final MockTransport mockTransport = new MockTransport() {

            int consecutiveFailedRequestsCount;

            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertThat(action, equalTo(LEADER_CHECK_ACTION_NAME));
                assertTrue(node.equals(leader1) || node.equals(leader2));
                super.onSendRequest(requestId, action, request, node);

                final boolean mustSucceed = leaderCheckRetryCount - 1 <= consecutiveFailedRequestsCount;
                final long responseDelay = randomLongBetween(0, leaderCheckTimeoutMillis + (mustSucceed ? -1 : 60000));
                final boolean successResponse = allResponsesFail.get() == false && (mustSucceed || randomBoolean());

                if (responseDelay >= leaderCheckTimeoutMillis || successResponse == false) {
                    consecutiveFailedRequestsCount += 1;
                } else {
                    consecutiveFailedRequestsCount = 0;
                }

                checkCount.incrementAndGet();

                deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + responseDelay, new Runnable() {
                    @Override
                    public void run() {
                        if (successResponse) {
                            handleResponse(requestId, Empty.INSTANCE);
                        } else {
                            handleRemoteError(requestId, new ElasticsearchException("simulated error"));
                        }
                    }

                    @Override
                    public String toString() {
                        return (successResponse ? "successful" : "unsuccessful") + " response to request " + requestId;
                    }
                });
            }
        };

        final TransportService transportService = mockTransport.createTransportService(settings,
            deterministicTaskQueue.getThreadPool(), NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean leaderFailed = new AtomicBoolean();

        final LeaderChecker leaderChecker = new LeaderChecker(settings, transportService,
            e -> {
                assertThat(e.getMessage(), matchesRegex("node \\[.*\\] failed \\[[1-9][0-9]*\\] consecutive checks"));
                assertTrue(leaderFailed.compareAndSet(false, true));
            }, () -> new StatusInfo(StatusInfo.Status.HEALTHY, "healthy-info"));

        logger.info("--> creating first checker");
        leaderChecker.updateLeader(leader1);
        {
            final long maxCheckCount = randomLongBetween(2, 1000);
            logger.info("--> checking that no failure is detected in {} checks", maxCheckCount);
            while (checkCount.get() < maxCheckCount) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }
        }
        leaderChecker.updateLeader(null);

        logger.info("--> running remaining tasks");
        deterministicTaskQueue.runAllTasks();
        assertFalse(leaderFailed.get());

        logger.info("--> creating second checker");
        leaderChecker.updateLeader(leader2);
        {
            checkCount.set(0);
            final long maxCheckCount = randomLongBetween(2, 1000);
            logger.info("--> checking again that no failure is detected in {} checks", maxCheckCount);
            while (checkCount.get() < maxCheckCount) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }

            deterministicTaskQueue.runAllRunnableTasks();

            final long failureTime = deterministicTaskQueue.getCurrentTimeMillis();
            allResponsesFail.set(true);
            logger.info("--> failing at {}ms", failureTime);

            while (leaderFailed.get() == false) {
                deterministicTaskQueue.advanceTime();
                deterministicTaskQueue.runAllRunnableTasks();
            }

            assertThat(deterministicTaskQueue.getCurrentTimeMillis() - failureTime,
                lessThanOrEqualTo((leaderCheckIntervalMillis + leaderCheckTimeoutMillis) * leaderCheckRetryCount
                    // needed because a successful check response might be in flight at the time of failure
                    + leaderCheckTimeoutMillis
                ));
        }
        leaderChecker.updateLeader(null);
    }

    enum Response {
        SUCCESS, REMOTE_ERROR, DIRECT_ERROR
    }

    public void testFollowerFailsImmediatelyOnDisconnection() {
        final DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode leader = new DiscoveryNode("leader", buildNewFakeTransportAddress(), Version.CURRENT);

        final Response[] responseHolder = new Response[]{Response.SUCCESS};

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getId()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(node, ClusterName.DEFAULT, Version.CURRENT));
                    return;
                }
                assertThat(action, equalTo(LEADER_CHECK_ACTION_NAME));
                assertEquals(node, leader);
                final Response response = responseHolder[0];

                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        switch (response) {
                            case SUCCESS:
                                handleResponse(requestId, Empty.INSTANCE);
                                break;
                            case REMOTE_ERROR:
                                handleRemoteError(requestId, new ConnectTransportException(leader, "simulated error"));
                                break;
                            case DIRECT_ERROR:
                                handleError(requestId, new ConnectTransportException(leader, "simulated error"));
                        }
                    }

                    @Override
                    public String toString() {
                        return response + " response to request " + requestId;
                    }
                });
            }
        };

        final TransportService transportService = mockTransport.createTransportService(settings,
            deterministicTaskQueue.getThreadPool(), NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean leaderFailed = new AtomicBoolean();
        final LeaderChecker leaderChecker = new LeaderChecker(settings, transportService,
            e -> {
                assertThat(e.getMessage(), anyOf(endsWith("disconnected"), endsWith("disconnected during check")));
                assertTrue(leaderFailed.compareAndSet(false, true));
            }, () -> new StatusInfo(StatusInfo.Status.HEALTHY, "healthy-info"));

        leaderChecker.updateLeader(leader);
        {
            while (deterministicTaskQueue.getCurrentTimeMillis() < 10 * LEADER_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).millis()) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }

            deterministicTaskQueue.runAllRunnableTasks();
            assertFalse(leaderFailed.get());

            responseHolder[0] = Response.REMOTE_ERROR;

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            assertTrue(leaderFailed.get());
        }
        leaderChecker.updateLeader(null);

        deterministicTaskQueue.runAllTasks();
        leaderFailed.set(false);
        responseHolder[0] = Response.SUCCESS;

        leaderChecker.updateLeader(leader);
        {
            while (deterministicTaskQueue.getCurrentTimeMillis() < 10 * LEADER_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).millis()) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }

            deterministicTaskQueue.runAllRunnableTasks();
            assertFalse(leaderFailed.get());

            responseHolder[0] = Response.DIRECT_ERROR;

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            assertTrue(leaderFailed.get());
        }

        deterministicTaskQueue.runAllTasks();
        leaderFailed.set(false);
        responseHolder[0] = Response.SUCCESS;

        leaderChecker.updateLeader(leader);
        {
            // need to connect first for disconnect to have any effect
            AbstractSimpleTransportTestCase.connectToNode(transportService, leader);

            transportService.disconnectFromNode(leader);
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(leaderFailed.get());
        }
    }

    public void testFollowerFailsImmediatelyOnHealthCheckFailure() {
        final DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode leader = new DiscoveryNode("leader", buildNewFakeTransportAddress(), Version.CURRENT);

        final Response[] responseHolder = new Response[]{Response.SUCCESS};

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getId()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(node, ClusterName.DEFAULT, Version.CURRENT));
                    return;
                }
                assertThat(action, equalTo(LEADER_CHECK_ACTION_NAME));
                assertEquals(node, leader);
                final Response response = responseHolder[0];

                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        switch (response) {
                            case SUCCESS:
                                handleResponse(requestId, Empty.INSTANCE);
                                break;
                            case REMOTE_ERROR:
                                handleRemoteError(requestId, new NodeHealthCheckFailureException("simulated error"));
                                break;
                        }
                    }

                    @Override
                    public String toString() {
                        return response + " response to request " + requestId;
                    }
                });
            }
        };

        final TransportService transportService = mockTransport.createTransportService(settings,
            deterministicTaskQueue.getThreadPool(), NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean leaderFailed = new AtomicBoolean();
        final LeaderChecker leaderChecker = new LeaderChecker(settings, transportService,
            e -> {
                assertThat(e.getMessage(), endsWith("failed health checks"));
                assertTrue(leaderFailed.compareAndSet(false, true));
            }, () -> new StatusInfo(StatusInfo.Status.HEALTHY, "healthy-info"));

        leaderChecker.updateLeader(leader);

        {
            while (deterministicTaskQueue.getCurrentTimeMillis() < 10 * LEADER_CHECK_INTERVAL_SETTING.get(Settings.EMPTY).millis()) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }

            deterministicTaskQueue.runAllRunnableTasks();
            assertFalse(leaderFailed.get());

            responseHolder[0] = Response.REMOTE_ERROR;

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();

            assertTrue(leaderFailed.get());
        }
    }

    public void testLeaderBehaviour() {
        final DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getId()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final CapturingTransport capturingTransport = new CapturingTransport();
        AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>(new StatusInfo(UNHEALTHY, "unhealthy-info"));

        final TransportService transportService = capturingTransport.createTransportService(settings,
            deterministicTaskQueue.getThreadPool(), NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        final LeaderChecker leaderChecker = new LeaderChecker(settings, transportService, e -> fail("shouldn't be checking anything"),
            () -> nodeHealthServiceStatus.get());

        final DiscoveryNodes discoveryNodes
            = DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()).build();

        {
            leaderChecker.setCurrentNodes(discoveryNodes);

            final CapturingTransportResponseHandler handler = new CapturingTransportResponseHandler();
            transportService.sendRequest(localNode, LEADER_CHECK_ACTION_NAME, new LeaderCheckRequest(otherNode), handler);
            deterministicTaskQueue.runAllTasks();

            assertFalse(handler.successfulResponseReceived);
            assertThat(handler.transportException.getRootCause(), instanceOf(NodeHealthCheckFailureException.class));
            NodeHealthCheckFailureException cause = (NodeHealthCheckFailureException) handler.transportException.getRootCause();
            assertThat(cause.getMessage(), equalTo("rejecting leader check from [" + otherNode
                + "] since node is unhealthy [unhealthy-info]"));
        }

        nodeHealthServiceStatus.getAndSet(new StatusInfo(HEALTHY, "healthy-info"));
        {
            leaderChecker.setCurrentNodes(discoveryNodes);

            final CapturingTransportResponseHandler handler = new CapturingTransportResponseHandler();
            transportService.sendRequest(localNode, LEADER_CHECK_ACTION_NAME, new LeaderCheckRequest(otherNode), handler);
            deterministicTaskQueue.runAllTasks();

            assertFalse(handler.successfulResponseReceived);
            assertThat(handler.transportException.getRootCause(), instanceOf(CoordinationStateRejectedException.class));
            CoordinationStateRejectedException cause = (CoordinationStateRejectedException) handler.transportException.getRootCause();
            assertThat(cause.getMessage(), equalTo("rejecting leader check since [" + otherNode + "] has been removed from the cluster"));
        }

        {
            leaderChecker.setCurrentNodes(DiscoveryNodes.builder(discoveryNodes).add(otherNode).build());

            final CapturingTransportResponseHandler handler = new CapturingTransportResponseHandler();
            transportService.sendRequest(localNode, LEADER_CHECK_ACTION_NAME, new LeaderCheckRequest(otherNode), handler);
            deterministicTaskQueue.runAllTasks();

            assertTrue(handler.successfulResponseReceived);
            assertThat(handler.transportException, nullValue());
        }

        {
            leaderChecker.setCurrentNodes(DiscoveryNodes.builder(discoveryNodes).add(otherNode).masterNodeId(null).build());

            final CapturingTransportResponseHandler handler = new CapturingTransportResponseHandler();
            transportService.sendRequest(localNode, LEADER_CHECK_ACTION_NAME, new LeaderCheckRequest(otherNode), handler);
            deterministicTaskQueue.runAllTasks();

            assertFalse(handler.successfulResponseReceived);
            assertThat(handler.transportException.getRootCause(), instanceOf(CoordinationStateRejectedException.class));
            CoordinationStateRejectedException cause = (CoordinationStateRejectedException) handler.transportException.getRootCause();
            assertThat(cause.getMessage(),
                equalTo("rejecting leader check from [" + otherNode + "] sent to a node that is no longer the master"));
        }
    }

    private class CapturingTransportResponseHandler implements TransportResponseHandler<Empty> {

        TransportException transportException;
        boolean successfulResponseReceived;

        @Override
        public void handleResponse(Empty response) {
            successfulResponseReceived = true;
        }

        @Override
        public void handleException(TransportException exp) {
            transportException = exp;
        }

        @Override
        public String executor() {
            return Names.GENERIC;
        }

        @Override
        public TransportResponse.Empty read(StreamInput in) {
            return TransportResponse.Empty.INSTANCE;
        }
    }

    public void testLeaderCheckRequestEqualsHashcodeSerialization() {
        LeaderCheckRequest request = new LeaderCheckRequest(
            new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT));
        //noinspection RedundantCast since it is needed for some IDEs (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(request,
                (CopyFunction<LeaderCheckRequest>) rq -> copyWriteable(rq, writableRegistry(), LeaderCheckRequest::new),
            rq -> new LeaderCheckRequest(new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT)));
    }
}
