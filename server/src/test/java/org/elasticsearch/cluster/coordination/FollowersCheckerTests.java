/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_ACTION_NAME;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class FollowersCheckerTests extends ESTestCase {

    public void testChecksExpectedNodes() {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("local-node");
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getName()).build();

        final DiscoveryNodes[] discoveryNodesHolder = new DiscoveryNodes[] {
            DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build() };

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final Set<DiscoveryNode> checkedNodes = new HashSet<>();
        final AtomicInteger checkCount = new AtomicInteger();

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertThat(action, equalTo(FOLLOWER_CHECK_ACTION_NAME));
                assertThat(request, instanceOf(FollowerCheckRequest.class));
                assertTrue(discoveryNodesHolder[0].nodeExists(node));
                assertThat(node, not(equalTo(localNode)));
                checkedNodes.add(node);
                checkCount.incrementAndGet();
                handleResponse(requestId, Empty.INSTANCE);
            }
        };

        final TransportService transportService = mockTransport.createTransportService(
            settings,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        final FollowersChecker followersChecker = new FollowersChecker(
            settings,
            transportService,
            fcr -> { assert false : fcr; },
            (node, reason) -> {
                assert false : node;
            },
            () -> new StatusInfo(StatusInfo.Status.HEALTHY, "healthy-info")
        );

        followersChecker.setCurrentNodes(discoveryNodesHolder[0]);
        deterministicTaskQueue.runAllTasks();

        assertThat(checkedNodes, empty());
        assertThat(followersChecker.getFaultyNodes(), empty());

        final DiscoveryNode otherNode1 = DiscoveryNodeUtils.create("other-node-1");
        followersChecker.setCurrentNodes(discoveryNodesHolder[0] = DiscoveryNodes.builder(discoveryNodesHolder[0]).add(otherNode1).build());
        while (checkCount.get() < 10) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(checkedNodes, contains(otherNode1));
        assertThat(followersChecker.getFaultyNodes(), empty());

        checkedNodes.clear();
        checkCount.set(0);
        final DiscoveryNode otherNode2 = DiscoveryNodeUtils.create("other-node-2");
        followersChecker.setCurrentNodes(discoveryNodesHolder[0] = DiscoveryNodes.builder(discoveryNodesHolder[0]).add(otherNode2).build());
        while (checkCount.get() < 10) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(checkedNodes, containsInAnyOrder(otherNode1, otherNode2));
        assertThat(followersChecker.getFaultyNodes(), empty());

        checkedNodes.clear();
        checkCount.set(0);
        followersChecker.setCurrentNodes(
            discoveryNodesHolder[0] = DiscoveryNodes.builder(discoveryNodesHolder[0]).remove(otherNode1).build()
        );
        while (checkCount.get() < 10) {
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(checkedNodes, contains(otherNode2));
        assertThat(followersChecker.getFaultyNodes(), empty());

        checkedNodes.clear();
        followersChecker.clearCurrentNodes();
        deterministicTaskQueue.runAllTasks();
        assertThat(checkedNodes, empty());
    }

    public void testFailsNodeThatDoesNotRespond() {
        final Settings settings = randomSettings();
        testBehaviourOfFailingNode(
            settings,
            () -> null,
            "followers check retry count exceeded [timeouts=" + FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings) + ", failures=0]",
            (FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings) - 1) * FOLLOWER_CHECK_INTERVAL_SETTING.get(settings).millis()
                + FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings) * FOLLOWER_CHECK_TIMEOUT_SETTING.get(settings).millis(),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testFailsNodeThatRejectsCheck() {
        final Settings settings = randomSettings();
        testBehaviourOfFailingNode(
            settings,
            () -> { throw new ElasticsearchException("simulated exception"); },
            "followers check retry count exceeded [timeouts=0, failures=" + FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings) + "]",
            (FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings) - 1) * FOLLOWER_CHECK_INTERVAL_SETTING.get(settings).millis(),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testFailsNodeThatRejectsCheckAndDoesNotRespond() {
        final Settings settings = randomSettings();
        final int retryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);
        final int timeoutCount = between(0, retryCount);
        final int failureCount = retryCount - timeoutCount;

        testBehaviourOfFailingNode(settings, new Supplier<Empty>() {

            private int timeoutsRemaining;
            private int failuresRemaining;

            @Override
            public Empty get() {
                if (timeoutsRemaining == 0 && failuresRemaining == 0) {
                    // node was added, reset counters
                    timeoutsRemaining = timeoutCount;
                    failuresRemaining = failureCount;
                }
                if (timeoutsRemaining == 0) {
                    assertThat(failuresRemaining--, greaterThan(0));
                    throw new ElasticsearchException("simulated exception");
                } else if (failuresRemaining == 0) {
                    assertThat(timeoutsRemaining--, greaterThan(0));
                    return null;
                } else if (randomBoolean()) {
                    assertThat(failuresRemaining--, greaterThan(0));
                    throw new ElasticsearchException("simulated exception");
                } else {
                    assertThat(timeoutsRemaining--, greaterThan(0));
                    return null;
                }
            }
        },
            "followers check retry count exceeded [timeouts=" + timeoutCount + ", failures=" + failureCount + "]",
            (retryCount - 1) * FOLLOWER_CHECK_INTERVAL_SETTING.get(settings).millis() + timeoutCount * FOLLOWER_CHECK_TIMEOUT_SETTING.get(
                settings
            ).millis(),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testFailureCounterResetsOnSuccess() {
        final Settings settings = randomSettings();
        final int retryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);
        final int maxRecoveries = randomIntBetween(3, 10);

        // passes just enough checks to keep it alive, up to maxRecoveries, and then fails completely
        testBehaviourOfFailingNode(settings, new Supplier<Empty>() {
            private int checkIndex;
            private int recoveries;

            @Override
            public Empty get() {
                checkIndex++;
                if (checkIndex % retryCount == 0 && recoveries < maxRecoveries) {
                    recoveries++;
                    return Empty.INSTANCE;
                }
                throw new ElasticsearchException("simulated exception");
            }
        },
            "followers check retry count exceeded [timeouts=0, failures=" + retryCount + "]",
            (retryCount * (maxRecoveries + 1) - 1) * FOLLOWER_CHECK_INTERVAL_SETTING.get(settings).millis(),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testTimeoutCounterResetsOnSuccess() {
        final Settings settings = randomSettings();
        final int retryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);
        final int maxRecoveries = randomIntBetween(3, 10);

        // passes just enough checks to keep it alive, up to maxRecoveries, and then fails completely
        testBehaviourOfFailingNode(settings, new Supplier<Empty>() {
            private int checkIndex;
            private int recoveries;

            @Override
            public Empty get() {
                checkIndex++;
                if (checkIndex % retryCount == 0 && recoveries < maxRecoveries) {
                    recoveries++;
                    return Empty.INSTANCE;
                }
                return null;
            }
        },
            "followers check retry count exceeded [timeouts=" + retryCount + ", failures=0]",
            (retryCount * (maxRecoveries + 1) - 1) * FOLLOWER_CHECK_INTERVAL_SETTING.get(settings).millis() + (retryCount * (maxRecoveries
                + 1) - maxRecoveries) * FOLLOWER_CHECK_TIMEOUT_SETTING.get(settings).millis(),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testFailsNodeThatIsDisconnected() {
        testBehaviourOfFailingNode(
            Settings.EMPTY,
            () -> { throw new ConnectTransportException(null, "simulated exception"); },
            "disconnected",
            0,
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    public void testFailsNodeThatDisconnects() {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("local-node");
        final DiscoveryNode otherNode = DiscoveryNodeUtils.create("other-node");
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getName()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertFalse(node.equals(localNode));
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(
                        requestId,
                        new TransportService.HandshakeResponse(Version.CURRENT, Build.CURRENT.hash(), node, ClusterName.DEFAULT)
                    );
                    return;
                }
                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        handleResponse(requestId, Empty.INSTANCE);
                    }

                    @Override
                    public String toString() {
                        return "sending response to [" + action + "][" + requestId + "] from " + node;
                    }
                });
            }
        };

        final TransportService transportService = mockTransport.createTransportService(
            settings,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean nodeFailed = new AtomicBoolean();

        final FollowersChecker followersChecker = new FollowersChecker(
            settings,
            transportService,
            fcr -> { assert false : fcr; },
            (node, reason) -> {
                assertTrue(nodeFailed.compareAndSet(false, true));
                assertThat(reason, equalTo("disconnected"));
            },
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build();
        followersChecker.setCurrentNodes(discoveryNodes);

        AbstractSimpleTransportTestCase.connectToNode(transportService, otherNode);
        transportService.disconnectFromNode(otherNode);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(nodeFailed.get());
        assertThat(followersChecker.getFaultyNodes(), contains(otherNode));
    }

    public void testFailsNodeThatIsUnhealthy() {
        testBehaviourOfFailingNode(
            randomSettings(),
            () -> { throw new NodeHealthCheckFailureException("non writable exception"); },
            "health check failed",
            0,
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
    }

    private void testBehaviourOfFailingNode(
        Settings testSettings,
        Supplier<TransportResponse.Empty> responder,
        String failureReason,
        long expectedFailureTime,
        NodeHealthService nodeHealthService
    ) {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("local-node");
        final DiscoveryNode otherNode = DiscoveryNodeUtils.create("other-node");
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getName()).put(testSettings).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertNotEquals(node, localNode);
                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        if (node.equals(otherNode) == false) {
                            // other nodes are ok
                            handleResponse(requestId, Empty.INSTANCE);
                            return;
                        }
                        try {
                            final Empty response = responder.get();
                            if (response != null) {
                                handleResponse(requestId, response);
                            }
                        } catch (Exception e) {
                            handleRemoteError(requestId, e);
                        }
                    }

                    @Override
                    public String toString() {
                        return "sending response to [" + action + "][" + requestId + "] from " + node;
                    }
                });
            }
        };

        final TransportService transportService = mockTransport.createTransportService(
            settings,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean nodeFailed = new AtomicBoolean();

        final FollowersChecker followersChecker = new FollowersChecker(
            settings,
            transportService,
            fcr -> { assert false : fcr; },
            (node, reason) -> {
                assertTrue(nodeFailed.compareAndSet(false, true));
                assertThat(reason, equalTo(failureReason));
            },
            nodeHealthService
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build();
        followersChecker.setCurrentNodes(discoveryNodes);
        while (nodeFailed.get() == false) {
            if (deterministicTaskQueue.hasRunnableTasks() == false) {
                deterministicTaskQueue.advanceTime();
            }
            deterministicTaskQueue.runAllRunnableTasks();
        }
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(expectedFailureTime));
        assertThat(followersChecker.getFaultyNodes(), contains(otherNode));

        deterministicTaskQueue.runAllTasks();

        // add another node and see that it schedules checks for this new node but keeps on considering the old one faulty
        final DiscoveryNode otherNode2 = DiscoveryNodeUtils.create("other-node-2");
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(otherNode2).build();
        followersChecker.setCurrentNodes(discoveryNodes);
        deterministicTaskQueue.runAllRunnableTasks();
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(followersChecker.getFaultyNodes(), contains(otherNode));

        // remove the faulty node and see that it is removed
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).remove(otherNode).build();
        followersChecker.setCurrentNodes(discoveryNodes);
        assertThat(followersChecker.getFaultyNodes(), empty());
        deterministicTaskQueue.runAllRunnableTasks();
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        // remove the working node and see that everything eventually stops
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).remove(otherNode2).build();
        followersChecker.setCurrentNodes(discoveryNodes);
        deterministicTaskQueue.runAllTasks();

        // add back the faulty node afresh and see that it fails again
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(otherNode).build();
        followersChecker.setCurrentNodes(discoveryNodes);
        nodeFailed.set(false);
        assertThat(followersChecker.getFaultyNodes(), empty());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(nodeFailed.get());
        assertThat(followersChecker.getFaultyNodes(), contains(otherNode));
    }

    public void testFollowerCheckRequestEqualsHashCodeSerialization() {
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new FollowerCheckRequest(randomNonNegativeLong(), DiscoveryNodeUtils.create(randomAlphaOfLength(10))),
            (CopyFunction<FollowerCheckRequest>) rq -> copyWriteable(rq, writableRegistry(), FollowerCheckRequest::new),
            rq -> {
                if (randomBoolean()) {
                    return new FollowerCheckRequest(rq.getTerm(), DiscoveryNodeUtils.create(randomAlphaOfLength(10)));
                } else {
                    return new FollowerCheckRequest(randomNonNegativeLong(), rq.getSender());
                }
            }
        );
    }

    public void testUnhealthyNodeRejectsImmediately() {

        final DiscoveryNode leader = DiscoveryNodeUtils.create("leader");
        final DiscoveryNode follower = DiscoveryNodeUtils.create("follower");
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), follower.getName()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("no requests expected");
            }
        };

        final TransportService transportService = mockTransport.createTransportService(
            settings,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> follower,
            null,
            emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        final AtomicBoolean calledCoordinator = new AtomicBoolean();
        final AtomicReference<RuntimeException> coordinatorException = new AtomicReference<>();

        final FollowersChecker followersChecker = new FollowersChecker(settings, transportService, fcr -> {
            assertTrue(calledCoordinator.compareAndSet(false, true));
            final RuntimeException exception = coordinatorException.get();
            if (exception != null) {
                throw exception;
            }
        }, (node, reason) -> { assert false : node; }, () -> new StatusInfo(UNHEALTHY, "unhealthy-info"));

        final long leaderTerm = randomLongBetween(2, Long.MAX_VALUE);
        final long followerTerm = randomLongBetween(1, leaderTerm - 1);
        followersChecker.updateFastResponseState(followerTerm, Mode.FOLLOWER);
        final AtomicReference<TransportException> receivedException = new AtomicReference<>();
        transportService.sendRequest(
            follower,
            FOLLOWER_CHECK_ACTION_NAME,
            new FollowerCheckRequest(leaderTerm, leader),
            new TransportResponseHandler.Empty() {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    fail("unexpected success");
                }

                @Override
                public void handleException(TransportException exp) {
                    assertThat(exp, not(nullValue()));
                    assertTrue(receivedException.compareAndSet(null, exp));
                }
            }
        );
        deterministicTaskQueue.runAllTasks();
        assertFalse(calledCoordinator.get());
        assertThat(receivedException.get(), not(nullValue()));
    }

    public void testResponder() {
        final DiscoveryNode leader = DiscoveryNodeUtils.create("leader");
        final DiscoveryNode follower = DiscoveryNodeUtils.create("follower");
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), follower.getName()).build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("no requests expected");
            }
        };

        final AtomicBoolean rejectExecution = new AtomicBoolean();

        final TransportService transportService = mockTransport.createTransportService(settings, deterministicTaskQueue.getThreadPool(r -> {
            if (rejectExecution.get()) {
                final var exception = new EsRejectedExecutionException("simulated rejection", true);
                if (r instanceof AbstractRunnable ar) {
                    ar.onRejection(exception);
                    return () -> {};
                } else {
                    throw exception;
                }
            } else {
                return r;
            }
        }), TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> follower, null, emptySet());

        final AtomicBoolean calledCoordinator = new AtomicBoolean();
        final AtomicReference<RuntimeException> coordinatorException = new AtomicReference<>();

        final FollowersChecker followersChecker = new FollowersChecker(settings, transportService, fcr -> {
            assertTrue(calledCoordinator.compareAndSet(false, true));
            final RuntimeException exception = coordinatorException.get();
            if (exception != null) {
                throw exception;
            }
        }, (node, reason) -> { assert false : node; }, () -> new StatusInfo(HEALTHY, "healthy-info"));

        transportService.start();
        transportService.acceptIncomingRequests();

        {
            // Does not call into the coordinator in the normal case
            final long term = randomNonNegativeLong();
            followersChecker.updateFastResponseState(term, Mode.FOLLOWER);

            final ExpectsSuccess expectsSuccess = new ExpectsSuccess();
            transportService.sendRequest(follower, FOLLOWER_CHECK_ACTION_NAME, new FollowerCheckRequest(term, leader), expectsSuccess);
            deterministicTaskQueue.runAllTasks();
            assertTrue(expectsSuccess.succeeded());
            assertFalse(calledCoordinator.get());
        }

        {
            // Does not call into the coordinator for a term that's too low, just rejects immediately
            final long leaderTerm = randomLongBetween(1, Long.MAX_VALUE - 1);
            final long followerTerm = randomLongBetween(leaderTerm + 1, Long.MAX_VALUE);
            followersChecker.updateFastResponseState(followerTerm, Mode.FOLLOWER);

            final AtomicReference<TransportException> receivedException = new AtomicReference<>();
            transportService.sendRequest(
                follower,
                FOLLOWER_CHECK_ACTION_NAME,
                new FollowerCheckRequest(leaderTerm, leader),
                new TransportResponseHandler.Empty() {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        fail("unexpected success");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, not(nullValue()));
                        assertTrue(receivedException.compareAndSet(null, exp));
                    }
                }
            );
            deterministicTaskQueue.runAllTasks();
            assertFalse(calledCoordinator.get());
            assertThat(receivedException.get(), not(nullValue()));
        }

        {
            // Calls into the coordinator if the term needs bumping
            final long leaderTerm = randomLongBetween(2, Long.MAX_VALUE);
            final long followerTerm = randomLongBetween(1, leaderTerm - 1);
            followersChecker.updateFastResponseState(followerTerm, Mode.FOLLOWER);

            final ExpectsSuccess expectsSuccess = new ExpectsSuccess();
            transportService.sendRequest(
                follower,
                FOLLOWER_CHECK_ACTION_NAME,
                new FollowerCheckRequest(leaderTerm, leader),
                expectsSuccess
            );
            deterministicTaskQueue.runAllTasks();
            assertTrue(expectsSuccess.succeeded());
            assertTrue(calledCoordinator.get());
            calledCoordinator.set(false);
        }

        {
            // Calls into the coordinator if not a follower
            final long term = randomNonNegativeLong();
            followersChecker.updateFastResponseState(term, randomFrom(Mode.LEADER, Mode.CANDIDATE));

            final ExpectsSuccess expectsSuccess = new ExpectsSuccess();
            transportService.sendRequest(follower, FOLLOWER_CHECK_ACTION_NAME, new FollowerCheckRequest(term, leader), expectsSuccess);
            deterministicTaskQueue.runAllTasks();
            assertTrue(expectsSuccess.succeeded());
            assertTrue(calledCoordinator.get());
            calledCoordinator.set(false);
        }

        {
            // If it calls into the coordinator and the coordinator throws an exception then it's passed back to the caller
            final long term = randomNonNegativeLong();
            followersChecker.updateFastResponseState(term, randomFrom(Mode.LEADER, Mode.CANDIDATE));
            final String exceptionMessage = "test simulated exception " + randomNonNegativeLong();
            coordinatorException.set(new ElasticsearchException(exceptionMessage));

            final AtomicReference<TransportException> receivedException = new AtomicReference<>();
            transportService.sendRequest(
                follower,
                FOLLOWER_CHECK_ACTION_NAME,
                new FollowerCheckRequest(term, leader),
                new TransportResponseHandler.Empty() {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        fail("unexpected success");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, not(nullValue()));
                        assertTrue(receivedException.compareAndSet(null, exp));
                    }
                }
            );
            deterministicTaskQueue.runAllTasks();
            assertTrue(calledCoordinator.get());
            assertThat(receivedException.get(), not(nullValue()));
            assertThat(receivedException.get().getRootCause().getMessage(), equalTo(exceptionMessage));
            calledCoordinator.set(false);
        }

        {
            // If it calls into the coordinator but the threadpool is shut down then the rejection is passed back to the caller
            rejectExecution.set(true);
            final long term = randomNonNegativeLong();
            followersChecker.updateFastResponseState(term, randomFrom(Mode.LEADER, Mode.CANDIDATE));

            final AtomicReference<TransportException> receivedException = new AtomicReference<>();
            transportService.sendRequest(
                follower,
                FOLLOWER_CHECK_ACTION_NAME,
                new FollowerCheckRequest(term, leader),
                new TransportResponseHandler.Empty() {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        fail("unexpected success");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, not(nullValue()));
                        assertTrue(receivedException.compareAndSet(null, exp));
                    }
                }
            );
            deterministicTaskQueue.runAllTasks();
            assertFalse(calledCoordinator.get());
            assertThat(receivedException.get(), not(nullValue()));
            assertThat(receivedException.get().getRootCause().getMessage(), equalTo("simulated rejection"));
        }
    }

    public void testPreferMasterNodes() {
        List<DiscoveryNode> nodes = randomNodes(10);
        DiscoveryNodes.Builder discoNodesBuilder = DiscoveryNodes.builder();
        nodes.forEach(dn -> discoNodesBuilder.add(dn));
        DiscoveryNodes discoveryNodes = discoNodesBuilder.localNodeId(nodes.get(0).getId()).build();
        CapturingTransport capturingTransport = new CapturingTransport();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> nodes.get(0),
            null,
            emptySet()
        );
        final FollowersChecker followersChecker = new FollowersChecker(
            Settings.EMPTY,
            transportService,
            fcr -> { assert false : fcr; },
            (node, reason) -> {
                assert false : node;
            },
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        followersChecker.setCurrentNodes(discoveryNodes);
        List<DiscoveryNode> followerTargets = Stream.of(capturingTransport.getCapturedRequestsAndClear()).map(cr -> cr.node()).toList();
        List<DiscoveryNode> sortedFollowerTargets = new ArrayList<>(followerTargets);
        Collections.sort(sortedFollowerTargets, Comparator.comparing(n -> n.isMasterNode() == false));
        assertEquals(sortedFollowerTargets, followerTargets);
    }

    private static List<DiscoveryNode> randomNodes(final int numNodes) {
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes, new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles())));
            nodesList.add(node);
        }
        return nodesList;
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.create("name_" + nodeId, "node_" + nodeId, buildNewFakeTransportAddress(), attributes, roles);
    }

    private static Settings randomSettings() {
        final Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            settingsBuilder.put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            settingsBuilder.put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), randomIntBetween(100, 100000) + "ms");
        }
        if (randomBoolean()) {
            settingsBuilder.put(FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), randomIntBetween(1, 100000) + "ms");
        }
        return settingsBuilder.build();
    }

    private static class ExpectsSuccess extends TransportResponseHandler.Empty {
        private final AtomicBoolean responseReceived = new AtomicBoolean();

        @Override
        public void handleResponse(TransportResponse.Empty response) {
            assertTrue(responseReceived.compareAndSet(false, true));
        }

        @Override
        public void handleException(TransportException exp) {
            throw new AssertionError("unexpected", exp);
        }

        public boolean succeeded() {
            return responseReceived.get();
        }

    }
}
