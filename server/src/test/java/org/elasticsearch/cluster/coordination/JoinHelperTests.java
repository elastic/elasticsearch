/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.CapturingTransport.CapturedRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.coordination.JoinHelper.PENDING_JOIN_WAITING_RESPONSE;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class JoinHelperTests extends ESTestCase {

    public void testJoinDeduplication() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        CapturingTransport capturingTransport = new HandshakingCapturingTransport();
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node0");
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var taskManger = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            capturingTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            clusterSettings,
            new ClusterConnectionManager(Settings.EMPTY, capturingTransport, threadPool.getThreadContext()),
            taskManger,
            Tracer.NOOP
        );
        JoinHelper joinHelper = new JoinHelper(
            null,
            new MasterService(Settings.EMPTY, clusterSettings, threadPool, taskManger),
            new NoOpClusterApplier(),
            transportService,
            () -> 0L,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            (s, p, r) -> {},
            () -> new StatusInfo(HEALTHY, "info"),
            new JoinReasonService(() -> 0L),
            new NoneCircuitBreakerService(),
            Function.identity(),
            (listener, term) -> listener.onResponse(null),
            CompatibilityVersionsUtils.staticCurrent(),
            new FeatureService(List.of())
        );
        transportService.start();

        DiscoveryNode node1 = DiscoveryNodeUtils.create("node1");
        DiscoveryNode node2 = DiscoveryNodeUtils.create("node2");
        final boolean mightSucceed = randomBoolean();

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 works
        Optional<Join> optionalJoin1 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(1));
        CapturedRequest capturedRequest1 = capturedRequests1[0];
        assertEquals(node1, capturedRequest1.node());

        assertTrue(joinHelper.isJoinPending());
        final var join1Term = optionalJoin1.stream().mapToLong(Join::term).findFirst().orElse(0L);
        final var join1Status = new JoinStatus(node1, join1Term, PENDING_JOIN_WAITING_RESPONSE, TimeValue.ZERO);
        assertThat(joinHelper.getInFlightJoinStatuses(), equalTo(List.of(join1Status)));

        // check that sending a join to node2 works
        Optional<Join> optionalJoin2 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2);
        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(1));
        CapturedRequest capturedRequest2 = capturedRequests2[0];
        assertEquals(node2, capturedRequest2.node());

        final var join2Term = optionalJoin2.stream().mapToLong(Join::term).findFirst().orElse(0L);
        final var join2Status = new JoinStatus(node2, join2Term, PENDING_JOIN_WAITING_RESPONSE, TimeValue.ZERO);
        assertThat(
            new HashSet<>(joinHelper.getInFlightJoinStatuses()),
            equalTo(
                Stream.of(join1Status, join2Status)
                    .filter(joinStatus -> joinStatus.term() == Math.max(join1Term, join2Term))
                    .collect(Collectors.toSet())
            )
        );

        // check that sending another join to node1 is a noop as the previous join is still in progress
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        assertThat(capturingTransport.getCapturedRequestsAndClear().length, equalTo(0));

        // complete the previous join to node1
        completeJoinRequest(capturingTransport, capturedRequest1, mightSucceed);
        assertThat(joinHelper.getInFlightJoinStatuses(), equalTo(List.of(join2Status)));

        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node());

        // check that sending another join to node2 works if the optionalJoin is different
        Optional<Join> optionalJoin2a = optionalJoin2.isPresent() && randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2a);
        CapturedRequest[] capturedRequests2a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2a.length, equalTo(1));
        CapturedRequest capturedRequest2a = capturedRequests2a[0];
        assertEquals(node2, capturedRequest2a.node());

        // complete all the joins and check that isJoinPending is updated
        assertTrue(joinHelper.isJoinPending());
        assertTrue(transportService.nodeConnected(node1));
        assertTrue(transportService.nodeConnected(node2));

        completeJoinRequest(capturingTransport, capturedRequest2, mightSucceed);
        completeJoinRequest(capturingTransport, capturedRequest1a, mightSucceed);
        completeJoinRequest(capturingTransport, capturedRequest2a, mightSucceed);
        assertFalse(joinHelper.isJoinPending());

        if (mightSucceed) {
            // successful requests hold the connections open until the cluster state is applied
            joinHelper.onClusterStateApplied();
        }
        assertFalse(transportService.nodeConnected(node1));
        assertFalse(transportService.nodeConnected(node2));
    }

    private void completeJoinRequest(CapturingTransport capturingTransport, CapturedRequest request, boolean mightSucceed) {
        if (mightSucceed && randomBoolean()) {
            capturingTransport.handleResponse(request.requestId(), ActionResponse.Empty.INSTANCE);
        } else {
            capturingTransport.handleRemoteError(request.requestId(), new CoordinationStateRejectedException("dummy"));
        }
    }

    public void testFailedJoinAttemptLogLevel() {
        assertThat(JoinHelper.FailedJoinAttempt.getLogLevel(new TransportException("generic transport exception")), is(Level.INFO));

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("remote transport exception with generic cause", new Exception())
            ),
            is(Level.INFO)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by CoordinationStateRejectedException", new CoordinationStateRejectedException("test"))
            ),
            is(Level.DEBUG)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException(
                    "caused by FailedToCommitClusterStateException",
                    new FailedToCommitClusterStateException("test")
                )
            ),
            is(Level.DEBUG)
        );

        assertThat(
            JoinHelper.FailedJoinAttempt.getLogLevel(
                new RemoteTransportException("caused by NotMasterException", new NotMasterException("test"))
            ),
            is(Level.DEBUG)
        );
    }

    public void testJoinFailureOnUnhealthyNodes() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        CapturingTransport capturingTransport = new HandshakingCapturingTransport();
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node0");
        ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var taskManger = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            capturingTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            clusterSettings,
            new ClusterConnectionManager(Settings.EMPTY, capturingTransport, threadPool.getThreadContext()),
            taskManger,
            Tracer.NOOP
        );
        AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>(new StatusInfo(UNHEALTHY, "unhealthy-info"));
        JoinHelper joinHelper = new JoinHelper(
            null,
            new MasterService(Settings.EMPTY, clusterSettings, threadPool, taskManger),
            new NoOpClusterApplier(),
            transportService,
            () -> 0L,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            (s, p, r) -> {},
            nodeHealthServiceStatus::get,
            new JoinReasonService(() -> 0L),
            new NoneCircuitBreakerService(),
            Function.identity(),
            (listener, term) -> listener.onResponse(null),
            CompatibilityVersionsUtils.staticCurrent(),
            new FeatureService(List.of())
        );
        transportService.start();

        DiscoveryNode node1 = DiscoveryNodeUtils.create("node1");
        DiscoveryNode node2 = DiscoveryNodeUtils.create("node2");

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node1 doesn't work
        Optional<Join> optionalJoin1 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node1, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node1, randomNonNegativeLong(), optionalJoin1);
        CapturedRequest[] capturedRequests1 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        // check that sending a join to node2 doesn't work
        Optional<Join> optionalJoin2 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));

        transportService.start();
        joinHelper.sendJoinRequest(node2, randomNonNegativeLong(), optionalJoin2);

        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(0));

        assertFalse(joinHelper.isJoinPending());

        nodeHealthServiceStatus.getAndSet(new StatusInfo(HEALTHY, "healthy-info"));
        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node());
    }

    @TestLogging(reason = "testing WARN logging", value = "org.elasticsearch.cluster.coordination.JoinHelper:WARN")
    public void testLatestStoredStateFailure() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        CapturingTransport capturingTransport = new HandshakingCapturingTransport();
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node0");
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var taskManger = new TaskManager(Settings.EMPTY, threadPool, Set.of());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            capturingTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            clusterSettings,
            new ClusterConnectionManager(Settings.EMPTY, capturingTransport, threadPool.getThreadContext()),
            taskManger,
            Tracer.NOOP
        );
        JoinHelper joinHelper = new JoinHelper(
            null,
            new MasterService(Settings.EMPTY, clusterSettings, threadPool, taskManger),
            new NoOpClusterApplier(),
            transportService,
            () -> 1L,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            (s, p, r) -> {},
            () -> new StatusInfo(HEALTHY, "info"),
            new JoinReasonService(() -> 0L),
            new NoneCircuitBreakerService(),
            Function.identity(),
            (listener, term) -> listener.onFailure(new ElasticsearchException("simulated")),
            CompatibilityVersionsUtils.staticCurrent(),
            new FeatureService(List.of())
        );

        final var joinAccumulator = joinHelper.new CandidateJoinAccumulator();
        final var joinListener = new PlainActionFuture<Void>();
        joinAccumulator.handleJoinRequest(localNode, CompatibilityVersionsUtils.staticCurrent(), Set.of(), joinListener);
        assert joinListener.isDone() == false;

        try (var mockLog = MockLog.capture(JoinHelper.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warning log",
                    JoinHelper.class.getCanonicalName(),
                    Level.WARN,
                    "failed to retrieve latest stored state after winning election in term [1]"
                )
            );
            joinAccumulator.close(Coordinator.Mode.LEADER);
            mockLog.assertAllExpectationsMatched();
        }

        assertEquals("simulated", expectThrows(ElasticsearchException.class, () -> FutureUtils.get(joinListener)).getMessage());
    }

    private static class HandshakingCapturingTransport extends CapturingTransport {

        @Override
        protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
            if (action.equals(HANDSHAKE_ACTION_NAME)) {
                handleResponse(
                    requestId,
                    new TransportService.HandshakeResponse(node.getVersion(), Build.current().hash(), node, ClusterName.DEFAULT)
                );
            } else {
                super.onSendRequest(requestId, action, request, node);
            }
        }
    }
}
