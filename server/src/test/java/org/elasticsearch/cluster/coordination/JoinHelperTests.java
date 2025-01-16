/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.CapturingTransport.CapturedRequest;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class JoinHelperTests extends ESTestCase {

    public void testJoinDeduplication() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        CapturingTransport capturingTransport = new HandshakingCapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            capturingTransport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet(),
            new ClusterConnectionManager(Settings.EMPTY, capturingTransport, threadPool.getThreadContext())
        );
        JoinHelper joinHelper = new JoinHelper(
            Settings.EMPTY,
            null,
            null,
            transportService,
            () -> 0L,
            () -> null,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            () -> new StatusInfo(HEALTHY, "info")
        );
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);
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
        assertEquals(node1, capturedRequest1.node);

        assertTrue(joinHelper.isJoinPending());

        // check that sending a join to node2 works
        Optional<Join> optionalJoin2 = randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2);
        CapturedRequest[] capturedRequests2 = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2.length, equalTo(1));
        CapturedRequest capturedRequest2 = capturedRequests2[0];
        assertEquals(node2, capturedRequest2.node);

        // check that sending another join to node1 is a noop as the previous join is still in progress
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        assertThat(capturingTransport.getCapturedRequestsAndClear().length, equalTo(0));

        // complete the previous join to node1
        completeJoinRequest(capturingTransport, capturedRequest1, mightSucceed);

        // check that sending another join to node1 now works again
        joinHelper.sendJoinRequest(node1, 0L, optionalJoin1);
        CapturedRequest[] capturedRequests1a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests1a.length, equalTo(1));
        CapturedRequest capturedRequest1a = capturedRequests1a[0];
        assertEquals(node1, capturedRequest1a.node);

        // check that sending another join to node2 works if the optionalJoin is different
        Optional<Join> optionalJoin2a = optionalJoin2.isPresent() && randomBoolean()
            ? Optional.empty()
            : Optional.of(new Join(localNode, node2, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()));
        joinHelper.sendJoinRequest(node2, 0L, optionalJoin2a);
        CapturedRequest[] capturedRequests2a = capturingTransport.getCapturedRequestsAndClear();
        assertThat(capturedRequests2a.length, equalTo(1));
        CapturedRequest capturedRequest2a = capturedRequests2a[0];
        assertEquals(node2, capturedRequest2a.node);

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
            capturingTransport.handleResponse(request.requestId, TransportResponse.Empty.INSTANCE);
        } else {
            capturingTransport.handleRemoteError(request.requestId, new CoordinationStateRejectedException("dummy"));
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

    public void testZen1JoinValidationRejectsMismatchedClusterUUID() {
        assertJoinValidationRejectsMismatchedClusterUUID(MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME);
    }

    public void testJoinValidationRejectsMismatchedClusterUUID() {
        assertJoinValidationRejectsMismatchedClusterUUID(JoinHelper.JOIN_VALIDATE_ACTION_NAME);
    }

    private void assertJoinValidationRejectsMismatchedClusterUUID(String actionName) {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        MockTransport mockTransport = new MockTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();

        TransportService transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet()
        );
        final String dataPath = "/my/data/path";
        new JoinHelper(
            Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), dataPath).build(),
            null,
            null,
            transportService,
            () -> 0L,
            () -> localClusterState,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            null
        ); // registers request handler
        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterState otherClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded())
            .build();

        final PlainActionFuture<TransportResponse.Empty> future = new PlainActionFuture<>();
        transportService.sendRequest(
            localNode,
            actionName,
            new ValidateJoinRequest(otherClusterState),
            new ActionListenerResponseHandler<>(future, in -> TransportResponse.Empty.INSTANCE)
        );
        deterministicTaskQueue.runAllTasks();

        final CoordinationStateRejectedException coordinationStateRejectedException = expectThrows(
            CoordinationStateRejectedException.class,
            future::actionGet
        );
        assertThat(
            coordinationStateRejectedException.getMessage(),
            allOf(
                containsString("This node previously joined a cluster with UUID"),
                containsString("and is now trying to join a different cluster"),
                containsString(localClusterState.metadata().clusterUUID()),
                containsString(otherClusterState.metadata().clusterUUID()),
                containsString(JoinHelper.getClusterUuidMismatchExplanation(Collections.singletonList(dataPath), 1))
            )
        );
    }

    public void testGetClusterUuidMismatchExplanation() {
        final List<String> paths = new ArrayList<>();
        paths.add("/foo/bar");

        assertThat(
            JoinHelper.getClusterUuidMismatchExplanation(paths, 1),
            equalTo(
                "This is forbidden and usually indicates an incorrect discovery or cluster bootstrapping configuration."
                    + " Note that the cluster UUID persists across restarts and can only be changed by deleting the contents of the"
                    + " node's data path [/foo/bar] which will also remove any data held by this node."
            )
        );
        assertThat(
            JoinHelper.getClusterUuidMismatchExplanation(paths, between(2, 10)),
            equalTo(
                "This is forbidden and usually indicates an incorrect discovery or cluster bootstrapping configuration."
                    + " Note that the cluster UUID persists across restarts and can only be changed by deleting the contents of the"
                    + " node's data path [/foo/bar] which will also remove any data held by all nodes that use this data path."
            )
        );

        paths.add("/baz/quux");

        assertThat(
            JoinHelper.getClusterUuidMismatchExplanation(paths, 1),
            equalTo(
                "This is forbidden and usually indicates an incorrect discovery or cluster bootstrapping configuration."
                    + " Note that the cluster UUID persists across restarts and can only be changed by deleting the contents of the"
                    + " node's data paths [/foo/bar, /baz/quux] which will also remove any data held by this node."
            )
        );
        assertThat(
            JoinHelper.getClusterUuidMismatchExplanation(paths, between(2, 10)),
            equalTo(
                "This is forbidden and usually indicates an incorrect discovery or cluster bootstrapping configuration."
                    + " Note that the cluster UUID persists across restarts and can only be changed by deleting the contents of the"
                    + " node's data paths [/foo/bar, /baz/quux] which will also remove any data held by all nodes"
                    + " that use these data paths."
            )
        );
    }

    public void testJoinFailureOnUnhealthyNodes() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        CapturingTransport capturingTransport = new HandshakingCapturingTransport();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet()
        );
        AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>(new StatusInfo(UNHEALTHY, "unhealthy-info"));
        JoinHelper joinHelper = new JoinHelper(
            Settings.EMPTY,
            null,
            null,
            transportService,
            () -> 0L,
            () -> null,
            (joinRequest, joinCallback) -> {
                throw new AssertionError();
            },
            startJoinRequest -> { throw new AssertionError(); },
            Collections.emptyList(),
            (s, p, r) -> {},
            () -> nodeHealthServiceStatus.get()
        );
        transportService.start();

        DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT);

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
        assertEquals(node1, capturedRequest1a.node);
    }

    private static class HandshakingCapturingTransport extends CapturingTransport {

        @Override
        protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
            if (action.equals(HANDSHAKE_ACTION_NAME)) {
                handleResponse(
                    requestId,
                    new TransportService.HandshakeResponse(node.getVersion(), Build.CURRENT.hash(), node, ClusterName.DEFAULT)
                );
            } else {
                super.onSendRequest(requestId, action, request, node);
            }
        }
    }
}
