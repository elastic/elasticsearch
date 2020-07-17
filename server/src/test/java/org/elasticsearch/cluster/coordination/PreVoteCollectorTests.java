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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.Names.SAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class PreVoteCollectorTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private PreVoteCollector preVoteCollector;
    private boolean electionOccurred = false;
    private DiscoveryNode localNode;
    private Map<DiscoveryNode, PreVoteResponse> responsesByNode = new HashMap<>();
    private long currentTerm, lastAcceptedTerm, lastAcceptedVersion;
    private TransportService transportService;
    private StatusInfo healthStatus;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(final long requestId, final String action, final TransportRequest request,
                                         final DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, is(REQUEST_PRE_VOTE_ACTION_NAME));
                assertThat(request, instanceOf(PreVoteRequest.class));
                assertThat(node, not(equalTo(localNode)));
                PreVoteRequest preVoteRequest = (PreVoteRequest) request;
                assertThat(preVoteRequest.getSourceNode(), equalTo(localNode));
                deterministicTaskQueue.scheduleNow(new Runnable() {
                    @Override
                    public void run() {
                        final PreVoteResponse response = responsesByNode.get(node);
                        if (response == null) {
                            handleRemoteError(requestId, new ConnectTransportException(node, "no response"));
                        } else {
                            handleResponse(requestId, response);
                        }
                    }

                    @Override
                    public String toString() {
                        return "response to " + request + " from " + node;
                    }
                });
            }

            @Override
            public void handleRemoteError(long requestId, Throwable t) {
                logger.warn("Remote error", t);
            }
        };
        lastAcceptedTerm = randomNonNegativeLong();
        currentTerm = randomLongBetween(lastAcceptedTerm, Long.MAX_VALUE);
        lastAcceptedVersion = randomNonNegativeLong();

        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(localNode, new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion));
        healthStatus = new StatusInfo(HEALTHY, "healthy-info");
        transportService = mockTransport.createTransportService(settings,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        preVoteCollector = new PreVoteCollector(transportService, () -> {
            assert electionOccurred == false;
            electionOccurred = true;
        }, l -> {
        }, ElectionStrategy.DEFAULT_INSTANCE, () -> healthStatus);
        preVoteCollector.update(getLocalPreVoteResponse(), null);
    }

    private PreVoteResponse getLocalPreVoteResponse() {
        return Objects.requireNonNull(responsesByNode.get(localNode));
    }

    private void startAndRunCollector(DiscoveryNode... votingNodes) {
        try (Releasable ignored = startCollector(votingNodes)) {
            runCollector();
        }
    }

    private void runCollector() {
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
    }

    private ClusterState makeClusterState(DiscoveryNode[] votingNodes) {
        final VotingConfiguration votingConfiguration = VotingConfiguration.of(votingNodes);
        return CoordinationStateTests.clusterState(lastAcceptedTerm, lastAcceptedVersion, localNode,
            votingConfiguration, votingConfiguration, 0);
    }

    private Releasable startCollector(DiscoveryNode... votingNodes) {
        return preVoteCollector.start(makeClusterState(votingNodes), responsesByNode.keySet());
    }

    public void testStartsElectionIfLocalNodeIsOnlyNode() {
        startAndRunCollector(localNode);
        assertTrue(electionOccurred);
    }

    public void testNoElectionStartIfLocalNodeIsOnlyNodeAndUnhealthy() {
        healthStatus = new StatusInfo(UNHEALTHY, "unhealthy-info");
        preVoteCollector.update(getLocalPreVoteResponse(), null);
        startAndRunCollector(localNode);
        assertFalse(electionOccurred);
    }

    public void testStartsElectionIfLocalNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, getLocalPreVoteResponse());
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
    }


    public void testStartsElectionIfOtherNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, getLocalPreVoteResponse());
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
    }

    public void testDoesNotStartsElectionIfOtherNodeIsQuorumAndDoesNotRespond() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, null);
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testUnhealthyNodeDoesNotOfferPreVote() {
        final long term = randomNonNegativeLong();
        healthStatus = new StatusInfo(UNHEALTHY, "unhealthy-info");
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        RemoteTransportException remoteTransportException = expectThrows(RemoteTransportException.class, () ->
            handlePreVoteRequestViaTransportService(new PreVoteRequest(otherNode, term)));
        assertThat(remoteTransportException.getCause(), instanceOf(NodeHealthCheckFailureException.class));
    }

    public void testDoesNotStartElectionIfStopped() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, getLocalPreVoteResponse());
        startCollector(otherNode).close();
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterTerms() {
        assumeTrue("unluckily chose lastAcceptedTerm too close to currentTerm, no later terms", lastAcceptedTerm < currentTerm - 1);

        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode,
            new PreVoteResponse(currentTerm, randomLongBetween(lastAcceptedTerm + 1, currentTerm - 1), randomNonNegativeLong()));
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterVersionInSameTerm() {
        assumeTrue("unluckily hit Long.MAX_VALUE for lastAcceptedVersion, cannot increment", lastAcceptedVersion < Long.MAX_VALUE);

        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode,
            new PreVoteResponse(currentTerm, lastAcceptedTerm, randomLongBetween(lastAcceptedVersion + 1, Long.MAX_VALUE)));
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testAcceptsPreVotesFromAnyVersionInEarlierTerms() {
        assumeTrue("unluckily hit 0 for lastAcceptedTerm, cannot decrement", 0 < lastAcceptedTerm);

        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode,
            new PreVoteResponse(currentTerm, randomLongBetween(0, lastAcceptedTerm - 1), randomNonNegativeLong()));
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
    }

    private PreVoteResponse randomPreVoteResponse() {
        final long currentTerm = randomNonNegativeLong();
        return new PreVoteResponse(currentTerm, randomLongBetween(0, currentTerm), randomNonNegativeLong());
    }

    public void testPrevotingIndicatesElectionSuccess() {
        assumeTrue("unluckily hit currentTerm = Long.MAX_VALUE, cannot increment", currentTerm < Long.MAX_VALUE);

        final Set<DiscoveryNode> votingNodesSet = new HashSet<>();
        final int nodeCount = randomIntBetween(0, 5);
        for (int i = 0; i < nodeCount; i++) {
            final DiscoveryNode otherNode = new DiscoveryNode("other-node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            responsesByNode.put(otherNode, randomBoolean() ? null : randomPreVoteResponse());
            PreVoteResponse newPreVoteResponse = new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
            preVoteCollector.update(newPreVoteResponse, null);
            if (randomBoolean()) {
                votingNodesSet.add(otherNode);
            }
        }

        DiscoveryNode[] votingNodes = votingNodesSet.toArray(new DiscoveryNode[0]);
        startAndRunCollector(votingNodes);

        final CoordinationState coordinationState = new CoordinationState(localNode,
            new InMemoryPersistedState(currentTerm, makeClusterState(votingNodes)), ElectionStrategy.DEFAULT_INSTANCE);

        final long newTerm = randomLongBetween(currentTerm + 1, Long.MAX_VALUE);

        coordinationState.handleStartJoin(new StartJoinRequest(localNode, newTerm));

        responsesByNode.forEach((otherNode, preVoteResponse) -> {
            if (preVoteResponse != null) {
                try {
                    coordinationState.handleJoin(new Join(otherNode, localNode, newTerm,
                        preVoteResponse.getLastAcceptedTerm(), preVoteResponse.getLastAcceptedVersion()));
                } catch (CoordinationStateRejectedException ignored) {
                    // ok to reject some joins.
                }
            }
        });

        assertThat(coordinationState.electionWon(), equalTo(electionOccurred));
    }

    private PreVoteResponse handlePreVoteRequestViaTransportService(PreVoteRequest preVoteRequest) {
        final AtomicReference<PreVoteResponse> responseRef = new AtomicReference<>();
        final AtomicReference<TransportException> exceptionRef = new AtomicReference<>();

        transportService.sendRequest(localNode, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
            new TransportResponseHandler<PreVoteResponse>() {
                @Override
                public PreVoteResponse read(StreamInput in) throws IOException {
                    return new PreVoteResponse(in);
                }

                @Override
                public void handleResponse(PreVoteResponse response) {
                    responseRef.set(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    exceptionRef.set(exp);
                }

                @Override
                public String executor() {
                    return SAME;
                }
            });

        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(deterministicTaskQueue.hasDeferredTasks());

        final PreVoteResponse response = responseRef.get();
        final TransportException transportException = exceptionRef.get();

        if (transportException != null) {
            assertThat(response, nullValue());
            throw transportException;
        }

        assertThat(response, not(nullValue()));
        return response;
    }

    public void testResponseIfCandidate() {
        final long term = randomNonNegativeLong();
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
        preVoteCollector.update(newPreVoteResponse, null);

        assertThat(handlePreVoteRequestViaTransportService(new PreVoteRequest(otherNode, term)), equalTo(newPreVoteResponse));
    }

    public void testResponseToNonLeaderIfNotCandidate() {
        final long term = randomNonNegativeLong();
        final DiscoveryNode leaderNode = new DiscoveryNode("leader-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
        preVoteCollector.update(newPreVoteResponse, leaderNode);

        RemoteTransportException remoteTransportException = expectThrows(RemoteTransportException.class, () ->
            handlePreVoteRequestViaTransportService(new PreVoteRequest(otherNode, term)));
        assertThat(remoteTransportException.getCause(), instanceOf(CoordinationStateRejectedException.class));
    }

    public void testResponseToRequestFromLeader() {
        // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a follower. It's possible that
        // the leader lost its quorum, but while we're still a follower we will not offer joins to any other node so there is no major
        // drawback in offering a join to our old leader. The advantage of this is that it makes it slightly more likely that the leader
        // won't change, and also that its re-election will happen more quickly than if it had to wait for a quorum of followers to also
        // detect its failure.

        final long term = randomNonNegativeLong();
        final DiscoveryNode leaderNode = new DiscoveryNode("leader-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(currentTerm, lastAcceptedTerm, lastAcceptedVersion);
        preVoteCollector.update(newPreVoteResponse, leaderNode);

        assertThat(handlePreVoteRequestViaTransportService(new PreVoteRequest(leaderNode, term)), equalTo(newPreVoteResponse));
    }
}
