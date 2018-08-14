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
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.PreVoteCollectorFactory.REQUEST_PRE_VOTE_ACTION_NAME;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PreVoteCollectorTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private PreVoteCollectorFactory preVoteCollectorFactory;
    private boolean electionOccurred = false;
    private DiscoveryNode localNode;
    private Map<DiscoveryNode, PreVoteResponse> responsesByNode = new HashMap<>();
    private long lastElectionMaxTermSeen;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings);
        final Transport capturingTransport = new CapturingTransport() {
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
        };
        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(localNode, new PreVoteResponse(3, 2, 1));
        TransportService transportService = new TransportService(settings, capturingTransport,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode, null, emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();

        preVoteCollectorFactory = new PreVoteCollectorFactory(settings, getLocalPreVoteResponse(), transportService, maxTermSeen -> {
            assert electionOccurred == false;
            electionOccurred = true;
            lastElectionMaxTermSeen = maxTermSeen;
        });
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
        deterministicTaskQueue.runAllRunnableTasks(random());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
    }

    private Releasable startCollector(DiscoveryNode... votingNodes) {
        final VotingConfiguration votingConfiguration
            = new VotingConfiguration(Arrays.stream(votingNodes).map(DiscoveryNode::getId).collect(Collectors.toSet()));
        final ClusterState clusterState = CoordinationStateTests.clusterState(0, 0, localNode, votingConfiguration, votingConfiguration, 0);
        return preVoteCollectorFactory.start(clusterState, responsesByNode.keySet());
    }

    public void testStartsElectionIfLocalNodeIsOnlyNode() {
        startAndRunCollector(localNode);
        assertTrue(electionOccurred);
    }

    public void testStartsElectionIfLocalNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
    }


    public void testStartsElectionIfOtherNodeIsQuorum() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
    }

    public void testDoesNotStartsElectionIfOtherNodeIsQuorumAndDoesNotRespond() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, null);
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testDoesNotStartElectionIfStopped() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 1));
        startCollector(otherNode).close();
        runCollector();
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterTerms() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 3, 1));
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testIgnoresPreVotesFromLaterVersionInSameTerm() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 2, 2));
        startAndRunCollector(otherNode);
        assertFalse(electionOccurred);
    }

    public void testAcceptsPreVotesFromLaterVersionInEarlierTerms() {
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode, new PreVoteResponse(3, 1, 2));
        startAndRunCollector(otherNode);
        assertTrue(electionOccurred);
        assertThat(lastElectionMaxTermSeen, is(3L));
    }

    public void testReturnsMaximumSeenTerm() {
        final DiscoveryNode otherNode1 = new DiscoveryNode("other-node-1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode2 = new DiscoveryNode("other-node-2", buildNewFakeTransportAddress(), Version.CURRENT);
        responsesByNode.put(otherNode1, new PreVoteResponse(4, 2, 1));
        responsesByNode.put(otherNode2, new PreVoteResponse(5, 2, 1));
        startAndRunCollector(otherNode1, otherNode2);
        assertTrue(electionOccurred);
        assertThat(lastElectionMaxTermSeen, is(5L));
    }

    public void testResponseIfCandidate() {
        final long term = randomNonNegativeLong();
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(5, 4, 3);
        preVoteCollectorFactory.update(newPreVoteResponse, null);
        final PreVoteResponse preVoteResponse = preVoteCollectorFactory.handlePreVoteRequest(new PreVoteRequest(otherNode, term));

        assertThat(preVoteResponse, equalTo(newPreVoteResponse));
        assertThat(preVoteCollectorFactory.getMaxTermSeen(), is(term));
    }

    public void testResponseToNonLeaderIfNotCandidate() {
        final long term = randomNonNegativeLong();
        final DiscoveryNode leaderNode = new DiscoveryNode("leader-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(5, 4, 3);
        preVoteCollectorFactory.update(newPreVoteResponse, leaderNode);

        expectThrows(CoordinationStateRejectedException.class, () ->
            preVoteCollectorFactory.handlePreVoteRequest(new PreVoteRequest(otherNode, term)));
        assertThat(preVoteCollectorFactory.getMaxTermSeen(), is(term));
    }

    public void testResponseToRequestFromLeader() {
        // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a
        // follower. It's possible that the leader lost its quorum, but while we're still a follower we will not
        // offer joins to any other node so there is no major drawback in offering a join to our old leader. The
        // advantage of this is that it makes it slightly more likely that the leader won't change, and also that
        // its re-election will happen more quickly than if it had to wait for a quorum of followers to also detect
        // its failure.

        final long term = randomNonNegativeLong();
        final DiscoveryNode leaderNode = new DiscoveryNode("leader-node", buildNewFakeTransportAddress(), Version.CURRENT);

        PreVoteResponse newPreVoteResponse = new PreVoteResponse(5, 4, 3);
        preVoteCollectorFactory.update(newPreVoteResponse, leaderNode);
        final PreVoteResponse preVoteResponse = preVoteCollectorFactory.handlePreVoteRequest(new PreVoteRequest(leaderNode, term));

        assertThat(preVoteResponse, equalTo(newPreVoteResponse));
        assertThat(preVoteCollectorFactory.getMaxTermSeen(), is(term));
    }
}
