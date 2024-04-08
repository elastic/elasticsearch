/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class StatefulPreVoteCollector extends PreVoteCollector {

    private static final Logger logger = LogManager.getLogger(StatefulPreVoteCollector.class);

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final TransportService transportService;
    private final Executor clusterCoordinationExecutor;
    private final Runnable startElection;
    private final LongConsumer updateMaxTermSeen;
    private final ElectionStrategy electionStrategy;
    private NodeHealthService nodeHealthService;

    public StatefulPreVoteCollector(
        final TransportService transportService,
        final Runnable startElection,
        final LongConsumer updateMaxTermSeen,
        final ElectionStrategy electionStrategy,
        NodeHealthService nodeHealthService,
        LeaderHeartbeatService leaderHeartbeatService
    ) {
        this.transportService = transportService;
        this.clusterCoordinationExecutor = transportService.getThreadPool().executor(Names.CLUSTER_COORDINATION);
        this.startElection = startElection;
        this.updateMaxTermSeen = updateMaxTermSeen;
        this.electionStrategy = electionStrategy;
        this.nodeHealthService = nodeHealthService;

        transportService.registerRequestHandler(
            REQUEST_PRE_VOTE_ACTION_NAME,
            this.clusterCoordinationExecutor,
            false,
            false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request))
        );
    }

    /**
     * Start a new pre-voting round.
     *
     * @param clusterState   the last-accepted cluster state
     * @param broadcastNodes the nodes from whom to request pre-votes
     * @return the pre-voting round, which can be closed to end the round early.
     */
    @Override
    public Releasable start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        PreVotingRound preVotingRound = new PreVotingRound(clusterState, state.v2().getCurrentTerm());
        preVotingRound.start(broadcastNodes);
        return preVotingRound;
    }

    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        updateMaxTermSeen.accept(request.getCurrentTerm());

        Tuple<DiscoveryNode, PreVoteResponse> state = this.state;
        assert state != null : "received pre-vote request before fully initialised";

        final DiscoveryNode leader = state.v1();
        final PreVoteResponse response = state.v2();

        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.getStatus() == UNHEALTHY) {
            String message = "rejecting " + request + " on unhealthy node: [" + statusInfo.getInfo() + "]";
            logger.debug(message);
            throw new NodeHealthCheckFailureException(message);
        }

        if (leader == null) {
            return response;
        }

        if (leader.equals(request.getSourceNode())) {
            // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a follower. It's possible
            // that the leader lost its quorum, but while we're still a follower we will not offer joins to any other node so there is no
            // major drawback in offering a join to our old leader. The advantage of this is that it makes it slightly more likely that the
            // leader won't change, and also that its re-election will happen more quickly than if it had to wait for a quorum of followers
            // to also detect its failure.
            return response;
        }

        throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" + "state=" + state + '}';
    }

    private class PreVotingRound implements Releasable {
        private final Map<DiscoveryNode, PreVoteResponse> preVotesReceived = newConcurrentMap();
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        private final PreVoteRequest preVoteRequest;
        private final ClusterState clusterState;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PreVotingRound(final ClusterState clusterState, final long currentTerm) {
            this.clusterState = clusterState;
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
        }

        void start(final Iterable<DiscoveryNode> broadcastNodes) {
            logger.debug("{} requesting pre-votes from {}", this, broadcastNodes);
            broadcastNodes.forEach(
                n -> transportService.sendRequest(
                    n,
                    REQUEST_PRE_VOTE_ACTION_NAME,
                    preVoteRequest,
                    new TransportResponseHandler<PreVoteResponse>() {
                        @Override
                        public PreVoteResponse read(StreamInput in) throws IOException {
                            return new PreVoteResponse(in);
                        }

                        @Override
                        public void handleResponse(PreVoteResponse response) {
                            handlePreVoteResponse(response, n);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug(() -> format("%s failed", this), exp);
                        }

                        @Override
                        public Executor executor() {
                            return clusterCoordinationExecutor;
                        }

                        @Override
                        public String toString() {
                            return "TransportResponseHandler{" + StatefulPreVoteCollector.this + ", node=" + n + '}';
                        }
                    }
                )
            );
        }

        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            updateMaxTermSeen.accept(response.getCurrentTerm());

            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term() && response.getLastAcceptedVersion() > clusterState.version())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }

            preVotesReceived.put(sender, response);

            // create a fake VoteCollection based on the pre-votes and check if there is an election quorum
            final VoteCollection voteCollection = new VoteCollection();
            final DiscoveryNode localNode = clusterState.nodes().getLocalNode();
            final PreVoteResponse localPreVoteResponse = getPreVoteResponse();

            preVotesReceived.forEach(
                (node, preVoteResponse) -> voteCollection.addJoinVote(
                    new Join(
                        node,
                        localNode,
                        preVoteResponse.getCurrentTerm(),
                        preVoteResponse.getLastAcceptedTerm(),
                        preVoteResponse.getLastAcceptedVersion()
                    )
                )
            );

            if (electionStrategy.isElectionQuorum(
                clusterState.nodes().getLocalNode(),
                localPreVoteResponse.getCurrentTerm(),
                localPreVoteResponse.getLastAcceptedTerm(),
                localPreVoteResponse.getLastAcceptedVersion(),
                clusterState.getLastCommittedConfiguration(),
                clusterState.getLastAcceptedConfiguration(),
                voteCollection
            ) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            if (electionStarted.compareAndSet(false, true) == false) {
                logger.debug("{} added {} from {} but election has already started", this, response, sender);
                return;
            }

            logger.debug("{} added {} from {}, starting election", this, response, sender);
            startElection.run();
        }

        @Override
        public String toString() {
            return "PreVotingRound{"
                + "preVotesReceived="
                + preVotesReceived
                + ", electionStarted="
                + electionStarted
                + ", preVoteRequest="
                + preVoteRequest
                + ", isClosed="
                + isClosed
                + '}';
        }

        @Override
        public void close() {
            final boolean isNotAlreadyClosed = isClosed.compareAndSet(false, true);
            assert isNotAlreadyClosed;
        }
    }
}
