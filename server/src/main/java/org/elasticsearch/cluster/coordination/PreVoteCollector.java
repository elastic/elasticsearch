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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.coordination.CoordinationState.isElectionQuorum;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

public class PreVoteCollector extends AbstractComponent {

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final TransportService transportService;
    private final Runnable startElection;

    @Nullable
    private volatile DiscoveryNode currentLeader; // null if there is currently no known leader
    private volatile PreVoteResponse preVoteResponse;

    PreVoteCollector(final Settings settings, final PreVoteResponse preVoteResponse,
                     final TransportService transportService, final Runnable startElection) {
        super(settings);
        this.preVoteResponse = preVoteResponse;
        this.transportService = transportService;
        this.startElection = startElection;

        // TODO does this need to be on the generic threadpool or can it use SAME?
        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, false, false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request)));
    }

    /**
     * Start a new pre-voting round.
     * @param clusterState the last-accepted cluster state
     * @param broadcastNodes the nodes from whom to request pre-votes
     * @return the pre-voting round, which can be closed to end the round early.
     */
    public Releasable start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        PreVotingRound currentRound = new PreVotingRound(clusterState, preVoteResponse.getCurrentTerm());
        currentRound.start(broadcastNodes);
        return currentRound;
    }

    public void update(final PreVoteResponse preVoteResponse, final DiscoveryNode currentLeader) {
        logger.trace("updating with preVoteResponse={}, currentLeader={}", preVoteResponse, currentLeader);
        this.preVoteResponse = preVoteResponse;
        this.currentLeader = currentLeader;
    }

    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        // TODO if we are a leader and the max term seen exceeds our term then we need to bump our term

        final DiscoveryNode currentLeader = this.currentLeader;
        if (currentLeader == null || currentLeader.equals(request.getSourceNode())) {
            return preVoteResponse;
        } else {
            throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
        }
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "currentLeader=" + currentLeader +
            ", preVoteResponse=" + preVoteResponse +
            '}';
    }

    private class PreVotingRound implements Releasable {
        private final Set<DiscoveryNode> preVotesReceived = newConcurrentSet();
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
            broadcastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        handlePreVoteResponse(response, n);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                            logger.debug("{} failed: {}", this, exp.getRootCause().getMessage());
                        } else {
                            logger.debug(new ParameterizedMessage("{} failed", this), exp);
                        }
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }

                    @Override
                    public String toString() {
                        return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                    }
                }));
        }

        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            // TODO the response carries the sender's current term. If an election starts then it should be in a higher term.

            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() > clusterState.version())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }

            preVotesReceived.add(sender);
            final VoteCollection voteCollection = new VoteCollection();
            preVotesReceived.forEach(voteCollection::addVote);

            if (isElectionQuorum(voteCollection, clusterState) == false) {
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
            return "PreVotingRound{" +
                "preVotesReceived=" + preVotesReceived +
                ", electionStarted=" + electionStarted +
                ", preVoteRequest=" + preVoteRequest +
                ", isClosed=" + isClosed +
                '}';
        }

        @Override
        public void close() {
            final boolean isClosedChanged = isClosed.compareAndSet(false, true);
            assert isClosedChanged;
        }
    }
}
