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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.elasticsearch.cluster.coordination.CoordinationState.isElectionQuorum;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

public class PreVoteCollector extends AbstractComponent {

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final AtomicLong maxTermSeen = new AtomicLong(0);
    private final TransportService transportService;
    private final LongConsumer startElection; // consumes maximum known term
    private volatile PreVoteResponse preVoteResponse;

    @Nullable
    private volatile PreVotingRound currentRound;
    @Nullable
    private volatile DiscoveryNode currentLeader;

    private long updateMaxTermSeen(long term) {
        return maxTermSeen.accumulateAndGet(term, Math::max);
    }

    PreVoteCollector(Settings settings, PreVoteResponse preVoteResponse, TransportService transportService, LongConsumer startElection) {
        super(settings);
        this.preVoteResponse = preVoteResponse;
        this.transportService = transportService;
        this.startElection = startElection;

        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, false, false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request)));
    }

    public void start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        logger.debug("{} starting", this);
        assert currentRound == null;
        currentRound = new PreVotingRound(clusterState);
        currentRound.start(broadcastNodes);
    }

    public void stop() {
        currentRound.stop();
        currentRound = null;
    }

    public void update(PreVoteResponse preVoteResponse, DiscoveryNode currentLeader) {
        logger.trace("updating with preVoteResponse={}, currentLeader={}", preVoteResponse, currentLeader);
        this.preVoteResponse = preVoteResponse;
        this.currentLeader = currentLeader;
    }

    private PreVoteResponse handlePreVoteRequest(PreVoteRequest request) {
        updateMaxTermSeen(request.getCurrentTerm());
        // TODO if we are a leader and the max term seen exceeds our term then we need to bump our term

        DiscoveryNode currentLeader = this.currentLeader;

        if (currentLeader == null || currentLeader.equals(request.getSourceNode())) {
            // TODO comment about rare case.
            return preVoteResponse;
        } else {
            throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
        }
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "currentRound=" + currentRound +
            ", currentLeader=" + currentLeader +
            ", preVoteResponse=" + preVoteResponse +
            '}';
    }

    private class PreVotingRound {
        private final Set<DiscoveryNode> preVotesReceived = newConcurrentSet();
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        private final PreVoteRequest preVoteRequest;
        private final ClusterState clusterState;
        private final AtomicBoolean isRunning = new AtomicBoolean();

        PreVotingRound(ClusterState clusterState) {
            this.clusterState = clusterState;
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), preVoteResponse.getCurrentTerm());
        }

        void start(final Iterable<DiscoveryNode> broadcastNodes) {

            final boolean isRunningChanged = isRunning.compareAndSet(false, true);
            assert isRunningChanged;

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

        void stop() {
            final boolean isRunningChanged = isRunning.compareAndSet(true, false);
            assert isRunningChanged;
        }

        private void handlePreVoteResponse(PreVoteResponse response, DiscoveryNode sender) {
            if (isRunning.get() == false) {
                logger.debug("{} ignoring {} from {}, no longer running", this, response, sender);
                return;
            }

            final long currentMaxTermSeen = updateMaxTermSeen(response.getCurrentTerm());

            final PreVoteResponse currentPreVoteResponse = preVoteResponse;
            if (response.getLastAcceptedTerm() > currentPreVoteResponse.getLastAcceptedTerm()
                || (response.getLastAcceptedTerm() == currentPreVoteResponse.getLastAcceptedTerm()
                && response.getLastAcceptedVersion() > currentPreVoteResponse.getLastAcceptedVersion())) {
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

            logger.debug("{} added {} from {}, starting election in term > {}", this, response, sender, currentMaxTermSeen);
            startElection.accept(currentMaxTermSeen);
        }

        @Override
        public String toString() {
            return "PreVotingRound{" +
                "preVotesReceived=" + preVotesReceived +
                ", electionStarted=" + electionStarted +
                ", preVoteRequest=" + preVoteRequest +
                ", isRunning=" + isRunning +
                '}';
        }
    }
}
