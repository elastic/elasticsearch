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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.UnicastConfiguredHostsResolver;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.function.Supplier;

public class Coordinator extends AbstractLifecycleComponent {

    private final TransportService transportService;
    private final JoinHelper joinHelper;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    // TODO: the following two fields are package-private as some tests require access to them
    // These tests can be rewritten to use public methods once Coordinator is more feature-complete
    final Object mutex = new Object();
    final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)

    private final PeerFinder peerFinder;
    private final PreVoteCollector preVoteCollector;
    private final ElectionSchedulerFactory electionSchedulerFactory;
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private JoinHelper.JoinAccumulator joinAccumulator;

    public Coordinator(Settings settings, TransportService transportService, AllocationService allocationService,
                       MasterService masterService, Supplier<CoordinationState.PersistedState> persistedStateSupplier,
                       UnicastHostsProvider unicastHostsProvider) {
        super(settings);
        this.transportService = transportService;
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService,
            this::getCurrentTerm, this::handleJoinRequest);
        this.persistedStateSupplier = persistedStateSupplier;
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = joinHelper.new CandidateJoinAccumulator();

        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, Randomness.get(), transportService.getThreadPool());
        this.preVoteCollector = new PreVoteCollector(settings, transportService, this::startElection);
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService, unicastHostsProvider);
    }

    private void closePrevotingAndElectionScheduler() {
        if (prevotingRound != null) {
            prevotingRound.close();
            prevotingRound = null;
        }

        if (electionScheduler != null) {
            electionScheduler.close();
            electionScheduler = null;
        }
    }

    private void startElection() {

    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug("joinLeaderInTerm: from [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());
        Join join = coordinationState.get().handleStartJoin(startJoinRequest);
        lastJoin = Optional.of(join);
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("joinLeaderInTerm");
        }
        return join;
    }

    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        logger.trace("handleJoin: as {}, handling {}", mode, joinRequest);
        transportService.connectToNode(joinRequest.getSourceNode());

        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        synchronized (mutex) {
            final CoordinationState coordState = coordinationState.get();
            final boolean prevElectionWon = coordState.electionWon();

            if (optionalJoin.isPresent()) {
                Join join = optionalJoin.get();
                // if someone thinks we should be master, let's add our vote and try to become one
                // note that the following line should never throw an exception
                ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(coordState::handleJoin);

                if (coordState.electionWon()) {
                    // if we have already won the election then the actual join does not matter for election purposes,
                    // so swallow any exception
                    try {
                        coordState.handleJoin(join);
                    } catch (CoordinationStateRejectedException e) {
                        logger.trace("failed to add join, ignoring", e);
                    }
                } else {
                    coordState.handleJoin(join); // this might fail and bubble up the exception
                }
            }

            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoin");
            }
        }
    }

    void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        lastKnownLeader = Optional.of(getLocalNode());
        peerFinder.deactivate(getLocalNode());
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());
    }

    void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("{}: becoming FOLLOWER of [{}] (was {}, lastKnownLeader was [{}])", method, leaderNode, mode, lastKnownLeader);

        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER;
            joinAccumulator.close(mode);
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator();
        }

        lastKnownLeader = Optional.of(leaderNode);
        peerFinder.deactivate(getLocalNode());
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), leaderNode);
    }

    private PreVoteResponse getPreVoteResponse() {
        return new PreVoteResponse(getCurrentTerm(), coordinationState.get().getLastAcceptedTerm(), coordinationState.get().getLastAcceptedVersion());
    }

    // package-visible for testing
    long getCurrentTerm() {
        synchronized (mutex) {
            return coordinationState.get().getCurrentTerm();
        }
    }

    // package-visible for testing
    Mode getMode() {
        synchronized (mutex) {
            return mode;
        }
    }

    // package-visible for testing
    DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    @Override
    protected void doStart() {
        CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
        coordinationState.set(new CoordinationState(settings, getLocalNode(), persistedState));
    }

    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

    public void invariant() {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinder.isActive() == false;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinder.isActive() == false;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinder.isActive();
                assert prevotingRound == null || electionScheduler != null;
            }
        }
    }

    public Optional<ClusterState> getLastCommittedState() {
        return null; // TODO
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    private class CoordinatorPeerFinder extends PeerFinder {

        public CoordinatorPeerFinder(Settings settings, TransportService transportService, UnicastHostsProvider unicastHostsProvider) {
            super(settings, transportService, new HandshakingTransportAddressConnector(settings, transportService),
                new UnicastConfiguredHostsResolver(settings, transportService, unicastHostsProvider));
        }

        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            // TODO
        }

        @Override
        protected void onFoundPeersUpdated() {
            synchronized (mutex) {
                if (mode == Mode.CANDIDATE) {
                    final CoordinationState.VoteCollection expectedVotes = new CoordinationState.VoteCollection();
                    getFoundPeers().forEach(expectedVotes::addVote);
                    final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
                    final boolean foundQuorum = CoordinationState.isElectionQuorum(expectedVotes, lastAcceptedState);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            electionScheduler = electionSchedulerFactory.startElectionScheduler(TimeValue.ZERO, () -> {
                                synchronized (mutex) {
                                    if (mode == Mode.CANDIDATE) {
                                        if (prevotingRound != null) {
                                            prevotingRound.close();
                                        }
                                        prevotingRound = preVoteCollector.start(lastAcceptedState, getFoundPeers());
                                    }
                                }
                            });
                        }
                    } else {
                        closePrevotingAndElectionScheduler();
                    }
                }
            }
        }
    }
}
