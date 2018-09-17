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
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Coordinator extends AbstractLifecycleComponent {

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final TransportService transportService;
    private final JoinHelper joinHelper;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    // TODO: the following two fields are package-private as some tests require access to them
    // These tests can be rewritten to use public methods once Coordinator is more feature-complete
    final Object mutex = new Object();
    final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)
    private volatile Optional<ClusterState> lastCommittedState = Optional.empty();

    private final PeerFinder peerFinder;
    private final PreVoteCollector preVoteCollector;
    private final ElectionSchedulerFactory electionSchedulerFactory;
    private final UnicastConfiguredHostsResolver configuredHostsResolver;
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;
    private AtomicLong maxTermSeen = new AtomicLong();

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
            this::getCurrentTerm, this::handleJoinRequest, this::joinLeaderInTerm);
        this.persistedStateSupplier = persistedStateSupplier;
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = joinHelper.new CandidateJoinAccumulator();

        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, Randomness.get(), transportService.getThreadPool());
        this.preVoteCollector = new PreVoteCollector(settings, transportService, this::startElection, this::updateMaxTermSeen);
        configuredHostsResolver = new UnicastConfiguredHostsResolver(settings, transportService, unicastHostsProvider);
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);

        transportService.registerRequestHandler(PUBLISH_STATE_ACTION_NAME, Names.GENERIC, false, false,
            in -> new PublishRequest(in, transportService.getLocalNode()),
            (request, channel, task) -> channel.sendResponse(handlePublishRequest(request)));

        transportService.registerRequestHandler(COMMIT_STATE_ACTION_NAME, Names.GENERIC, false, false,
            ApplyCommitRequest::new,
            (request, channel, task) -> {
                handleApplyCommit(request);
                channel.sendResponse(Empty.INSTANCE);
            });
    }

    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            lastCommittedState = Optional.of(coordinationState.get().getLastAcceptedState());
        }
    }

    private PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        synchronized (mutex) {
            final DiscoveryNode sourceNode = publishRequest.getAcceptedState().nodes().getMasterNode();
            logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);
            ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            if (sourceNode.equals(getLocalNode()) == false) {
                becomeFollower("handlePublishRequest", sourceNode);
            }

            return new PublishWithJoinResponse(publishResponse,
                joinWithDestination(lastJoin, sourceNode, publishRequest.getAcceptedState().term()));
        }
    }

    private static Optional<Join> joinWithDestination(Optional<Join> lastJoin, DiscoveryNode leader, long term) {
        if (lastJoin.isPresent()
            && lastJoin.get().getTargetNode().getId().equals(leader.getId())
            && lastJoin.get().getTerm() == term) {
            return lastJoin;
        }

        return Optional.empty();
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

    private void updateMaxTermSeen(final long term) {
        maxTermSeen.updateAndGet(oldMaxTerm -> Math.max(oldMaxTerm, term));
        // TODO if we are leader here, and there is no publication in flight, then we should bump our term
        // (if we are leader and there _is_ a publication in flight then doing so would cancel the publication, so don't do that, but
        // do check for this after the publication completes)
    }

    private void startElection() {
        synchronized (mutex) {
            // The preVoteCollector is only active while we are candidate, but it does not call this method with synchronisation, so we have
            // to check our mode again here.
            if (mode == Mode.CANDIDATE) {
                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen.get()) + 1);
                getDiscoveredNodes().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
            }
        }
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        synchronized (mutex) {
            logger.debug("joinLeaderInTerm: for [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());
            final Join join = coordinationState.get().handleStartJoin(startJoinRequest);
            lastJoin = Optional.of(join);
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm");
            }
            return join;
        }
    }

    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);
        transportService.connectToNode(joinRequest.getSourceNode());

        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        synchronized (mutex) {
            final CoordinationState coordState = coordinationState.get();
            final boolean prevElectionWon = coordState.electionWon();

            optionalJoin.ifPresent(this::handleJoin);
            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoinRequest");
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
        peerFinder.deactivate(leaderNode);
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), leaderNode);
    }

    private PreVoteResponse getPreVoteResponse() {
        return new PreVoteResponse(getCurrentTerm(), coordinationState.get().getLastAcceptedTerm(),
            coordinationState.get().getLastAcceptedVersion());
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
        configuredHostsResolver.start();
    }

    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }
    }

    @Override
    protected void doStop() {
        configuredHostsResolver.stop();
    }

    @Override
    protected void doClose() {

    }

    public void invariant() {
        synchronized (mutex) {
            final Optional<DiscoveryNode> peerFinderLeader = peerFinder.getLeader();
            if (mode == Mode.LEADER) {
                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
            }
        }
    }

    // for tests
    boolean hasJoinVoteFrom(DiscoveryNode localNode) {
        return coordinationState.get().containsJoinVoteFor(localNode);
    }

    void handleJoin(Join join) {
        synchronized (mutex) {
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            if (coordinationState.get().electionWon()) {
                // if we have already won the election then the actual join does not matter for election purposes,
                // so swallow any exception
                try {
                    coordinationState.get().handleJoin(join);
                } catch (CoordinationStateRejectedException e) {
                    logger.debug("failed to add join, ignoring", e);
                }
            } else {
                coordinationState.get().handleJoin(join); // this might fail and bubble up the exception
            }
        }
    }

    public ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    public Optional<ClusterState> getLastCommittedState() {
        return lastCommittedState;
    }

    private List<DiscoveryNode> getDiscoveredNodes() {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(getLocalNode());
        peerFinder.getFoundPeers().forEach(nodes::add);
        return nodes;
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    private class CoordinatorPeerFinder extends PeerFinder {

        CoordinatorPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                              ConfiguredHostsResolver configuredHostsResolver) {
            super(settings, transportService, transportAddressConnector, configuredHostsResolver);
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
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
                    final boolean foundQuorum = CoordinationState.isElectionQuorum(expectedVotes, lastAcceptedState);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            final TimeValue gracePeriod = TimeValue.ZERO; // TODO variable grace period
                            electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, () -> {
                                synchronized (mutex) {
                                    if (mode == Mode.CANDIDATE) {
                                        if (prevotingRound != null) {
                                            prevotingRound.close();
                                        }
                                        prevotingRound = preVoteCollector.start(lastAcceptedState, getDiscoveredNodes());
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
