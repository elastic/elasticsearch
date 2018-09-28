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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.coordination.JoinHelper.InitialJoinAccumulator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.UnicastConfiguredHostsResolver;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Coordinator extends AbstractLifecycleComponent implements Discovery {

    // the timeout for the publication of each value
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TransportService transportService;
    private final MasterService masterService;
    private final JoinHelper joinHelper;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
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
    private final TimeValue publishTimeout;
    private final PublicationTransportHandler publicationHandler;
    private final LeaderChecker leaderChecker;
    private final FollowersChecker followersChecker;
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;
    @Nullable
    private Releasable leaderCheckScheduler;
    private AtomicLong maxTermSeen = new AtomicLong();

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private JoinHelper.JoinAccumulator joinAccumulator;
    private Optional<Publication> currentPublication = Optional.empty();

    public Coordinator(Settings settings, TransportService transportService, AllocationService allocationService,
                       MasterService masterService, Supplier<CoordinationState.PersistedState> persistedStateSupplier,
                       UnicastHostsProvider unicastHostsProvider, Random random) {
        super(settings);
        this.transportService = transportService;
        this.masterService = masterService;
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService,
            this::getCurrentTerm, this::handleJoinRequest, this::joinLeaderInTerm);
        this.persistedStateSupplier = persistedStateSupplier;
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = new InitialJoinAccumulator();
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        this.preVoteCollector = new PreVoteCollector(settings, transportService, this::startElection, this::updateMaxTermSeen);
        configuredHostsResolver = new UnicastConfiguredHostsResolver(settings, transportService, unicastHostsProvider);
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        this.publicationHandler = new PublicationTransportHandler(transportService, this::handlePublishRequest, this::handleApplyCommit);
        this.leaderChecker = new LeaderChecker(settings, transportService, getOnLeaderFailure());
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::onFollowerFailure);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        masterService.setClusterStateSupplier(this::getStateForMasterService);
    }

    private Runnable getOnLeaderFailure() {
        return new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    becomeCandidate("onLeaderFailure");
                }
            }

            @Override
            public String toString() {
                return "notification of leader failure";
            }
        };
    }

    private void onFollowerFailure(DiscoveryNode discoveryNode) {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                masterService.submitStateUpdateTask("node-left",
                    new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, "node left"),
                    ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                    nodeRemovalExecutor,
                    nodeRemovalExecutor);
            }
        }
    }

    private void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException("onFollowerCheckRequest: current term is ["
                    + getCurrentTerm() + "], rejecting " + followerCheckRequest);
            }

            becomeFollower("onFollowerCheckRequest", followerCheckRequest.getSender());
        }
    }

    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            lastCommittedState = Optional.of(coordinationState.get().getLastAcceptedState());
            // TODO: send to applier
        }
    }

    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode()) :
            publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

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
            peerFinder.setCurrentTerm(getCurrentTerm());
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm"); // updates followersChecker
            } else {
                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
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
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;
            cancelActivePublication();
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);

            if (leaderCheckScheduler != null) {
                leaderCheckScheduler.close();
                leaderCheckScheduler = null;
            }

            followersChecker.clearCurrentNodes();
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        lastKnownLeader = Optional.of(getLocalNode());
        peerFinder.deactivate(getLocalNode());
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        assert leaderCheckScheduler == null : leaderCheckScheduler;
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug("{}: becoming FOLLOWER of [{}] (was {}, lastKnownLeader was [{}])", method, leaderNode, mode, lastKnownLeader);

        final boolean restartLeaderChecker = (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) == false;

        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER;
            joinAccumulator.close(mode);
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator();
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
        }

        lastKnownLeader = Optional.of(leaderNode);
        peerFinder.deactivate(leaderNode);
        closePrevotingAndElectionScheduler();
        cancelActivePublication();
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        if (restartLeaderChecker) {
            if (leaderCheckScheduler != null) {
                leaderCheckScheduler.close();
            }
            leaderCheckScheduler = leaderChecker.startLeaderChecker(leaderNode);
        }

        followersChecker.clearCurrentNodes();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
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

    // package-visible for testing
    boolean publicationInProgress() {
        synchronized (mutex) {
            return currentPublication.isPresent();
        }
    }

    @Override
    protected void doStart() {
        CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
        coordinationState.set(new CoordinationState(settings, getLocalNode(), persistedState));
        peerFinder.setCurrentTerm(getCurrentTerm());
        configuredHostsResolver.start();
    }

    @Override
    public DiscoveryStats stats() {
        // TODO implement
        return null;
    }

    @Override
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
            assert peerFinder.getCurrentTerm() == getCurrentTerm();
            assert followersChecker.getFastResponseState().term == getCurrentTerm();
            assert followersChecker.getFastResponseState().mode == getMode();
            if (mode == Mode.LEADER) {
                final boolean becomingMaster = getStateForMasterService().term() != getCurrentTerm();

                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert becomingMaster || getStateForMasterService().nodes().getMasterNodeId() != null : getStateForMasterService();
                assert leaderCheckScheduler == null : leaderCheckScheduler;

                final Set<DiscoveryNode> knownFollowers = followersChecker.getKnownFollowers();
                final Set<DiscoveryNode> lastPublishedNodes = new HashSet<>();
                if (becomingMaster == false || publicationInProgress()) {
                    final ClusterState lastPublishedState
                        = currentPublication.map(Publication::publishedState).orElse(coordinationState.get().getLastAcceptedState());
                    lastPublishedState.nodes().forEach(lastPublishedNodes::add);
                    assert lastPublishedNodes.remove(getLocalNode());
                }
                assert lastPublishedNodes.equals(knownFollowers) : lastPublishedNodes + " != " + knownFollowers;
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert leaderCheckScheduler != null;
                assert followersChecker.getKnownFollowers().isEmpty();
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert leaderCheckScheduler == null : leaderCheckScheduler;
                assert followersChecker.getKnownFollowers().isEmpty();
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

    ClusterState getStateForMasterService() {
        synchronized (mutex) {
            // expose last accepted cluster state as base state upon which the master service
            // speculatively calculates the next cluster state update
            final ClusterState clusterState = coordinationState.get().getLastAcceptedState();
            if (mode != Mode.LEADER || clusterState.term() != getCurrentTerm()) {
                // the master service checks if the local node is the master node in order to fail execution of the state update early
                return clusterStateWithNoMasterBlock(clusterState);
            }
            return clusterState;
        }
    }

    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        if (clusterState.nodes().getMasterNodeId() != null) {
            // remove block if it already exists before adding new one
            assert clusterState.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID) == false :
                "NO_MASTER_BLOCK should only be added by Coordinator";
            // TODO: allow dynamically configuring NO_MASTER_BLOCK_ALL
            final ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks()).addGlobalBlock(
                DiscoverySettings.NO_MASTER_BLOCK_WRITES).build();
            final DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
            return ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(discoveryNodes).build();
        } else {
            return clusterState;
        }
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        try {
            synchronized (mutex) {
                assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

                if (mode != Mode.LEADER) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed publication as not currently leading",
                        clusterChangedEvent.source()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("node stepped down as leader during publication"));
                    return;
                }

                if (currentPublication.isPresent()) {
                    assert false : "[" + currentPublication.get() + "] in progress, cannot start new publication";
                    logger.warn(() -> new ParameterizedMessage("[{}] failed publication as already publication in progress",
                        clusterChangedEvent.source()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("publication " + currentPublication.get() +
                        " already in progress"));
                    return;
                }

                // there is no equals on cluster state, so we just serialize it to XContent and compare JSON representation
                assert clusterChangedEvent.previousState() == coordinationState.get().getLastAcceptedState() ||
                    Strings.toString(clusterChangedEvent.previousState()).equals(
                        Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())));

                final ClusterState clusterState = clusterChangedEvent.state();

                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId())) :
                    getLocalNode() + " should be in published " + clusterState;

                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);

                final ListenableFuture<Void> localNodeAckEvent = new ListenableFuture<>();
                final AckListener wrappedAckListener = new AckListener() {
                    @Override
                    public void onCommit(TimeValue commitTime) {
                        ackListener.onCommit(commitTime);
                    }

                    @Override
                    public void onNodeAck(DiscoveryNode node, Exception e) {
                        // acking and cluster state application for local node is handled specially
                        if (node.equals(getLocalNode())) {
                            synchronized (mutex) {
                                if (e == null) {
                                    localNodeAckEvent.onResponse(null);
                                } else {
                                    localNodeAckEvent.onFailure(e);
                                }
                            }
                        } else {
                            ackListener.onNodeAck(node, e);
                        }
                    }
                };

                final Publication publication = new Publication(settings, publishRequest, wrappedAckListener,
                    transportService.getThreadPool()::relativeTimeInMillis) {

                    @Override
                    protected void onCompletion(boolean committed) {
                        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                        assert currentPublication.get() == this;
                        currentPublication = Optional.empty();
                        updateMaxTermSeen(getCurrentTerm()); // triggers term bump if new term was found during publication

                        localNodeAckEvent.addListener(new ActionListener<Void>() {
                            @Override
                            public void onResponse(Void ignore) {
                                assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                                assert coordinationState.get().getLastAcceptedTerm() == publishRequest.getAcceptedState().term()
                                    && coordinationState.get().getLastAcceptedVersion() == publishRequest.getAcceptedState().version()
                                    : "onPossibleCompletion: term or version mismatch when publishing [" + this
                                    + "]: current version is now [" + coordinationState.get().getLastAcceptedVersion()
                                    + "] in term [" + coordinationState.get().getLastAcceptedTerm() + "]";
                                assert committed;

                                // TODO: send to applier
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                                if (publishRequest.getAcceptedState().term() == coordinationState.get().getCurrentTerm() &&
                                    publishRequest.getAcceptedState().version() == coordinationState.get().getLastPublishedVersion()) {
                                    becomeCandidate("Publication.onCompletion(false)");
                                }
                                FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException(
                                    "publication failed", e);
                                ackListener.onNodeAck(getLocalNode(), exception); // other nodes have acked, but not the master.
                                publishListener.onFailure(exception);
                            }
                        }, transportService.getThreadPool().generic());
                    }

                    @Override
                    protected boolean isPublishQuorum(CoordinationState.VoteCollection votes) {
                        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                        return coordinationState.get().isPublishQuorum(votes);
                    }

                    @Override
                    protected Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode,
                                                                                 PublishResponse publishResponse) {
                        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                        assert getCurrentTerm() >= publishResponse.getTerm();
                        return coordinationState.get().handlePublishResponse(sourceNode, publishResponse);
                    }

                    @Override
                    protected void onJoin(Join join) {
                        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                        if (join.getTerm() == getCurrentTerm()) {
                            handleJoin(join);
                        }
                        // TODO: what to do on missing join?
                    }

                    @Override
                    protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                                      ActionListener<PublishWithJoinResponse> responseActionListener) {
                        publicationHandler.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
                    }

                    @Override
                    protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                                   ActionListener<Empty> responseActionListener) {
                        publicationHandler.sendApplyCommit(destination, applyCommit, wrapWithMutex(responseActionListener));
                    }
                };

                assert currentPublication.isPresent() == false
                    : "[" + currentPublication.get() + "] in progress, cannot start [" + publication + ']';
                currentPublication = Optional.of(publication);

                transportService.getThreadPool().schedule(publishTimeout, Names.GENERIC, new Runnable() {
                    @Override
                    public void run() {
                        synchronized (mutex) {
                            publication.onTimeout();
                        }
                    }

                    @Override
                    public String toString() {
                        return "scheduled timeout for " + publication;
                    }
                });

                leaderChecker.setCurrentNodes(publishRequest.getAcceptedState().nodes());
                followersChecker.setCurrentNodes(publishRequest.getAcceptedState().nodes());
                publication.start(followersChecker.getFaultyNodes());
            }
        } catch (Exception e) {
            logger.debug(() -> new ParameterizedMessage("[{}] publishing failed", clusterChangedEvent.source()), e);
            publishListener.onFailure(new FailedToCommitClusterStateException("publishing failed", e));
        }
    }

    private <T> ActionListener<T> wrapWithMutex(ActionListener<T> listener) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T t) {
                synchronized (mutex) {
                    listener.onResponse(t);
                }
            }

            @Override
            public void onFailure(Exception e) {
                synchronized (mutex) {
                    listener.onFailure(e);
                }
            }
        };
    }

    private void cancelActivePublication() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (currentPublication.isPresent()) {
            currentPublication.get().onTimeout();
            assert currentPublication.isPresent() == false;
        }
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
            synchronized (mutex) {
                ensureTermAtLeast(masterNode, term);
                joinHelper.sendJoinRequest(masterNode, joinWithDestination(lastJoin, masterNode, term));
            }
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
                            electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
                                @Override
                                public void run() {
                                    synchronized (mutex) {
                                        if (mode == Mode.CANDIDATE) {
                                            if (prevotingRound != null) {
                                                prevotingRound.close();
                                            }
                                            prevotingRound = preVoteCollector.start(lastAcceptedState, getDiscoveredNodes());
                                        }
                                    }
                                }

                                @Override
                                public String toString() {
                                    return "scheduling of new prevoting round";
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
