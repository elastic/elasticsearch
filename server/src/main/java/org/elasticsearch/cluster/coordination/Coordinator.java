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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.coordination.JoinHelper.InitialJoinAccumulator;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.UnicastConfiguredHostsResolver;
import org.elasticsearch.discovery.zen.PendingClusterStateStats;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_WRITES;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class Coordinator extends AbstractLifecycleComponent implements Discovery {
    private static final Logger logger = LogManager.getLogger(Coordinator.class);

    // the timeout for the publication of each value
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final Settings settings;
    private final TransportService transportService;
    private final MasterService masterService;
    private final JoinHelper joinHelper;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    // TODO: the following two fields are package-private as some tests require access to them
    // These tests can be rewritten to use public methods once Coordinator is more feature-complete
    final Object mutex = new Object();
    final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)
    private volatile ClusterState applierState; // the state that should be exposed to the cluster state applier

    private final PeerFinder peerFinder;
    private final PreVoteCollector preVoteCollector;
    private final ElectionSchedulerFactory electionSchedulerFactory;
    private final UnicastConfiguredHostsResolver configuredHostsResolver;
    private final TimeValue publishTimeout;
    private final PublicationTransportHandler publicationHandler;
    private final LeaderChecker leaderChecker;
    private final FollowersChecker followersChecker;
    private final ClusterApplier clusterApplier;
    @Nullable
    private Releasable electionScheduler;
    @Nullable
    private Releasable prevotingRound;
    private long maxTermSeen;
    private final Reconfigurator reconfigurator;
    private final ClusterBootstrapService clusterBootstrapService;
    private final LagDetector lagDetector;
    private final ClusterFormationFailureHelper clusterFormationFailureHelper;

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private JoinHelper.JoinAccumulator joinAccumulator;
    private Optional<CoordinatorPublication> currentPublication = Optional.empty();

    private final Set<Consumer<Iterable<DiscoveryNode>>> discoveredNodesListeners = newConcurrentSet();

    public Coordinator(String nodeName, Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                       NamedWriteableRegistry namedWriteableRegistry, AllocationService allocationService, MasterService masterService,
                       Supplier<CoordinationState.PersistedState> persistedStateSupplier, UnicastHostsProvider unicastHostsProvider,
                       ClusterApplier clusterApplier, Random random) {
        super(settings);
        this.settings = settings;
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
        this.preVoteCollector = new PreVoteCollector(transportService, this::startElection, this::updateMaxTermSeen);
        configuredHostsResolver = new UnicastConfiguredHostsResolver(nodeName, settings, transportService, unicastHostsProvider);
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry,
            this::handlePublishRequest, this::handleApplyCommit);
        this.leaderChecker = new LeaderChecker(settings, transportService, getOnLeaderFailure());
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::removeNode);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.clusterApplier = clusterApplier;
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        this.reconfigurator = new Reconfigurator(settings, clusterSettings);
        this.clusterBootstrapService = new ClusterBootstrapService(settings, transportService);
        this.lagDetector = new LagDetector(settings, transportService.getThreadPool(), n -> removeNode(n, "lagging"),
            transportService::getLocalNode);
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(settings, this::getClusterFormationState,
            transportService.getThreadPool());
    }

    private ClusterFormationState getClusterFormationState() {
        return new ClusterFormationState(settings, getStateForMasterService(), peerFinder.getLastResolvedAddresses(),
            StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false).collect(Collectors.toList()));
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

    private void removeNode(DiscoveryNode discoveryNode, String reason) {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                masterService.submitStateUpdateTask("node-left",
                    new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason),
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

    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            final ClusterState committedState = hideStateIfNotRecovered(coordinationState.get().getLastAcceptedState());
            applierState = mode == Mode.CANDIDATE ? clusterStateWithNoMasterBlock(committedState) : committedState;
            if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
                // master node applies the committed state at the end of the publication process, not here.
                applyListener.onResponse(null);
            } else {
                clusterApplier.onNewClusterState(applyCommitRequest.toString(), () -> applierState,
                    new ClusterApplyListener() {

                        @Override
                        public void onFailure(String source, Exception e) {
                            applyListener.onFailure(e);
                        }

                        @Override
                        public void onSuccess(String source) {
                            applyListener.onResponse(null);
                        }
                    });
            }
        }
    }

    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode()) :
            publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

        synchronized (mutex) {
            final DiscoveryNode sourceNode = publishRequest.getAcceptedState().nodes().getMasterNode();
            logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

            if (sourceNode.equals(getLocalNode()) && mode != Mode.LEADER) {
                // Rare case in which we stood down as leader between starting this publication and receiving it ourselves. The publication
                // is already failed so there is no point in proceeding.
                throw new CoordinationStateRejectedException("no longer leading this publication's term: " + publishRequest);
            }

            ensureTermAtLeast(sourceNode, publishRequest.getAcceptedState().term());
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            if (sourceNode.equals(getLocalNode())) {
                preVoteCollector.update(getPreVoteResponse(), getLocalNode());
            } else {
                becomeFollower("handlePublishRequest", sourceNode); // also updates preVoteCollector
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
        synchronized (mutex) {
            maxTermSeen = Math.max(maxTermSeen, term);
            final long currentTerm = getCurrentTerm();
            if (mode == Mode.LEADER && maxTermSeen > currentTerm) {
                // Bump our term. However if there is a publication in flight then doing so would cancel the publication, so don't do that
                // since we check whether a term bump is needed at the end of the publication too.
                if (publicationInProgress()) {
                    logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, enqueueing term bump",
                        maxTermSeen, currentTerm);
                } else {
                    try {
                        ensureTermAtLeast(getLocalNode(), maxTermSeen);
                        startElection();
                    } catch (Exception e) {
                        logger.warn(new ParameterizedMessage("failed to bump term to {}", maxTermSeen), e);
                        becomeCandidate("updateMaxTermSeen");
                    }
                }
            }
        }
    }

    private void startElection() {
        synchronized (mutex) {
            // The preVoteCollector is only active while we are candidate, but it does not call this method with synchronisation, so we have
            // to check our mode again here.
            if (mode == Mode.CANDIDATE) {
                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen) + 1);
                logger.debug("starting election with {}", startJoinRequest);
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
                becomeCandidate("joinLeaderInTerm"); // updates followersChecker and preVoteCollector
            } else {
                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
                preVoteCollector.update(getPreVoteResponse(), null);
            }
            return join;
        }
    }

    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";
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
            clusterFormationFailureHelper.start();
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderChecker.updateLeader(null);

            followersChecker.clearCurrentNodes();
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
            lagDetector.clearTrackedNodes();

            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, (source, e) -> {
                });
            }
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        lastKnownLeader = Optional.of(getLocalNode());
        peerFinder.deactivate(getLocalNode());
        clusterFormationFailureHelper.stop();
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        assert leaderChecker.leader() == null : leaderChecker.leader();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert leaderNode.isMasterNode() : leaderNode + " became a leader but is not master-eligible";

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
        clusterFormationFailureHelper.stop();
        closePrevotingAndElectionScheduler();
        cancelActivePublication();
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        if (restartLeaderChecker) {
            leaderChecker.updateLeader(leaderNode);
        }

        followersChecker.clearCurrentNodes();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        lagDetector.clearTrackedNodes();
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

    // visible for testing
    public DiscoveryNode getLocalNode() {
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
        synchronized (mutex) {
            CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
            coordinationState.set(new CoordinationState(settings, getLocalNode(), persistedState));
            peerFinder.setCurrentTerm(getCurrentTerm());
            configuredHostsResolver.start();
            ClusterState initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(NO_MASTER_BLOCK_WRITES)) // TODO: allow dynamically configuring NO_MASTER_BLOCK_ALL
                .nodes(DiscoveryNodes.builder().add(getLocalNode()).localNodeId(getLocalNode().getId()))
                .build();
            applierState = initialState;
            clusterApplier.setInitialState(initialState);
        }
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(new PendingClusterStateStats(0, 0, 0), publicationHandler.stats());
    }

    @Override
    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }

        if (isInitialConfigurationSet() == false) {
            clusterBootstrapService.start();
        }
    }

    @Override
    protected void doStop() {
        configuredHostsResolver.stop();
        clusterBootstrapService.stop();
    }

    @Override
    protected void doClose() {
    }

    public void invariant() {
        synchronized (mutex) {
            final Optional<DiscoveryNode> peerFinderLeader = peerFinder.getLeader();
            assert peerFinder.getCurrentTerm() == getCurrentTerm();
            assert followersChecker.getFastResponseState().term == getCurrentTerm() : followersChecker.getFastResponseState();
            assert followersChecker.getFastResponseState().mode == getMode() : followersChecker.getFastResponseState();
            assert (applierState.nodes().getMasterNodeId() == null) == applierState.blocks().hasGlobalBlock(NO_MASTER_BLOCK_WRITES.id());
            assert preVoteCollector.getPreVoteResponse().equals(getPreVoteResponse())
                : preVoteCollector + " vs " + getPreVoteResponse();

            assert lagDetector.getTrackedNodes().contains(getLocalNode()) == false : lagDetector.getTrackedNodes();
            assert followersChecker.getKnownFollowers().equals(lagDetector.getTrackedNodes())
                : followersChecker.getKnownFollowers() + " vs " + lagDetector.getTrackedNodes();

            if (mode == Mode.LEADER) {
                final boolean becomingMaster = getStateForMasterService().term() != getCurrentTerm();

                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert becomingMaster || getStateForMasterService().nodes().getMasterNodeId() != null : getStateForMasterService();
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert applierState.nodes().getMasterNodeId() == null || getLocalNode().equals(applierState.nodes().getMasterNode());
                assert preVoteCollector.getLeader() == getLocalNode() : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;

                final boolean activePublication = currentPublication.map(CoordinatorPublication::isActiveForCurrentLeader).orElse(false);
                if (becomingMaster && activePublication == false) {
                    // cluster state update task to become master is submitted to MasterService, but publication has not started yet
                    assert followersChecker.getKnownFollowers().isEmpty() : followersChecker.getKnownFollowers();
                } else {
                    final ClusterState lastPublishedState;
                    if (activePublication) {
                        // active publication in progress: followersChecker is up-to-date with nodes that we're actively publishing to
                        lastPublishedState = currentPublication.get().publishedState();
                    } else {
                        // no active publication: followersChecker is up-to-date with the nodes of the latest publication
                        lastPublishedState = coordinationState.get().getLastAcceptedState();
                    }
                    final Set<DiscoveryNode> lastPublishedNodes = new HashSet<>();
                    lastPublishedState.nodes().forEach(lastPublishedNodes::add);
                    assert lastPublishedNodes.remove(getLocalNode()); // followersChecker excludes local node
                    assert lastPublishedNodes.equals(followersChecker.getKnownFollowers()) :
                        lastPublishedNodes + " != " + followersChecker.getKnownFollowers();
                }

                assert becomingMaster || activePublication ||
                    coordinationState.get().getLastAcceptedConfiguration().equals(coordinationState.get().getLastCommittedConfiguration())
                    : coordinationState.get().getLastAcceptedConfiguration() + " != "
                    + coordinationState.get().getLastCommittedConfiguration();

            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert lastKnownLeader.equals(Optional.of(leaderChecker.leader()));
                assert followersChecker.getKnownFollowers().isEmpty();
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader().equals(lastKnownLeader.get()) : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
                assert getStateForMasterService().nodes().getMasterNodeId() == null : getStateForMasterService();
                assert leaderChecker.currentNodeIsMaster() == false;
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert followersChecker.getKnownFollowers().isEmpty();
                assert applierState.nodes().getMasterNodeId() == null;
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader() == null : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning();
            }
        }
    }

    public boolean isInitialConfigurationSet() {
        return getStateForMasterService().getLastAcceptedConfiguration().isEmpty() == false;
    }

    /**
     * Sets the initial configuration by resolving the given {@link BootstrapConfiguration} to concrete nodes. This method is safe to call
     * more than once, as long as each call's bootstrap configuration resolves to the same set of nodes.
     *
     * @param bootstrapConfiguration A description of the nodes that should form the initial configuration.
     * @return whether this call successfully set the initial configuration - if false, the cluster has already been bootstrapped.
     */
    public boolean setInitialConfiguration(final BootstrapConfiguration bootstrapConfiguration) {
        final List<DiscoveryNode> selfAndDiscoveredPeers = new ArrayList<>();
        selfAndDiscoveredPeers.add(getLocalNode());
        getFoundPeers().forEach(selfAndDiscoveredPeers::add);
        final VotingConfiguration votingConfiguration = bootstrapConfiguration.resolve(selfAndDiscoveredPeers);
        return setInitialConfiguration(votingConfiguration);
    }

    /**
     * Sets the initial configuration to the given {@link VotingConfiguration}. This method is safe to call
     * more than once, as long as the argument to each call is the same.
     *
     * @param votingConfiguration The nodes that should form the initial configuration.
     * @return whether this call successfully set the initial configuration - if false, the cluster has already been bootstrapped.
     */
    public boolean setInitialConfiguration(final VotingConfiguration votingConfiguration) {
        synchronized (mutex) {
            final ClusterState currentState = getStateForMasterService();

            if (isInitialConfigurationSet()) {
                return false;
            }

            assert currentState.term() == 0 : currentState;
            assert currentState.version() == 0 : currentState;

            if (mode != Mode.CANDIDATE) {
                throw new CoordinationStateRejectedException("Cannot set initial configuration in mode " + mode);
            }

            final List<DiscoveryNode> knownNodes = new ArrayList<>();
            knownNodes.add(getLocalNode());
            peerFinder.getFoundPeers().forEach(knownNodes::add);
            if (votingConfiguration.hasQuorum(knownNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList())) == false) {
                throw new CoordinationStateRejectedException("not enough nodes discovered to form a quorum in the initial configuration " +
                    "[knownNodes=" + knownNodes + ", " + votingConfiguration + "]");
            }

            logger.info("setting initial configuration to {}", votingConfiguration);
            final Builder builder = masterService.incrementVersion(currentState);
            final CoordinationMetaData coordinationMetaData = CoordinationMetaData.builder(currentState.coordinationMetaData())
                .lastAcceptedConfiguration(votingConfiguration)
                .lastCommittedConfiguration(votingConfiguration)
                .build();

            MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
            // automatically generate a UID for the metadata if we need to
            metaDataBuilder.generateClusterUuidIfNeeded(); // TODO generate UUID in bootstrapping tool?
            metaDataBuilder.coordinationMetaData(coordinationMetaData);
            builder.metaData(metaDataBuilder);
            coordinationState.get().setInitialState(builder.build());
            preVoteCollector.update(getPreVoteResponse(), null); // pick up the change to last-accepted version
            startElectionScheduler();
            return true;
        }
    }

    // Package-private for testing
    ClusterState improveConfiguration(ClusterState clusterState) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

        final Set<DiscoveryNode> liveNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(this::hasJoinVoteFrom).collect(Collectors.toSet());
        final VotingConfiguration newConfig = reconfigurator.reconfigure(liveNodes,
            clusterState.getVotingConfigExclusions().stream().map(VotingConfigExclusion::getNodeId).collect(Collectors.toSet()),
            clusterState.getLastAcceptedConfiguration());
        if (newConfig.equals(clusterState.getLastAcceptedConfiguration()) == false) {
            assert coordinationState.get().joinVotesHaveQuorumFor(newConfig);
            return ClusterState.builder(clusterState).metaData(MetaData.builder(clusterState.metaData())
                .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData())
                    .lastAcceptedConfiguration(newConfig).build())).build();
        }
        return clusterState;
    }

    private AtomicBoolean reconfigurationTaskScheduled = new AtomicBoolean();

    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        final ClusterState state = getLastAcceptedState();
        if (improveConfiguration(state) != state && reconfigurationTaskScheduled.compareAndSet(false, true)) {
            logger.trace("scheduling reconfiguration");
            masterService.submitStateUpdateTask("reconfigure", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    reconfigurationTaskScheduled.set(false);
                    synchronized (mutex) {
                        return improveConfiguration(currentState);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    reconfigurationTaskScheduled.set(false);
                    logger.debug("reconfiguration failed", e);
                }
            });
        }
    }

    // for tests
    boolean hasJoinVoteFrom(DiscoveryNode node) {
        return coordinationState.get().containsJoinVoteFor(node);
    }

    private void handleJoin(Join join) {
        synchronized (mutex) {
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            if (coordinationState.get().electionWon()) {
                // If we have already won the election then the actual join does not matter for election purposes, so swallow any exception
                final boolean isNewJoin = handleJoinIgnoringExceptions(join);

                // If we haven't completely finished becoming master then there's already a publication scheduled which will, in turn,
                // schedule a reconfiguration if needed. It's benign to schedule a reconfiguration anyway, but it might fail if it wins the
                // race against the election-winning publication and log a big error message, which we can prevent by checking this here:
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                if (isNewJoin && establishedAsMaster && publicationInProgress() == false) {
                    scheduleReconfigurationIfNeeded();
                }
            } else {
                coordinationState.get().handleJoin(join); // this might fail and bubble up the exception
            }
        }
    }

    /**
     * @return true iff the join was from a new node and was successfully added
     */
    private boolean handleJoinIgnoringExceptions(Join join) {
        try {
            return coordinationState.get().handleJoin(join);
        } catch (CoordinationStateRejectedException e) {
            logger.debug(new ParameterizedMessage("failed to add {} - ignoring", join), e);
            return false;
        }
    }

    public ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    @Nullable
    public ClusterState getApplierState() {
        return applierState;
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
                NO_MASTER_BLOCK_WRITES).build();
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
                        Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())))
                    : Strings.toString(clusterChangedEvent.previousState()) + " vs "
                    + Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState()));

                final ClusterState clusterState = clusterChangedEvent.state();

                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId())) :
                    getLocalNode() + " should be in published " + clusterState;

                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);
                final PublicationTransportHandler.PublicationContext publicationContext =
                    publicationHandler.newPublicationContext(clusterChangedEvent);
                final CoordinatorPublication publication = new CoordinatorPublication(publishRequest, publicationContext,
                    new ListenableFuture<>(), ackListener, publishListener);
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

                final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                leaderChecker.setCurrentNodes(publishNodes);
                followersChecker.setCurrentNodes(publishNodes);
                lagDetector.setTrackedNodes(publishNodes);
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
            final Iterable<DiscoveryNode> foundPeers;
            synchronized (mutex) {
                foundPeers = getFoundPeers();
                if (mode == Mode.CANDIDATE) {
                    final CoordinationState.VoteCollection expectedVotes = new CoordinationState.VoteCollection();
                    foundPeers.forEach(expectedVotes::addVote);
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
                    final boolean foundQuorum = CoordinationState.isElectionQuorum(expectedVotes, lastAcceptedState);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            startElectionScheduler();
                        }
                    } else {
                        closePrevotingAndElectionScheduler();
                    }
                }
            }

            for (Consumer<Iterable<DiscoveryNode>> discoveredNodesListener : discoveredNodesListeners) {
                discoveredNodesListener.accept(foundPeers);
            }
        }
    }

    private void startElectionScheduler() {
        assert electionScheduler == null : electionScheduler;

        if (getLocalNode().isMasterNode() == false) {
            return;
        }

        final TimeValue gracePeriod = TimeValue.ZERO; // TODO variable grace period
        electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    if (mode == Mode.CANDIDATE) {
                        if (prevotingRound != null) {
                            prevotingRound.close();
                        }
                        final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
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

    public Releasable withDiscoveryListener(Consumer<Iterable<DiscoveryNode>> listener) {
        discoveredNodesListeners.add(listener);
        return () -> {
            boolean removed = discoveredNodesListeners.remove(listener);
            assert removed : listener;
        };
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        // TODO everyone takes this and adds the local node. Maybe just add the local node here?
        return peerFinder.getFoundPeers();
    }

    class CoordinatorPublication extends Publication {

        private final PublishRequest publishRequest;
        private final ListenableFuture<Void> localNodeAckEvent;
        private final AckListener ackListener;
        private final ActionListener<Void> publishListener;
        private final PublicationTransportHandler.PublicationContext publicationContext;

        // We may not have accepted our own state before receiving a join from another node, causing its join to be rejected (we cannot
        // safely accept a join whose last-accepted term/version is ahead of ours), so store them up and process them at the end.
        private final List<Join> receivedJoins = new ArrayList<>();
        private boolean receivedJoinsProcessed;

        CoordinatorPublication(PublishRequest publishRequest, PublicationTransportHandler.PublicationContext publicationContext,
                               ListenableFuture<Void> localNodeAckEvent, AckListener ackListener, ActionListener<Void> publishListener) {
            super(publishRequest,
                new AckListener() {
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
                            if (e == null) {
                                lagDetector.setAppliedVersion(node, publishRequest.getAcceptedState().version());
                            }
                        }
                    }
                },
                transportService.getThreadPool()::relativeTimeInMillis);
            this.publishRequest = publishRequest;
            this.publicationContext = publicationContext;
            this.localNodeAckEvent = localNodeAckEvent;
            this.ackListener = ackListener;
            this.publishListener = publishListener;
        }

        private void removePublicationAndPossiblyBecomeCandidate(String reason) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            assert currentPublication.get() == this;
            currentPublication = Optional.empty();
            logger.debug("publication ended unsuccessfully: {}", this);

            // check if node has not already switched modes (by bumping term)
            if (isActiveForCurrentLeader()) {
                becomeCandidate(reason);
            }
        }

        boolean isActiveForCurrentLeader() {
            // checks if this publication can still influence the mode of the current publication
            return mode == Mode.LEADER && publishRequest.getAcceptedState().term() == getCurrentTerm();
        }

        @Override
        protected void onCompletion(boolean committed) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

            localNodeAckEvent.addListener(new ActionListener<Void>() {
                @Override
                public void onResponse(Void ignore) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    assert committed;

                    receivedJoins.forEach(CoordinatorPublication.this::handleAssociatedJoin);
                    assert receivedJoinsProcessed == false;
                    receivedJoinsProcessed = true;

                    clusterApplier.onNewClusterState(CoordinatorPublication.this.toString(), () -> applierState,
                        new ClusterApplyListener() {
                            @Override
                            public void onFailure(String source, Exception e) {
                                synchronized (mutex) {
                                    removePublicationAndPossiblyBecomeCandidate("clusterApplier#onNewClusterState");
                                }
                                ackListener.onNodeAck(getLocalNode(), e);
                                publishListener.onFailure(e);
                            }

                            @Override
                            public void onSuccess(String source) {
                                synchronized (mutex) {
                                    assert currentPublication.get() == CoordinatorPublication.this;
                                    currentPublication = Optional.empty();
                                    logger.debug("publication ended successfully: {}", CoordinatorPublication.this);
                                    // trigger term bump if new term was found during publication
                                    updateMaxTermSeen(getCurrentTerm());

                                    if (mode == Mode.LEADER) {
                                        scheduleReconfigurationIfNeeded();
                                    }
                                    lagDetector.startLagDetector(publishRequest.getAcceptedState().version());
                                }
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }
                        });
                }

                @Override
                public void onFailure(Exception e) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");

                    final FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException("publication failed", e);
                    ackListener.onNodeAck(getLocalNode(), exception); // other nodes have acked, but not the master.
                    publishListener.onFailure(exception);
                }
            }, EsExecutors.newDirectExecutorService(), transportService.getThreadPool().getThreadContext());
        }

        private void handleAssociatedJoin(Join join) {
            if (join.getTerm() == getCurrentTerm() && hasJoinVoteFrom(join.getSourceNode()) == false) {
                logger.trace("handling {}", join);
                handleJoin(join);
            }
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
            if (receivedJoinsProcessed) {
                // a late response may arrive after the state has been locally applied, meaning that receivedJoins has already been
                // processed, so we have to handle this late response here.
                handleAssociatedJoin(join);
            } else {
                receivedJoins.add(join);
            }
        }

        @Override
        protected void onMissingJoin(DiscoveryNode discoveryNode) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            // The remote node did not include a join vote in its publish response. We do not persist joins, so it could be that the remote
            // node voted for us and then rebooted, or it could be that it voted for a different node in this term. If we don't have a copy
            // of a join from this node then we assume the latter and bump our term to obtain a vote from this node.
            if (hasJoinVoteFrom(discoveryNode) == false) {
                final long term = publishRequest.getAcceptedState().term();
                logger.debug("onMissingJoin: no join vote from {}, bumping term to exceed {}", discoveryNode, term);
                updateMaxTermSeen(term + 1);
            }
        }

        @Override
        protected void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                          ActionListener<PublishWithJoinResponse> responseActionListener) {
            publicationContext.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
        }

        @Override
        protected void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                       ActionListener<Empty> responseActionListener) {
            publicationContext.sendApplyCommit(destination, applyCommit, wrapWithMutex(responseActionListener));
        }
    }

    // TODO: only here temporarily for BWC development, remove once complete
    public static Settings.Builder addZen1Attribute(Settings.Builder builder) {
        return builder.put("node.attr.zen1", true);
    }

    // TODO: only here temporarily for BWC development, remove once complete
    public static boolean isZen1Node(DiscoveryNode discoveryNode) {
        return discoveryNode.getVersion().before(Version.V_7_0_0) ||
            discoveryNode.getAttributes().containsKey("zen1");
    }
}
