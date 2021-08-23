/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.coordination.FollowersChecker.FollowerCheckRequest;
import org.elasticsearch.cluster.coordination.JoinHelper.InitialJoinAccumulator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class Coordinator extends AbstractLifecycleComponent implements Discovery {

    private static final Logger logger = LogManager.getLogger(Coordinator.class);

    // the timeout before emitting an info log about a slow-running publication
    public static final Setting<TimeValue> PUBLISH_INFO_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.info_timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // the timeout for the publication of each value
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.publish.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final Settings settings;
    private final boolean singleNodeDiscovery;
    private final ElectionStrategy electionStrategy;
    private final TransportService transportService;
    private final MasterService masterService;
    private final AllocationService allocationService;
    private final JoinHelper joinHelper;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    private final NoMasterBlockService noMasterBlockService;
    final Object mutex = new Object(); // package-private to allow tests to call methods that assert that the mutex is held
    private final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)
    private volatile ClusterState applierState; // the state that should be exposed to the cluster state applier

    private final PeerFinder peerFinder;
    private final PreVoteCollector preVoteCollector;
    private final Random random;
    private final ElectionSchedulerFactory electionSchedulerFactory;
    private final SeedHostsResolver configuredHostsResolver;
    private final TimeValue publishTimeout;
    private final TimeValue publishInfoTimeout;
    private final PublicationTransportHandler publicationHandler;
    private final LeaderChecker leaderChecker;
    private final FollowersChecker followersChecker;
    private final ClusterApplier clusterApplier;
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;
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
    private final NodeHealthService nodeHealthService;

    /**
     * @param nodeName The name of the node, used to name the {@link java.util.concurrent.ExecutorService} of the {@link SeedHostsResolver}.
     * @param onJoinValidators A collection of join validators to restrict which nodes may join the cluster.
     */
    public Coordinator(String nodeName, Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                       NamedWriteableRegistry namedWriteableRegistry, AllocationService allocationService, MasterService masterService,
                       Supplier<CoordinationState.PersistedState> persistedStateSupplier, SeedHostsProvider seedHostsProvider,
                       ClusterApplier clusterApplier, Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators, Random random,
                       RerouteService rerouteService, ElectionStrategy electionStrategy, NodeHealthService nodeHealthService) {
        this.settings = settings;
        this.transportService = transportService;
        this.masterService = masterService;
        this.allocationService = allocationService;
        this.onJoinValidators = JoinTaskExecutor.addBuiltInJoinValidators(onJoinValidators);
        this.singleNodeDiscovery = DiscoveryModule.isSingleNodeDiscovery(settings);
        this.electionStrategy = electionStrategy;
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService, this::getCurrentTerm,
            this::getStateForMasterService, this::handleJoinRequest, this::joinLeaderInTerm, this.onJoinValidators, rerouteService,
            nodeHealthService);
        this.persistedStateSupplier = persistedStateSupplier;
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = new InitialJoinAccumulator();
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        this.publishInfoTimeout = PUBLISH_INFO_TIMEOUT_SETTING.get(settings);
        this.random = random;
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        this.preVoteCollector = new PreVoteCollector(transportService, this::startElection, this::updateMaxTermSeen, electionStrategy,
            nodeHealthService);
        configuredHostsResolver = new SeedHostsResolver(nodeName, settings, transportService, seedHostsProvider);
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
            new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry,
            this::handlePublishRequest, this::handleApplyCommit);
        this.leaderChecker = new LeaderChecker(settings, transportService, this::onLeaderFailure, nodeHealthService);
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::removeNode,
            nodeHealthService);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.clusterApplier = clusterApplier;
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        this.reconfigurator = new Reconfigurator(settings, clusterSettings);
        this.clusterBootstrapService = new ClusterBootstrapService(settings, transportService, this::getFoundPeers,
            this::isInitialConfigurationSet, this::setInitialConfiguration);
        this.lagDetector = new LagDetector(settings, transportService.getThreadPool(), n -> removeNode(n, "lagging"),
            transportService::getLocalNode);
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(settings, this::getClusterFormationState,
            transportService.getThreadPool(), joinHelper::logLastFailedJoinAttempt);
        this.nodeHealthService = nodeHealthService;
    }

    private ClusterFormationState getClusterFormationState() {
        return new ClusterFormationState(settings, getStateForMasterService(), peerFinder.getLastResolvedAddresses(),
            Stream.concat(Stream.of(getLocalNode()), StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false))
                    .collect(Collectors.toList()), getCurrentTerm(), electionStrategy, nodeHealthService.getHealth());
    }

    private void onLeaderFailure(Exception e) {
        synchronized (mutex) {
            if (mode != Mode.CANDIDATE) {
                assert lastKnownLeader.isPresent();
                logger.info(new ParameterizedMessage("master node [{}] failed, restarting discovery", lastKnownLeader.get()), e);
            }
            becomeCandidate("onLeaderFailure");
        }
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

    void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException("onFollowerCheckRequest: current term is ["
                    + getCurrentTerm() + "], rejecting " + followerCheckRequest);
            }

            // check if node has accepted a state in this term already. If not, this node has never committed a cluster state in this
            // term and therefore never removed the NO_MASTER_BLOCK for this term. This logic ensures that we quickly turn a node
            // into follower, even before receiving the first cluster state update, but also don't have to deal with the situation
            // where we would possibly have to remove the NO_MASTER_BLOCK from the applierState when turning a candidate back to follower.
            if (getLastAcceptedState().term() < getCurrentTerm()) {
                becomeFollower("onFollowerCheckRequest", followerCheckRequest.getSender());
            } else if (mode == Mode.FOLLOWER) {
                logger.trace("onFollowerCheckRequest: responding successfully to {}", followerCheckRequest);
            } else if (joinHelper.isJoinPending()) {
                logger.trace("onFollowerCheckRequest: rejoining master, responding successfully to {}", followerCheckRequest);
            } else {
                logger.trace("onFollowerCheckRequest: received check from faulty master, rejecting {}", followerCheckRequest);
                throw new CoordinationStateRejectedException(
                    "onFollowerCheckRequest: received check from faulty master, rejecting " + followerCheckRequest);
            }
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
                        public void onFailure(Exception e) {
                            applyListener.onFailure(e);
                        }

                        @Override
                        public void onSuccess() {
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

            final ClusterState localState = coordinationState.get().getLastAcceptedState();

            if (localState.metadata().clusterUUIDCommitted() &&
                localState.metadata().clusterUUID().equals(publishRequest.getAcceptedState().metadata().clusterUUID()) == false) {
                logger.warn("received cluster state from {} with a different cluster uuid {} than local cluster uuid {}, rejecting",
                    sourceNode, publishRequest.getAcceptedState().metadata().clusterUUID(), localState.metadata().clusterUUID());
                throw new CoordinationStateRejectedException("received cluster state from " + sourceNode +
                    " with a different cluster uuid " + publishRequest.getAcceptedState().metadata().clusterUUID() +
                    " than local cluster uuid " + localState.metadata().clusterUUID() + ", rejecting");
            }

            if (publishRequest.getAcceptedState().term() > localState.term()) {
                // only do join validation if we have not accepted state from this master yet
                onJoinValidators.forEach(a -> a.accept(getLocalNode(), publishRequest.getAcceptedState()));
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
            && lastJoin.get().targetMatches(leader)
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
                    logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, enqueueing term bump", maxTermSeen, currentTerm);
                } else {
                    try {
                        logger.debug("updateMaxTermSeen: maxTermSeen = {} > currentTerm = {}, bumping term", maxTermSeen, currentTerm);
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
                if (localNodeMayWinElection(getLastAcceptedState()) == false) {
                    logger.trace("skip election as local node may not win it: {}", getLastAcceptedState().coordinationMetadata());
                    return;
                }

                final StartJoinRequest startJoinRequest
                    = new StartJoinRequest(getLocalNode(), Math.max(getCurrentTerm(), maxTermSeen) + 1);
                logger.debug("starting election with {}", startJoinRequest);
                getDiscoveredNodes().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
            }
        }
    }

    private void abdicateTo(DiscoveryNode newMaster) {
        assert Thread.holdsLock(mutex);
        assert mode == Mode.LEADER : "expected to be leader on abdication but was " + mode;
        assert newMaster.isMasterNode() : "should only abdicate to master-eligible node but was " + newMaster;
        final StartJoinRequest startJoinRequest = new StartJoinRequest(newMaster, Math.max(getCurrentTerm(), maxTermSeen) + 1);
        logger.info("abdicating to {} with term {}", newMaster, startJoinRequest.getTerm());
        getLastAcceptedState().nodes().mastersFirstStream().forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
        // handling of start join messages on the local node will be dispatched to the generic thread-pool
        assert mode == Mode.LEADER : "should still be leader after sending abdication messages " + mode;
        // explicitly move node to candidate state so that the next cluster state update task yields an onNoLongerMaster event
        becomeCandidate("after abdicating to " + newMaster);
    }

    private static boolean localNodeMayWinElection(ClusterState lastAcceptedState) {
        final DiscoveryNode localNode = lastAcceptedState.nodes().getLocalNode();
        assert localNode != null;
        return nodeMayWinElection(lastAcceptedState, localNode);
    }

    private static boolean nodeMayWinElection(ClusterState lastAcceptedState, DiscoveryNode node) {
        final String nodeId = node.getId();
        return lastAcceptedState.getLastCommittedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getLastAcceptedConfiguration().getNodeIds().contains(nodeId)
            || lastAcceptedState.getVotingConfigExclusions().stream().noneMatch(vce -> vce.getNodeId().equals(nodeId));
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

        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                "] set to [" + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE  + "] discovery"));
            return;
        }

        transportService.connectToNode(joinRequest.getSourceNode(), ActionListener.wrap(ignore -> {
            final ClusterState stateForJoinValidation = getStateForMasterService();

            if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
                onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
                if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                    // we do this in a couple of places including the cluster update thread. This one here is really just best effort
                    // to ensure we fail as fast as possible.
                    JoinTaskExecutor.ensureVersionBarrier(
                        joinRequest.getSourceNode().getVersion(),
                        stateForJoinValidation.getNodes().getMinNodeVersion());
                }
                sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);
            } else {
                processJoinRequest(joinRequest, joinCallback);
            }
        }, joinCallback::onFailure));
    }

    // package private for tests
    void sendValidateJoinRequest(ClusterState stateForJoinValidation, JoinRequest joinRequest,
                                 JoinHelper.JoinCallback joinCallback) {
        // validate the join on the joining node, will throw a failure if it fails the validation
        joinHelper.sendValidateJoinRequest(joinRequest.getSourceNode(), stateForJoinValidation, new ActionListener<Empty>() {
            @Override
            public void onResponse(Empty empty) {
                try {
                    processJoinRequest(joinRequest, joinCallback);
                } catch (Exception e) {
                    joinCallback.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]",
                    joinRequest.getSourceNode()), e);
                joinCallback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            }
        });
    }

    private void processJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        synchronized (mutex) {
            updateMaxTermSeen(joinRequest.getTerm());

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
        logger.debug("{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            final Mode prevMode = mode;
            mode = Mode.CANDIDATE;
            cancelActivePublication("become candidate: " + method);
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());
            clusterFormationFailureHelper.start();

            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderChecker.updateLeader(null);

            followersChecker.clearCurrentNodes();
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
            lagDetector.clearTrackedNodes();

            if (prevMode == Mode.LEADER) {
                cleanMasterService();
            }

            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, e -> {
                });
            }
        }

        preVoteCollector.update(getPreVoteResponse(), null);
    }

    void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        logger.debug("{}: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
            method, getCurrentTerm(), mode, lastKnownLeader);

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
        assert mode != Mode.LEADER : "do not switch to follower from leader (should be candidate first)";

        if (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) {
            logger.trace("{}: coordinator remaining FOLLOWER of [{}] in term {}",
                method, leaderNode, getCurrentTerm());
        } else {
            logger.debug("{}: coordinator becoming FOLLOWER of [{}] in term {} (was {}, lastKnownLeader was [{}])",
                method, leaderNode, getCurrentTerm(), mode, lastKnownLeader);
        }

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
        cancelActivePublication("become follower: " + method);
        preVoteCollector.update(getPreVoteResponse(), leaderNode);

        if (restartLeaderChecker) {
            leaderChecker.updateLeader(leaderNode);
        }

        followersChecker.clearCurrentNodes();
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
        lagDetector.clearTrackedNodes();
    }

    private void cleanMasterService() {
        masterService.submitStateUpdateTask("clean-up after stepping down as master",
            new LocalClusterUpdateTask() {
                @Override
                public void onFailure(String source, Exception e) {
                    // ignore
                    logger.trace("failed to clean-up after stepping down as master", e);
                }

                @Override
                public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                    if (currentState.nodes().isLocalNodeElectedMaster() == false) {
                        allocationService.cleanCaches();
                    }
                    return unchanged();
                }

            });
    }

    private PreVoteResponse getPreVoteResponse() {
        return new PreVoteResponse(getCurrentTerm(), coordinationState.get().getLastAcceptedTerm(),
            coordinationState.get().getLastAcceptedState().version());
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
        synchronized (mutex) {
            CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
            coordinationState.set(new CoordinationState(getLocalNode(), persistedState, electionStrategy));
            peerFinder.setCurrentTerm(getCurrentTerm());
            configuredHostsResolver.start();
            final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
            if (lastAcceptedState.metadata().clusterUUIDCommitted()) {
                logger.info("cluster UUID [{}]", lastAcceptedState.metadata().clusterUUID());
            }
            final VotingConfiguration votingConfiguration = lastAcceptedState.getLastCommittedConfiguration();
            if (singleNodeDiscovery &&
                votingConfiguration.isEmpty() == false &&
                votingConfiguration.hasQuorum(Collections.singleton(getLocalNode().getId())) == false) {
                throw new IllegalStateException("cannot start with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() + "] set to [" +
                    DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE + "] when local node " + getLocalNode() +
                    " does not have quorum in voting configuration " + votingConfiguration);
            }
            ClusterState initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(noMasterBlockService.getNoMasterBlock()))
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
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
    }

    @Override
    protected void doStop() {
        configuredHostsResolver.stop();
    }

    @Override
    protected void doClose() throws IOException {
        final CoordinationState coordinationState = this.coordinationState.get();
        if (coordinationState != null) {
            // This looks like a race that might leak an unclosed CoordinationState if it's created while execution is here, but this method
            // is synchronized on AbstractLifecycleComponent#lifestyle, as is the doStart() method that creates the CoordinationState, so
            // it's all ok.
            synchronized (mutex) {
                coordinationState.close();
            }
        }
    }

    public void invariant() {
        synchronized (mutex) {
            final Optional<DiscoveryNode> peerFinderLeader = peerFinder.getLeader();
            assert peerFinder.getCurrentTerm() == getCurrentTerm();
            assert followersChecker.getFastResponseState().term == getCurrentTerm() : followersChecker.getFastResponseState();
            assert followersChecker.getFastResponseState().mode == getMode() : followersChecker.getFastResponseState();
            assert (applierState.nodes().getMasterNodeId() == null) == applierState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID);
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
                assert getLocalNode().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null && applierState.term() < getCurrentTerm());
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
                assert lastKnownLeader.get().equals(applierState.nodes().getMasterNode()) ||
                    (applierState.nodes().getMasterNodeId() == null &&
                        (applierState.term() < getCurrentTerm() || applierState.version() < getLastAcceptedState().version()));
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
                logger.debug("initial configuration already set, ignoring {}", votingConfiguration);
                return false;
            }

            if (getLocalNode().isMasterNode() == false) {
                logger.debug("skip setting initial configuration as local node is not a master-eligible node");
                throw new CoordinationStateRejectedException(
                    "this node is not master-eligible, but cluster bootstrapping can only happen on a master-eligible node");
            }

            if (votingConfiguration.getNodeIds().contains(getLocalNode().getId()) == false) {
                logger.debug("skip setting initial configuration as local node is not part of initial configuration");
                throw new CoordinationStateRejectedException("local node is not part of initial configuration");
            }

            final List<DiscoveryNode> knownNodes = new ArrayList<>();
            knownNodes.add(getLocalNode());
            peerFinder.getFoundPeers().forEach(knownNodes::add);

            if (votingConfiguration.hasQuorum(knownNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toList())) == false) {
                logger.debug("skip setting initial configuration as not enough nodes discovered to form a quorum in the " +
                    "initial configuration [knownNodes={}, {}]", knownNodes, votingConfiguration);
                throw new CoordinationStateRejectedException("not enough nodes discovered to form a quorum in the initial configuration " +
                    "[knownNodes=" + knownNodes + ", " + votingConfiguration + "]");
            }

            logger.info("setting initial configuration to {}", votingConfiguration);
            final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder(currentState.coordinationMetadata())
                .lastAcceptedConfiguration(votingConfiguration)
                .lastCommittedConfiguration(votingConfiguration)
                .build();

            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            // automatically generate a UID for the metadata if we need to
            metadataBuilder.generateClusterUuidIfNeeded();
            metadataBuilder.coordinationMetadata(coordinationMetadata);

            coordinationState.get().setInitialState(ClusterState.builder(currentState).metadata(metadataBuilder).build());
            assert localNodeMayWinElection(getLastAcceptedState()) :
                "initial state does not allow local node to win election: " + getLastAcceptedState().coordinationMetadata();
            preVoteCollector.update(getPreVoteResponse(), null); // pick up the change to last-accepted version
            startElectionScheduler();
            return true;
        }
    }

    // Package-private for testing
    ClusterState improveConfiguration(ClusterState clusterState) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert validVotingConfigExclusionState(clusterState) : clusterState;

        // exclude any nodes whose ID is in the voting config exclusions list ...
        final Stream<String> excludedNodeIds = clusterState.getVotingConfigExclusions().stream().map(VotingConfigExclusion::getNodeId);
        // ... and also automatically exclude the node IDs of master-ineligible nodes that were previously master-eligible and are still in
        // the voting config. We could exclude all the master-ineligible nodes here, but there could be quite a few of them and that makes
        // the logging much harder to follow.
        final Stream<String> masterIneligibleNodeIdsInVotingConfig = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(n -> n.isMasterNode() == false
                && (clusterState.getLastAcceptedConfiguration().getNodeIds().contains(n.getId())
                || clusterState.getLastCommittedConfiguration().getNodeIds().contains(n.getId())))
            .map(DiscoveryNode::getId);

        final Set<DiscoveryNode> liveNodes = StreamSupport.stream(clusterState.nodes().spliterator(), false)
            .filter(DiscoveryNode::isMasterNode).filter(coordinationState.get()::containsJoinVoteFor).collect(Collectors.toSet());
        final VotingConfiguration newConfig = reconfigurator.reconfigure(liveNodes,
            Stream.concat(masterIneligibleNodeIdsInVotingConfig, excludedNodeIds).collect(Collectors.toSet()),
            getLocalNode(), clusterState.getLastAcceptedConfiguration());

        if (newConfig.equals(clusterState.getLastAcceptedConfiguration()) == false) {
            assert coordinationState.get().joinVotesHaveQuorumFor(newConfig);
            return ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata())
                .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata())
                    .lastAcceptedConfiguration(newConfig).build())).build();
        }
        return clusterState;
    }

    /*
    * Valid Voting Configuration Exclusion state criteria:
    * 1. Every voting config exclusion with an ID of _absent_ should not match any nodes currently in the cluster by name
    * 2. Every voting config exclusion with a name of _absent_ should not match any nodes currently in the cluster by ID
     */
    static boolean validVotingConfigExclusionState(ClusterState clusterState) {
        Set<VotingConfigExclusion> votingConfigExclusions = clusterState.getVotingConfigExclusions();
        Set<String> nodeNamesWithAbsentId = votingConfigExclusions.stream()
                                                .filter(e -> e.getNodeId().equals(VotingConfigExclusion.MISSING_VALUE_MARKER))
                                                .map(VotingConfigExclusion::getNodeName)
                                                .collect(Collectors.toSet());
        Set<String> nodeIdsWithAbsentName = votingConfigExclusions.stream()
                                                .filter(e -> e.getNodeName().equals(VotingConfigExclusion.MISSING_VALUE_MARKER))
                                                .map(VotingConfigExclusion::getNodeId)
                                                .collect(Collectors.toSet());
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (node.isMasterNode() &&
                (nodeIdsWithAbsentName.contains(node.getId()) || nodeNamesWithAbsentId.contains(node.getName()))) {
                    return false;
            }
        }

        return true;
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

    // exposed for tests
    boolean missingJoinVoteFrom(DiscoveryNode node) {
        return node.isMasterNode() && coordinationState.get().containsJoinVoteFor(node) == false;
    }

    private void handleJoin(Join join) {
        synchronized (mutex) {
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            if (coordinationState.get().electionWon()) {
                // If we have already won the election then the actual join does not matter for election purposes, so swallow any exception
                final boolean isNewJoinFromMasterEligibleNode = handleJoinIgnoringExceptions(join);

                // If we haven't completely finished becoming master then there's already a publication scheduled which will, in turn,
                // schedule a reconfiguration if needed. It's benign to schedule a reconfiguration anyway, but it might fail if it wins the
                // race against the election-winning publication and log a big error message, which we can prevent by checking this here:
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                if (isNewJoinFromMasterEligibleNode && establishedAsMaster && publicationInProgress() == false) {
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
            assert clusterState.nodes().getLocalNode() != null;
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
            assert clusterState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID) == false :
                "NO_MASTER_BLOCK should only be added by Coordinator";
            final ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks()).addGlobalBlock(
                noMasterBlockService.getNoMasterBlock()).build();
            final DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
            return ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(discoveryNodes).build();
        } else {
            return clusterState;
        }
    }

    @Override
    public void publish(
        ClusterStatePublicationEvent clusterStatePublicationEvent,
        ActionListener<Void> publishListener,
        AckListener ackListener
    ) {
        try {
            synchronized (mutex) {
                if (mode != Mode.LEADER || getCurrentTerm() != clusterStatePublicationEvent.getNewState().term()) {
                    logger.debug(() -> new ParameterizedMessage("[{}] failed publication as node is no longer master for term {}",
                        clusterStatePublicationEvent.getSummary(), clusterStatePublicationEvent.getNewState().term()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("node is no longer master for term " +
                        clusterStatePublicationEvent.getNewState().term() + " while handling publication"));
                    return;
                }

                if (currentPublication.isPresent()) {
                    assert false : "[" + currentPublication.get() + "] in progress, cannot start new publication";
                    logger.warn(() -> new ParameterizedMessage("[{}] failed publication as already publication in progress",
                        clusterStatePublicationEvent.getSummary()));
                    publishListener.onFailure(new FailedToCommitClusterStateException("publication " + currentPublication.get() +
                        " already in progress"));
                    return;
                }

                assert assertPreviousStateConsistency(clusterStatePublicationEvent);

                final ClusterState clusterState = clusterStatePublicationEvent.getNewState();

                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId())) :
                    getLocalNode() + " should be in published " + clusterState;

                final PublicationTransportHandler.PublicationContext publicationContext =
                    publicationHandler.newPublicationContext(clusterStatePublicationEvent);

                final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);
                final CoordinatorPublication publication = new CoordinatorPublication(publishRequest, publicationContext,
                    new ListenableFuture<>(), ackListener, publishListener);
                currentPublication = Optional.of(publication);

                final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                leaderChecker.setCurrentNodes(publishNodes);
                followersChecker.setCurrentNodes(publishNodes);
                lagDetector.setTrackedNodes(publishNodes);
                publication.start(followersChecker.getFaultyNodes());
            }
        } catch (Exception e) {
            logger.debug(() -> new ParameterizedMessage("[{}] publishing failed", clusterStatePublicationEvent.getSummary()), e);
            publishListener.onFailure(new FailedToCommitClusterStateException("publishing failed", e));
        }
    }

    // there is no equals on cluster state, so we just serialize it to XContent and compare Maps
    // deserialized from the resulting JSON
    private boolean assertPreviousStateConsistency(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        assert clusterStatePublicationEvent.getOldState() == coordinationState.get().getLastAcceptedState() ||
            XContentHelper.convertToMap(
                JsonXContent.jsonXContent, Strings.toString(clusterStatePublicationEvent.getOldState()), false
            ).equals(
                XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())),
                    false))
            : Strings.toString(clusterStatePublicationEvent.getOldState()) + " vs "
            + Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState()));
        return true;
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

    private void cancelActivePublication(String reason) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (currentPublication.isPresent()) {
            currentPublication.get().cancel(reason);
        }
    }

    public Collection<BiConsumer<DiscoveryNode, ClusterState>> getOnJoinValidators() {
        return onJoinValidators;
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }

    private class CoordinatorPeerFinder extends PeerFinder {

        CoordinatorPeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                              ConfiguredHostsResolver configuredHostsResolver) {
            super(settings, transportService, transportAddressConnector,
                singleNodeDiscovery ? hostsResolver -> Collections.emptyList() : configuredHostsResolver);
        }

        @Override
        protected void onActiveMasterFound(DiscoveryNode masterNode, long term) {
            synchronized (mutex) {
                ensureTermAtLeast(masterNode, term);
                joinHelper.sendJoinRequest(masterNode, getCurrentTerm(), joinWithDestination(lastJoin, masterNode, term));
            }
        }

        @Override
        protected void startProbe(TransportAddress transportAddress) {
            if (singleNodeDiscovery == false) {
                super.startProbe(transportAddress);
            }
        }

        @Override
        protected void onFoundPeersUpdated() {
            synchronized (mutex) {
                final Iterable<DiscoveryNode> foundPeers = getFoundPeers();
                if (mode == Mode.CANDIDATE) {
                    final VoteCollection expectedVotes = new VoteCollection();
                    foundPeers.forEach(expectedVotes::addVote);
                    expectedVotes.addVote(Coordinator.this.getLocalNode());
                    final boolean foundQuorum = coordinationState.get().isElectionQuorum(expectedVotes);

                    if (foundQuorum) {
                        if (electionScheduler == null) {
                            startElectionScheduler();
                        }
                    } else {
                        closePrevotingAndElectionScheduler();
                    }
                }
            }

            clusterBootstrapService.onFoundPeersUpdated();
        }
    }

    private void startElectionScheduler() {
        assert electionScheduler == null : electionScheduler;

        if (getLocalNode().isMasterNode() == false) {
            return;
        }

        final TimeValue gracePeriod = TimeValue.ZERO;
        electionScheduler = electionSchedulerFactory.startElectionScheduler(gracePeriod, new Runnable() {
            @Override
            public void run() {
                synchronized (mutex) {
                    if (mode == Mode.CANDIDATE) {
                        final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();

                        if (localNodeMayWinElection(lastAcceptedState) == false) {
                            logger.trace("skip prevoting as local node may not win election: {}",
                                lastAcceptedState.coordinationMetadata());
                            return;
                        }

                        final StatusInfo statusInfo = nodeHealthService.getHealth();
                        if (statusInfo.getStatus() == UNHEALTHY) {
                            logger.debug("skip prevoting as local node is unhealthy: [{}]", statusInfo.getInfo());
                            return;
                        }

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

    public Iterable<DiscoveryNode> getFoundPeers() {
        return peerFinder.getFoundPeers();
    }

    /**
     * If there is any current committed publication, this method cancels it.
     * This method is used exclusively by tests.
     * @return true if publication was cancelled, false if there is no current committed publication.
     */
    boolean cancelCommittedPublication() {
        synchronized (mutex) {
            if (currentPublication.isPresent()) {
                final CoordinatorPublication publication = currentPublication.get();
                if (publication.isCommitted()) {
                    publication.cancel("cancelCommittedPublication");
                    logger.debug("Cancelled publication of [{}].", publication);
                    return true;
                }
            }
            return false;
        }
    }

    class CoordinatorPublication extends Publication {

        private final PublishRequest publishRequest;
        private final ListenableFuture<Void> localNodeAckEvent;
        private final AckListener ackListener;
        private final ActionListener<Void> publishListener;
        private final PublicationTransportHandler.PublicationContext publicationContext;

        @Nullable // if using single-node discovery
        private final Scheduler.ScheduledCancellable timeoutHandler;
        private final Scheduler.Cancellable infoTimeoutHandler;

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

            this.timeoutHandler = singleNodeDiscovery ? null : transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        cancel("timed out after " + publishTimeout);
                    }
                }

                @Override
                public String toString() {
                    return "scheduled timeout for " + CoordinatorPublication.this;
                }
            }, publishTimeout, Names.GENERIC);

            this.infoTimeoutHandler = transportService.getThreadPool().schedule(new Runnable() {
                @Override
                public void run() {
                    synchronized (mutex) {
                        logIncompleteNodes(Level.INFO);
                    }
                }

                @Override
                public String toString() {
                    return "scheduled timeout for reporting on " + CoordinatorPublication.this;
                }
            }, publishInfoTimeout, Names.GENERIC);
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
                            public void onFailure(Exception e) {
                                synchronized (mutex) {
                                    removePublicationAndPossiblyBecomeCandidate("clusterApplier#onNewClusterState");
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), e);
                                publishListener.onFailure(e);
                            }

                            @Override
                            public void onSuccess() {
                                synchronized (mutex) {
                                    assert currentPublication.get() == CoordinatorPublication.this;
                                    currentPublication = Optional.empty();
                                    logger.debug("publication ended successfully: {}", CoordinatorPublication.this);
                                    // trigger term bump if new term was found during publication
                                    updateMaxTermSeen(getCurrentTerm());

                                    if (mode == Mode.LEADER) {
                                        // if necessary, abdicate to another node or improve the voting configuration
                                        boolean attemptReconfiguration = true;
                                        final ClusterState state = getLastAcceptedState(); // committed state
                                        if (localNodeMayWinElection(state) == false) {
                                            final List<DiscoveryNode> masterCandidates = completedNodes().stream()
                                                .filter(DiscoveryNode::isMasterNode)
                                                .filter(node -> nodeMayWinElection(state, node))
                                                .filter(node -> {
                                                    // check if master candidate would be able to get an election quorum if we were to
                                                    // abdicate to it. Assume that every node that completed the publication can provide
                                                    // a vote in that next election and has the latest state.
                                                    final long futureElectionTerm = state.term() + 1;
                                                    final VoteCollection futureVoteCollection = new VoteCollection();
                                                    completedNodes().forEach(completedNode -> futureVoteCollection.addJoinVote(
                                                        new Join(completedNode, node, futureElectionTerm, state.term(), state.version())));
                                                    return electionStrategy.isElectionQuorum(node, futureElectionTerm,
                                                        state.term(), state.version(), state.getLastCommittedConfiguration(),
                                                        state.getLastAcceptedConfiguration(), futureVoteCollection);
                                                })
                                                .collect(Collectors.toList());
                                            if (masterCandidates.isEmpty() == false) {
                                                abdicateTo(masterCandidates.get(random.nextInt(masterCandidates.size())));
                                                attemptReconfiguration = false;
                                            }
                                        }
                                        if (attemptReconfiguration) {
                                            scheduleReconfigurationIfNeeded();
                                        }
                                    }
                                    lagDetector.startLagDetector(publishRequest.getAcceptedState().version());
                                    logIncompleteNodes(Level.WARN);
                                }
                                cancelTimeoutHandlers();
                                ackListener.onNodeAck(getLocalNode(), null);
                                publishListener.onResponse(null);
                            }
                        });
                }

                @Override
                public void onFailure(Exception e) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");
                    cancelTimeoutHandlers();

                    final FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException("publication failed", e);
                    ackListener.onNodeAck(getLocalNode(), exception); // other nodes have acked, but not the master.
                    publishListener.onFailure(exception);
                }
            }, EsExecutors.DIRECT_EXECUTOR_SERVICE, transportService.getThreadPool().getThreadContext());
        }

        private void cancelTimeoutHandlers() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
            infoTimeoutHandler.cancel();
        }

        private void handleAssociatedJoin(Join join) {
            if (join.getTerm() == getCurrentTerm() && missingJoinVoteFrom(join.getSourceNode())) {
                logger.trace("handling {}", join);
                handleJoin(join);
            }
        }

        @Override
        protected boolean isPublishQuorum(VoteCollection votes) {
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
            if (missingJoinVoteFrom(discoveryNode)) {
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
}
