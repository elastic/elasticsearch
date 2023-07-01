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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalMasterServiceTask;
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
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.ConfiguredHostsResolver;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.HandshakingTransportAddressConnector;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.discovery.TransportAddressConnector;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class Coordinator extends AbstractLifecycleComponent implements ClusterStatePublisher {

    private static final Logger logger = LogManager.getLogger(Coordinator.class);

    // the timeout before emitting an info log about a slow-running publication
    public static final Setting<TimeValue> PUBLISH_INFO_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.publish.info_timeout",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    // the timeout for the publication of each value
    public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.publish.timeout",
        TimeValue.timeValueMillis(30000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SINGLE_NODE_CLUSTER_SEED_HOSTS_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.discovery_configuration_check.interval",
        TimeValue.timeValueMillis(30000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final Settings settings;
    private final boolean singleNodeDiscovery;
    private final ElectionStrategy electionStrategy;
    private final TransportService transportService;
    private final MasterService masterService;
    private final AllocationService allocationService;
    private final JoinHelper joinHelper;
    private final JoinValidationService joinValidationService;
    private final MasterServiceTaskQueue<NodeLeftExecutor.Task> nodeLeftQueue;
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
    private final TimeValue singleNodeClusterSeedHostsCheckInterval;
    @Nullable
    private Scheduler.Cancellable singleNodeClusterChecker = null;
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
    private final JoinReasonService joinReasonService;

    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private JoinHelper.JoinAccumulator joinAccumulator;
    private Optional<CoordinatorPublication> currentPublication = Optional.empty();
    private final NodeHealthService nodeHealthService;
    private final List<PeerFinderListener> peerFinderListeners;
    private final LeaderHeartbeatService leaderHeartbeatService;

    /**
     * @param nodeName The name of the node, used to name the {@link java.util.concurrent.ExecutorService} of the {@link SeedHostsResolver}.
     * @param onJoinValidators A collection of join validators to restrict which nodes may join the cluster.
     */
    public Coordinator(
        String nodeName,
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        Client client,
        NamedWriteableRegistry namedWriteableRegistry,
        AllocationService allocationService,
        MasterService masterService,
        Supplier<CoordinationState.PersistedState> persistedStateSupplier,
        SeedHostsProvider seedHostsProvider,
        ClusterApplier clusterApplier,
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators,
        Random random,
        RerouteService rerouteService,
        ElectionStrategy electionStrategy,
        NodeHealthService nodeHealthService,
        CircuitBreakerService circuitBreakerService,
        Reconfigurator reconfigurator,
        LeaderHeartbeatService leaderHeartbeatService,
        PreVoteCollector.Factory preVoteCollectorFactory
    ) {
        this.settings = settings;
        this.transportService = transportService;
        this.masterService = masterService;
        this.allocationService = allocationService;
        this.onJoinValidators = NodeJoinExecutor.addBuiltInJoinValidators(onJoinValidators);
        this.singleNodeDiscovery = DiscoveryModule.isSingleNodeDiscovery(settings);
        this.electionStrategy = electionStrategy;
        this.joinReasonService = new JoinReasonService(transportService.getThreadPool()::relativeTimeInMillis);
        this.joinHelper = new JoinHelper(
            allocationService,
            masterService,
            clusterApplier,
            transportService,
            this::getCurrentTerm,
            this::handleJoinRequest,
            this::joinLeaderInTerm,
            rerouteService,
            nodeHealthService,
            joinReasonService,
            circuitBreakerService,
            reconfigurator::maybeReconfigureAfterNewMasterIsElected,
            this::getLatestStoredStateAfterWinningAnElection
        );
        this.joinValidationService = new JoinValidationService(
            settings,
            transportService,
            this::getApplierState, // best-effort validation can use any recent state
            this.onJoinValidators
        );
        this.persistedStateSupplier = persistedStateSupplier;
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
        this.joinAccumulator = new InitialJoinAccumulator();
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        this.publishInfoTimeout = PUBLISH_INFO_TIMEOUT_SETTING.get(settings);
        this.singleNodeClusterSeedHostsCheckInterval = SINGLE_NODE_CLUSTER_SEED_HOSTS_CHECK_INTERVAL_SETTING.get(settings);
        this.random = random;
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        this.preVoteCollector = preVoteCollectorFactory.create(
            transportService,
            this::startElection,
            this::updateMaxTermSeen,
            electionStrategy,
            nodeHealthService,
            leaderHeartbeatService
        );
        configuredHostsResolver = new SeedHostsResolver(nodeName, settings, transportService, seedHostsProvider);
        this.peerFinder = new CoordinatorPeerFinder(
            settings,
            transportService,
            new HandshakingTransportAddressConnector(settings, transportService),
            configuredHostsResolver
        );
        transportService.registerRequestHandler(
            COMMIT_STATE_ACTION_NAME,
            Names.CLUSTER_COORDINATION,
            false,
            false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit(request, new ChannelActionListener<>(channel).map(r -> Empty.INSTANCE))
        );
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry, this::handlePublishRequest);
        this.leaderChecker = new LeaderChecker(settings, transportService, this::onLeaderFailure, nodeHealthService);
        this.followersChecker = new FollowersChecker(
            settings,
            transportService,
            this::onFollowerCheckRequest,
            this::removeNode,
            nodeHealthService
        );
        this.nodeLeftQueue = masterService.createTaskQueue("node-left", Priority.IMMEDIATE, new NodeLeftExecutor(allocationService));
        this.clusterApplier = clusterApplier;
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        this.reconfigurator = reconfigurator;
        this.clusterBootstrapService = new ClusterBootstrapService(
            settings,
            transportService,
            this::getFoundPeers,
            this::isInitialConfigurationSet,
            this::setInitialConfiguration
        );
        this.lagDetector = new LagDetector(
            settings,
            transportService.getThreadPool(),
            new LagDetector.HotThreadsLoggingLagListener(
                transportService,
                client,
                (node, appliedVersion, expectedVersion) -> removeNode(node, "lagging")
            ),
            transportService::getLocalNode
        );
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(
            settings,
            this::getClusterFormationState,
            transportService.getThreadPool(),
            joinHelper::logLastFailedJoinAttempt
        );
        this.nodeHealthService = nodeHealthService;
        this.peerFinderListeners = new CopyOnWriteArrayList<>();
        this.peerFinderListeners.add(clusterBootstrapService);
        this.leaderHeartbeatService = leaderHeartbeatService;
    }

    /**
     * This method returns an object containing information about why cluster formation failed, which can be useful in troubleshooting.
     * @return Information about why cluster formation failed
     */
    public ClusterFormationState getClusterFormationState() {
        return new ClusterFormationState(
            settings,
            getLastAcceptedState(), // doesn't care about blocks or the current master node so no need for getStateForMasterService
            peerFinder.getLastResolvedAddresses(),
            Stream.concat(Stream.of(getLocalNode()), StreamSupport.stream(peerFinder.getFoundPeers().spliterator(), false)).toList(),
            getCurrentTerm(),
            electionStrategy,
            nodeHealthService.getHealth(),
            joinHelper.getInFlightJoinStatuses()
        );
    }

    private void onLeaderFailure(Supplier<String> message, Exception e) {
        synchronized (mutex) {
            if (mode != Mode.CANDIDATE) {
                assert lastKnownLeader.isPresent();
                if (logger.isDebugEnabled()) {
                    // TODO this is a workaround for log4j's Supplier. We should remove this, once using ES logging api
                    logger.info(() -> message.get(), e);
                } else {
                    logger.info(() -> message.get());
                }
            }
            becomeCandidate("onLeaderFailure");
        }
    }

    private void removeNode(DiscoveryNode discoveryNode, String reason) {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                var task = new NodeLeftExecutor.Task(discoveryNode, reason, () -> joinReasonService.onNodeRemoved(discoveryNode, reason));
                nodeLeftQueue.submitTask("node-left", task, null);
            }
        }
    }

    void onFollowerCheckRequest(FollowerCheckRequest followerCheckRequest) {
        synchronized (mutex) {
            ensureTermAtLeast(followerCheckRequest.getSender(), followerCheckRequest.getTerm());

            if (getCurrentTerm() != followerCheckRequest.getTerm()) {
                logger.trace("onFollowerCheckRequest: current term is [{}], rejecting {}", getCurrentTerm(), followerCheckRequest);
                throw new CoordinationStateRejectedException(
                    "onFollowerCheckRequest: current term is [" + getCurrentTerm() + "], rejecting " + followerCheckRequest
                );
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
                    "onFollowerCheckRequest: received check from faulty master, rejecting " + followerCheckRequest
                );
            }
        }
    }

    private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
        synchronized (mutex) {
            logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

            coordinationState.get().handleCommit(applyCommitRequest);
            final ClusterState committedState = hideStateIfNotRecovered(coordinationState.get().getLastAcceptedState());
            applierState = mode == Mode.CANDIDATE ? clusterStateWithNoMasterBlock(committedState) : committedState;
            updateSingleNodeClusterChecker(); // in case nodes increase/decrease, possibly update the single-node checker
            if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
                // master node applies the committed state at the end of the publication process, not here.
                applyListener.onResponse(null);
            } else {
                clusterApplier.onNewClusterState(applyCommitRequest.toString(), () -> applierState, applyListener.map(r -> {
                    onClusterStateApplied();
                    return r;
                }));
            }
        }
    }

    private void onClusterStateApplied() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        if (getMode() != Mode.CANDIDATE) {
            joinHelper.onClusterStateApplied();
        }
        if (getLocalNode().isMasterNode()) {
            joinReasonService.onClusterStateApplied(applierState.nodes());
        }
    }

    PublishWithJoinResponse handlePublishRequest(PublishRequest publishRequest) {
        assert publishRequest.getAcceptedState().nodes().getLocalNode().equals(getLocalNode())
            : publishRequest.getAcceptedState().nodes().getLocalNode() + " != " + getLocalNode();

        final ClusterState newClusterState = publishRequest.getAcceptedState();
        if (newClusterState.nodes().isLocalNodeElectedMaster() == false) {
            // background initialization on the current master has been started by the master service already
            newClusterState.initializeAsync(transportService.getThreadPool().generic());
        }

        synchronized (mutex) {
            final DiscoveryNode sourceNode = newClusterState.nodes().getMasterNode();
            logger.trace("handlePublishRequest: handling [{}] from [{}]", publishRequest, sourceNode);

            if (sourceNode.equals(getLocalNode()) && mode != Mode.LEADER) {
                // Rare case in which we stood down as leader between starting this publication and receiving it ourselves. The publication
                // is already failed so there is no point in proceeding.
                throw new CoordinationStateRejectedException("no longer leading this publication's term: " + publishRequest);
            }

            final ClusterState localState = coordinationState.get().getLastAcceptedState();

            if (localState.metadata().clusterUUIDCommitted()
                && localState.metadata().clusterUUID().equals(newClusterState.metadata().clusterUUID()) == false) {
                logger.warn(
                    "received cluster state from {} with a different cluster uuid {} than local cluster uuid {}, rejecting",
                    sourceNode,
                    newClusterState.metadata().clusterUUID(),
                    localState.metadata().clusterUUID()
                );
                throw new CoordinationStateRejectedException(
                    "received cluster state from "
                        + sourceNode
                        + " with a different cluster uuid "
                        + newClusterState.metadata().clusterUUID()
                        + " than local cluster uuid "
                        + localState.metadata().clusterUUID()
                        + ", rejecting"
                );
            }

            if (newClusterState.term() > localState.term()) {
                // only do join validation if we have not accepted state from this master yet
                onJoinValidators.forEach(a -> a.accept(getLocalNode(), newClusterState));
            }

            ensureTermAtLeast(sourceNode, newClusterState.term());
            final PublishResponse publishResponse = coordinationState.get().handlePublishRequest(publishRequest);

            if (sourceNode.equals(getLocalNode())) {
                preVoteCollector.update(getPreVoteResponse(), getLocalNode());
            } else {
                becomeFollower("handlePublishRequest", sourceNode); // also updates preVoteCollector
            }

            return new PublishWithJoinResponse(publishResponse, joinWithDestination(lastJoin, sourceNode, newClusterState.term()));
        }
    }

    private static Optional<Join> joinWithDestination(Optional<Join> lastJoin, DiscoveryNode leader, long term) {
        if (lastJoin.isPresent() && lastJoin.get().targetMatches(leader) && lastJoin.get().getTerm() == term) {
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
                        logger.warn(() -> format("failed to bump term to %s", maxTermSeen), e);
                        becomeCandidate("updateMaxTermSeen");
                    }
                }
            }
        }
    }

    private long getTermForNewElection() {
        assert Thread.holdsLock(mutex);
        return Math.max(getCurrentTerm(), maxTermSeen) + 1;
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

                final var electionTerm = getTermForNewElection();
                logger.debug("starting election for {} in term {}", getLocalNode(), electionTerm);
                broadcastStartJoinRequest(getLocalNode(), electionTerm, getDiscoveredNodes());
            }
        }
    }

    private void broadcastStartJoinRequest(DiscoveryNode candidateMasterNode, long term, List<DiscoveryNode> discoveredNodes) {
        electionStrategy.onNewElection(candidateMasterNode, term, new ActionListener<>() {
            @Override
            public void onResponse(StartJoinRequest startJoinRequest) {
                discoveredNodes.forEach(node -> joinHelper.sendStartJoinRequest(startJoinRequest, node));
            }

            @Override
            public void onFailure(Exception e) {
                logger.log(
                    e instanceof CoordinationStateRejectedException ? Level.DEBUG : Level.WARN,
                    Strings.format("election attempt for [%s] in term [%d] failed", candidateMasterNode, term),
                    e
                );
            }
        });
    }

    private void abdicateTo(DiscoveryNode newMaster) {
        assert Thread.holdsLock(mutex);
        assert mode == Mode.LEADER : "expected to be leader on abdication but was " + mode;
        assert newMaster.isMasterNode() : "should only abdicate to master-eligible node but was " + newMaster;
        final var electionTerm = getTermForNewElection();
        logger.info("abdicating to {} with term {}", newMaster, electionTerm);
        broadcastStartJoinRequest(newMaster, electionTerm, getLastAcceptedState().nodes().mastersFirstStream().toList());
        // handling of start join messages on the local node will be dispatched to the coordination thread-pool
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

    private void handleJoinRequest(JoinRequest joinRequest, ActionListener<Void> joinListener) {
        assert Thread.holdsLock(mutex) == false;
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);

        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinListener.onFailure(
                new IllegalStateException(
                    "cannot join node with ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "] discovery"
                )
            );
            return;
        }

        transportService.connectToNode(joinRequest.getSourceNode(), new ActionListener<>() {
            @Override
            public void onResponse(Releasable response) {
                boolean retainConnection = false;
                try {
                    validateJoinRequest(
                        joinRequest,
                        ActionListener.runBefore(joinListener, () -> Releasables.close(response))
                            .delegateFailure((l, ignored) -> processJoinRequest(joinRequest, l))
                    );
                    retainConnection = true;
                } catch (Exception e) {
                    joinListener.onFailure(e);
                } finally {
                    if (retainConnection == false) {
                        Releasables.close(response);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> format(
                        "received join request from [%s] but could not connect back to the joining node",
                        joinRequest.getSourceNode()
                    ),
                    e
                );

                joinListener.onFailure(
                    // NodeDisconnectedException mainly to suppress uninteresting stack trace
                    new NodeDisconnectedException(
                        joinRequest.getSourceNode(),
                        String.format(
                            Locale.ROOT,
                            "failure when opening connection back from [%s] to [%s]",
                            getLocalNode().descriptionWithoutAttributes(),
                            joinRequest.getSourceNode().descriptionWithoutAttributes()
                        ),
                        JoinHelper.JOIN_ACTION_NAME,
                        e
                    )
                );
            }
        });
    }

    private void validateJoinRequest(JoinRequest joinRequest, ActionListener<Void> validateListener) {

        // Before letting the node join the cluster, ensure:
        // - it's a new enough version to pass the version barrier
        // - we have a healthy STATE channel to the node
        // - if we're already master that it can make sense of the current cluster state.
        // - we have a healthy PING channel to the node

        final ClusterState stateForJoinValidation;
        synchronized (mutex) {
            // similar to getStateForMasterService(), but don't rebuild the state if we're not the master since we don't use it in that case
            final ClusterState lastAcceptedState = coordinationState.get().getLastAcceptedState();
            assert lastAcceptedState.nodes().getLocalNode() != null;
            if (mode != Mode.LEADER || lastAcceptedState.term() != getCurrentTerm()) {
                stateForJoinValidation = null;
            } else {
                stateForJoinValidation = lastAcceptedState;
            }
        }

        final ListenableActionFuture<Empty> validateStateListener = new ListenableActionFuture<>();
        if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
            onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
            if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                // We do this in a couple of places including the cluster update thread. This one here is really just best effort to ensure
                // we fail as fast as possible.
                NodeJoinExecutor.ensureVersionBarrier(
                    joinRequest.getSourceNode().getVersion(),
                    stateForJoinValidation.getNodes().getMinNodeVersion()
                );
            }
            sendJoinValidate(joinRequest.getSourceNode(), validateStateListener);
        } else {
            sendJoinPing(joinRequest.getSourceNode(), TransportRequestOptions.Type.STATE, validateStateListener);
        }

        sendJoinPing(joinRequest.getSourceNode(), TransportRequestOptions.Type.PING, new ActionListener<>() {
            @Override
            public void onResponse(Empty empty) {
                validateStateListener.addListener(validateListener.map(ignored -> null));
            }

            @Override
            public void onFailure(Exception e) {
                // The join will be rejected, but we wait for the state validation to complete as well since the node will retry and we
                // don't want lots of cluster states in flight.
                validateStateListener.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Empty empty) {
                        validateListener.onFailure(e);
                    }

                    @Override
                    public void onFailure(Exception e2) {
                        e2.addSuppressed(e);
                        validateListener.onFailure(e2);
                    }
                });
            }
        });
    }

    private void sendJoinValidate(DiscoveryNode discoveryNode, ActionListener<Empty> listener) {
        joinValidationService.validateJoin(discoveryNode, listener.delegateResponse((delegate, e) -> {
            logger.warn(() -> "failed to validate incoming join request from node [" + discoveryNode + "]", e);
            delegate.onFailure(
                new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "failure when sending a join validation request from [%s] to [%s]",
                        getLocalNode().descriptionWithoutAttributes(),
                        discoveryNode.descriptionWithoutAttributes()
                    ),
                    e
                )
            );
        }));
    }

    private void sendJoinPing(DiscoveryNode discoveryNode, TransportRequestOptions.Type channelType, ActionListener<Empty> listener) {
        transportService.sendRequest(
            discoveryNode,
            JoinHelper.JOIN_PING_ACTION_NAME,
            TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.of(null, channelType),
            new ActionListenerResponseHandler<>(listener.delegateResponse((l, e) -> {
                logger.warn(() -> format("failed to ping joining node [%s] on channel type [%s]", discoveryNode, channelType), e);
                listener.onFailure(
                    new IllegalStateException(
                        String.format(
                            Locale.ROOT,
                            "failure when sending a join ping request from [%s] to [%s]",
                            getLocalNode().descriptionWithoutAttributes(),
                            discoveryNode.descriptionWithoutAttributes()
                        ),
                        e
                    )
                );
            }), i -> Empty.INSTANCE, Names.CLUSTER_COORDINATION)
        );
    }

    private void processJoinRequest(JoinRequest joinRequest, ActionListener<Void> joinListener) {
        assert Transports.assertNotTransportThread("blocking on coordinator mutex and maybe doing IO to increase term");
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();
        try {
            synchronized (mutex) {
                updateMaxTermSeen(joinRequest.getTerm());

                final CoordinationState coordState = coordinationState.get();
                final boolean prevElectionWon = coordState.electionWon();

                optionalJoin.ifPresent(this::handleJoin);
                joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinRequest.getTransportVersion(), joinListener);

                if (prevElectionWon == false && coordState.electionWon()) {
                    becomeLeader();
                }
            }
        } catch (Exception e) {
            joinListener.onFailure(e);
        }
    }

    private void updateSingleNodeClusterChecker() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

        if (mode == Mode.LEADER && applierState.nodes().size() == 1) {
            if (singleNodeClusterChecker == null) {
                // Make a single-node checker if none exists
                singleNodeClusterChecker = transportService.getThreadPool().scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        Coordinator.this.checkSingleNodeCluster();
                    }

                    @Override
                    public String toString() {
                        return "single-node cluster checker";
                    }
                }, this.singleNodeClusterSeedHostsCheckInterval, Names.SAME);
            }
            return;
        }

        // In case of a multi-node cluster, there is no need for the single-node checker so cancel it
        if (singleNodeClusterChecker != null) {
            singleNodeClusterChecker.cancel();
            singleNodeClusterChecker = null;
        }
    }

    private void checkSingleNodeCluster() {
        if (mode != Mode.LEADER || applierState.nodes().size() > 1) {
            return;
        }

        if (DISCOVERY_SEED_HOSTS_SETTING.exists(settings)) {
            if (DISCOVERY_SEED_HOSTS_SETTING.get(settings).isEmpty()) {
                // For a single-node cluster, the only acceptable setting is an empty list.
                return;
            } else {
                logger.warn(
                    """
                        This node is a fully-formed single-node cluster with cluster UUID [{}], but it is configured as if to \
                        discover other nodes and form a multi-node cluster via the [{}] setting. Fully-formed clusters do not \
                        attempt to discover other nodes, and nodes with different cluster UUIDs cannot belong to the same cluster. \
                        The cluster UUID persists across restarts and can only be changed by deleting the contents of the node's \
                        data path(s). Remove the discovery configuration to suppress this message.""",
                    applierState.metadata().clusterUUID(),
                    DISCOVERY_SEED_HOSTS_SETTING.getKey() + "=" + DISCOVERY_SEED_HOSTS_SETTING.get(settings)
                );
            }
        }
    }

    void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.debug(
            "{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
            method,
            getCurrentTerm(),
            mode,
            lastKnownLeader
        );

        if (mode != Mode.CANDIDATE) {
            final Mode prevMode = mode;
            mode = Mode.CANDIDATE;
            cancelActivePublication("become candidate: " + method);
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());
            clusterFormationFailureHelper.start();

            leaderHeartbeatService.stop();
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
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, ActionListener.noop());
            }
        }

        updateSingleNodeClusterChecker();
        preVoteCollector.update(getPreVoteResponse(), null);
    }

    private void becomeLeader() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        final var leaderTerm = getCurrentTerm();
        logger.debug(
            "handleJoinRequest: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
            leaderTerm,
            mode,
            lastKnownLeader
        );

        mode = Mode.LEADER;
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        lastKnownLeader = Optional.of(getLocalNode());
        peerFinder.deactivate(getLocalNode());
        clusterFormationFailureHelper.stop();
        closePrevotingAndElectionScheduler();
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());
        leaderHeartbeatService.start(
            getLocalNode(),
            leaderTerm,
            new ThreadedActionListener<>(transportService.getThreadPool().executor(Names.CLUSTER_COORDINATION), new ActionListener<>() {
                @Override
                public void onResponse(Long newTerm) {
                    assert newTerm != null && newTerm > leaderTerm : newTerm + " vs " + leaderTerm;
                    updateMaxTermSeen(newTerm);
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO tests for heartbeat failures
                    logger.warn(() -> Strings.format("failed to write heartbeat for term [%s]", leaderTerm), e);
                    synchronized (mutex) {
                        if (getCurrentTerm() == leaderTerm) {
                            becomeCandidate("leaderHeartbeatService");
                        }
                    }
                }

                @Override
                public String toString() {
                    return "term change heartbeat listener";
                }
            })
        );

        assert leaderChecker.leader() == null : leaderChecker.leader();
        followersChecker.updateFastResponseState(leaderTerm, mode);

        updateSingleNodeClusterChecker();
    }

    void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert leaderNode.isMasterNode() : leaderNode + " became a leader but is not master-eligible";
        assert mode != Mode.LEADER : "do not switch to follower from leader (should be candidate first)";

        if (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) {
            logger.trace("{}: coordinator remaining FOLLOWER of [{}] in term {}", method, leaderNode, getCurrentTerm());
        } else {
            logger.debug(
                "{}: coordinator becoming FOLLOWER of [{}] in term {} (was {}, lastKnownLeader was [{}])",
                method,
                leaderNode,
                getCurrentTerm(),
                mode,
                lastKnownLeader
            );
        }

        final boolean restartLeaderChecker = (mode == Mode.FOLLOWER && Optional.of(leaderNode).equals(lastKnownLeader)) == false;

        if (mode != Mode.FOLLOWER) {
            mode = Mode.FOLLOWER;
            joinAccumulator.close(mode);
            joinAccumulator = new JoinHelper.FollowerJoinAccumulator();
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderHeartbeatService.stop();
        }

        updateSingleNodeClusterChecker();
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
        new LocalMasterServiceTask(Priority.NORMAL) {
            @Override
            public void onFailure(Exception e) {
                // ignore
                logger.trace("failed to clean-up after stepping down as master", e);
            }

            @Override
            public void execute(ClusterState currentState) {
                if (currentState.nodes().isLocalNodeElectedMaster() == false) {
                    allocationService.cleanCaches();
                }
            }
        }.submit(masterService, "clean-up after stepping down as master");
    }

    private PreVoteResponse getPreVoteResponse() {
        return new PreVoteResponse(
            getCurrentTerm(),
            coordinationState.get().getLastAcceptedTerm(),
            coordinationState.get().getLastAcceptedState().version()
        );
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
            clusterBootstrapService.logBootstrapState(lastAcceptedState.metadata());
            final VotingConfiguration votingConfiguration = lastAcceptedState.getLastCommittedConfiguration();
            if (singleNodeDiscovery
                && votingConfiguration.isEmpty() == false
                && votingConfiguration.hasQuorum(Collections.singleton(getLocalNode().getId())) == false) {
                throw new IllegalStateException(
                    "cannot start with ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "] when local node "
                        + getLocalNode()
                        + " does not have quorum in voting configuration "
                        + votingConfiguration
                );
            }
            final Metadata.Builder metadata = Metadata.builder();
            if (lastAcceptedState.metadata().clusterUUIDCommitted()) {
                metadata.clusterUUID(lastAcceptedState.metadata().clusterUUID()).clusterUUIDCommitted(true);
            }
            ClusterState initialState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                .blocks(
                    ClusterBlocks.builder()
                        .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                        .addGlobalBlock(noMasterBlockService.getNoMasterBlock())
                )
                .nodes(DiscoveryNodes.builder().add(getLocalNode()).localNodeId(getLocalNode().getId()))
                .putTransportVersion(getLocalNode().getId(), TransportVersion.current())
                .metadata(metadata)
                .build();
            applierState = initialState;
            clusterApplier.setInitialState(initialState);
        }
    }

    public DiscoveryStats stats() {
        return new DiscoveryStats(
            new PendingClusterStateStats(0, 0, 0),
            publicationHandler.stats(),
            getLocalNode().isMasterNode() ? masterService.getClusterStateUpdateStats() : null,
            clusterApplier.getStats()
        );
    }

    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
    }

    @Override
    protected void doStop() {
        configuredHostsResolver.stop();
        joinValidationService.stop();
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
            final ClusterState lastAcceptedClusterState = getStateForMasterService();

            assert peerFinder.getCurrentTerm() == getCurrentTerm();
            assert followersChecker.getFastResponseState().term() == getCurrentTerm() : followersChecker.getFastResponseState();
            assert followersChecker.getFastResponseState().mode() == getMode() : followersChecker.getFastResponseState();
            assert (applierState.nodes().getMasterNodeId() == null) == applierState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID);
            assert preVoteCollector.getPreVoteResponse().equals(getPreVoteResponse()) : preVoteCollector + " vs " + getPreVoteResponse();

            assert lagDetector.getTrackedNodes().contains(getLocalNode()) == false : lagDetector.getTrackedNodes();
            assert followersChecker.getKnownFollowers().equals(lagDetector.getTrackedNodes())
                : followersChecker.getKnownFollowers() + " vs " + lagDetector.getTrackedNodes();
            assert singleNodeClusterChecker == null || (mode == Mode.LEADER && applierState.nodes().size() == 1)
                : "Single node checker must exist iff there is a single-node cluster";

            if (mode == Mode.LEADER) {
                final boolean becomingMaster = lastAcceptedClusterState.term() != getCurrentTerm();

                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
                assert joinAccumulator instanceof JoinHelper.LeaderJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert becomingMaster || lastAcceptedClusterState.nodes().getMasterNodeId() != null : lastAcceptedClusterState;
                assert leaderChecker.leader() == null : leaderChecker.leader();
                assert getLocalNode().equals(applierState.nodes().getMasterNode())
                    || (applierState.nodes().getMasterNodeId() == null && applierState.term() < getCurrentTerm());
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
                    assert lastPublishedNodes.equals(followersChecker.getKnownFollowers())
                        : lastPublishedNodes + " != " + followersChecker.getKnownFollowers();
                }

                assert becomingMaster
                    || activePublication
                    || coordinationState.get()
                        .getLastAcceptedConfiguration()
                        .equals(coordinationState.get().getLastCommittedConfiguration())
                    : coordinationState.get().getLastAcceptedConfiguration()
                        + " != "
                        + coordinationState.get().getLastCommittedConfiguration();
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
                assert joinAccumulator instanceof JoinHelper.FollowerJoinAccumulator;
                assert peerFinderLeader.equals(lastKnownLeader) : peerFinderLeader;
                assert electionScheduler == null : electionScheduler;
                assert prevotingRound == null : prevotingRound;
                assert lastAcceptedClusterState.nodes().getMasterNodeId() == null : lastAcceptedClusterState;
                assert leaderChecker.currentNodeIsMaster() == false;
                assert lastKnownLeader.equals(Optional.of(leaderChecker.leader()));
                assert followersChecker.getKnownFollowers().isEmpty();
                assert lastKnownLeader.get().equals(applierState.nodes().getMasterNode())
                    || (applierState.nodes().getMasterNodeId() == null
                        && (applierState.term() < getCurrentTerm() || applierState.version() < getLastAcceptedState().version()));
                assert currentPublication.map(Publication::isCommitted).orElse(true);
                assert preVoteCollector.getLeader().equals(lastKnownLeader.get()) : preVoteCollector;
                assert clusterFormationFailureHelper.isRunning() == false;
            } else {
                assert mode == Mode.CANDIDATE;
                assert joinAccumulator instanceof JoinHelper.CandidateJoinAccumulator;
                assert peerFinderLeader.isPresent() == false : peerFinderLeader;
                assert prevotingRound == null || electionScheduler != null;
                assert lastAcceptedClusterState.nodes().getMasterNodeId() == null : lastAcceptedClusterState;
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
        return getLastAcceptedState().getLastAcceptedConfiguration().isEmpty() == false;
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
                    "this node is not master-eligible, but cluster bootstrapping can only happen on a master-eligible node"
                );
            }

            if (votingConfiguration.getNodeIds().contains(getLocalNode().getId()) == false) {
                logger.debug("skip setting initial configuration as local node is not part of initial configuration");
                throw new CoordinationStateRejectedException("local node is not part of initial configuration");
            }

            final List<DiscoveryNode> knownNodes = new ArrayList<>();
            knownNodes.add(getLocalNode());
            peerFinder.getFoundPeers().forEach(knownNodes::add);

            if (votingConfiguration.hasQuorum(knownNodes.stream().map(DiscoveryNode::getId).toList()) == false) {
                logger.debug(
                    "skip setting initial configuration as not enough nodes discovered to form a quorum in the "
                        + "initial configuration [knownNodes={}, {}]",
                    knownNodes,
                    votingConfiguration
                );
                throw new CoordinationStateRejectedException(
                    "not enough nodes discovered to form a quorum in the initial configuration "
                        + "[knownNodes="
                        + knownNodes
                        + ", "
                        + votingConfiguration
                        + "]"
                );
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
            assert localNodeMayWinElection(getLastAcceptedState())
                : "initial state does not allow local node to win election: " + getLastAcceptedState().coordinationMetadata();
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
        final Stream<String> masterIneligibleNodeIdsInVotingConfig = clusterState.nodes()
            .stream()
            .filter(
                n -> n.isMasterNode() == false
                    && (clusterState.getLastAcceptedConfiguration().getNodeIds().contains(n.getId())
                        || clusterState.getLastCommittedConfiguration().getNodeIds().contains(n.getId()))
            )
            .map(DiscoveryNode::getId);

        DiscoveryNode localNode = getLocalNode();
        final Set<DiscoveryNode> liveNodes = clusterState.nodes()
            .stream()
            .filter(DiscoveryNode::isMasterNode)
            .filter((n) -> coordinationState.get().containsJoinVoteFor(n) || n.equals(localNode))
            .collect(Collectors.toSet());
        final VotingConfiguration newConfig = reconfigurator.reconfigure(
            liveNodes,
            Stream.concat(masterIneligibleNodeIdsInVotingConfig, excludedNodeIds).collect(Collectors.toSet()),
            localNode,
            clusterState.getLastAcceptedConfiguration()
        );

        if (newConfig.equals(clusterState.getLastAcceptedConfiguration()) == false) {
            assert coordinationState.get().joinVotesHaveQuorumFor(newConfig);
            return ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .coordinationMetadata(
                            CoordinationMetadata.builder(clusterState.coordinationMetadata()).lastAcceptedConfiguration(newConfig).build()
                        )
                )
                .build();
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
            if (node.isMasterNode() && (nodeIdsWithAbsentName.contains(node.getId()) || nodeNamesWithAbsentId.contains(node.getName()))) {
                return false;
            }
        }

        return true;
    }

    private final AtomicBoolean reconfigurationTaskScheduled = new AtomicBoolean();

    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        final ClusterState state = getLastAcceptedState();
        if (improveConfiguration(state) != state && reconfigurationTaskScheduled.compareAndSet(false, true)) {
            logger.trace("scheduling reconfiguration");
            submitUnbatchedTask("reconfigure", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    reconfigurationTaskScheduled.set(false);
                    synchronized (mutex) {
                        return improveConfiguration(currentState);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    reconfigurationTaskScheduled.set(false);
                    logger.debug("reconfiguration failed", e);
                }
            });
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(String source, ClusterStateUpdateTask task) {
        masterService.submitUnbatchedStateUpdateTask(source, task);
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
            logger.debug(() -> "failed to add " + join + " - ignoring", e);
            return false;
        }
    }

    public ClusterState getLastAcceptedState() {
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    private void getLatestStoredStateAfterWinningAnElection(ActionListener<ClusterState> listener, long joiningTerm) {
        // using a SubscribableListener to stay on the current thread if (and only if) nothing async happened
        final var latestStoredStateListener = new SubscribableListener<ClusterState>();
        persistedStateSupplier.get().getLatestStoredState(joiningTerm, latestStoredStateListener);
        latestStoredStateListener.addListener(listener.delegateResponse((delegate, e) -> {
            synchronized (mutex) {
                // TODO: add test coverage for this branch
                becomeCandidate("failed fetching latest stored state");
            }
            delegate.onFailure(e);
        }), transportService.getThreadPool().executor(Names.CLUSTER_COORDINATION), null);
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

    /**
     * Get the last-accepted state, adding a no-master block and removing the master node ID if we are not currently the master. This is the
     * state that the master service uses as input to the cluster state update computation. Note that it's quite expensive to adjust blocks
     * in a large cluster state, so avoid using this where possible.
     */
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

    /**
     * Add a no-master block and remove the master node ID from the given cluster state. Note that it's quite expensive to add blocks in a
     * large cluster state, so avoid using this where possible.
     */
    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        if (clusterState.nodes().getMasterNodeId() != null) {
            // remove block if it already exists before adding new one
            assert clusterState.blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID) == false
                : "NO_MASTER_BLOCK should only be added by Coordinator";
            final ClusterBlocks clusterBlocks = ClusterBlocks.builder()
                .blocks(clusterState.blocks())
                .addGlobalBlock(noMasterBlockService.getNoMasterBlock())
                .build();
            return ClusterState.builder(clusterState).blocks(clusterBlocks).nodes(clusterState.nodes().withMasterNodeId(null)).build();
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
                    logger.debug(
                        () -> format(
                            "[%s] failed publication as node is no longer master for term %s",
                            clusterStatePublicationEvent.getSummary(),
                            clusterStatePublicationEvent.getNewState().term()
                        )
                    );
                    publishListener.onFailure(
                        new FailedToCommitClusterStateException(
                            "node is no longer master for term "
                                + clusterStatePublicationEvent.getNewState().term()
                                + " while handling publication"
                        )
                    );
                    return;
                }

                if (currentPublication.isPresent()) {
                    assert false : "[" + currentPublication.get() + "] in progress, cannot start new publication";
                    logger.warn(
                        () -> format(
                            "[%s] failed publication as already publication in progress",
                            clusterStatePublicationEvent.getSummary()
                        )
                    );
                    publishListener.onFailure(
                        new FailedToCommitClusterStateException("publication " + currentPublication.get() + " already in progress")
                    );
                    return;
                }

                assert assertPreviousStateConsistency(clusterStatePublicationEvent);

                final ClusterState clusterState = clusterStatePublicationEvent.getNewState();

                assert getLocalNode().equals(clusterState.getNodes().get(getLocalNode().getId()))
                    : getLocalNode() + " should be in published " + clusterState;

                final long publicationContextConstructionStartMillis = transportService.getThreadPool().rawRelativeTimeInMillis();
                final PublicationTransportHandler.PublicationContext publicationContext = publicationHandler.newPublicationContext(
                    clusterStatePublicationEvent
                );
                try {
                    clusterStatePublicationEvent.setPublicationContextConstructionElapsedMillis(
                        transportService.getThreadPool().rawRelativeTimeInMillis() - publicationContextConstructionStartMillis
                    );

                    final PublishRequest publishRequest = coordinationState.get().handleClientValue(clusterState);
                    final CoordinatorPublication publication = new CoordinatorPublication(
                        clusterStatePublicationEvent,
                        publishRequest,
                        publicationContext,
                        new ListenableFuture<>(),
                        ackListener,
                        publishListener
                    );
                    currentPublication = Optional.of(publication);

                    final DiscoveryNodes publishNodes = publishRequest.getAcceptedState().nodes();
                    leaderChecker.setCurrentNodes(publishNodes);
                    followersChecker.setCurrentNodes(publishNodes);
                    lagDetector.setTrackedNodes(publishNodes);
                    publication.start(followersChecker.getFaultyNodes());
                } catch (Exception e) {
                    assert currentPublication.isEmpty() : e; // should not fail after setting currentPublication
                    becomeCandidate("publish");
                } finally {
                    publicationContext.decRef();
                }
            }
        } catch (Exception e) {
            logger.debug(() -> "[" + clusterStatePublicationEvent.getSummary() + "] publishing failed", e);
            publishListener.onFailure(new FailedToCommitClusterStateException("publishing failed", e));
        }
    }

    // there is no equals on cluster state, so we just serialize it to XContent and compare Maps
    // deserialized from the resulting JSON
    private boolean assertPreviousStateConsistency(ClusterStatePublicationEvent clusterStatePublicationEvent) {
        assert clusterStatePublicationEvent.getOldState() == coordinationState.get().getLastAcceptedState()
            || XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(clusterStatePublicationEvent.getOldState()), false)
                .equals(
                    XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        Strings.toString(clusterStateWithNoMasterBlock(coordinationState.get().getLastAcceptedState())),
                        false
                    )
                )
            : Strings.toString(clusterStatePublicationEvent.getOldState())
                + " vs "
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

    // for tests
    boolean hasIdleJoinValidationService() {
        return joinValidationService.isIdle();
    }

    public void addPeerFinderListener(PeerFinderListener peerFinderListener) {
        this.peerFinderListeners.add(peerFinderListener);
    }

    public enum Mode {
        CANDIDATE,
        LEADER,
        FOLLOWER
    }

    private class CoordinatorPeerFinder extends PeerFinder {

        CoordinatorPeerFinder(
            Settings settings,
            TransportService transportService,
            TransportAddressConnector transportAddressConnector,
            ConfiguredHostsResolver configuredHostsResolver
        ) {
            super(
                settings,
                transportService,
                transportAddressConnector,
                singleNodeDiscovery ? hostsResolver -> {} : configuredHostsResolver
            );
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
                if (mode == Mode.CANDIDATE) {
                    final VoteCollection expectedVotes = new VoteCollection();
                    getFoundPeers().forEach(expectedVotes::addVote);
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
            peerFinderListeners.forEach(PeerFinderListener::onFoundPeersUpdated);
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
                            logger.trace("skip prevoting as local node may not win election: {}", lastAcceptedState.coordinationMetadata());
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

    public PeerFinder getPeerFinder() {
        return this.peerFinder;
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

    private void beforeCommit(long term, long version, ActionListener<Void> listener) {
        electionStrategy.beforeCommit(term, version, listener);
    }

    class CoordinatorPublication extends Publication {

        private final ClusterStatePublicationEvent clusterStatePublicationEvent;
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

        CoordinatorPublication(
            ClusterStatePublicationEvent clusterStatePublicationEvent,
            PublishRequest publishRequest,
            PublicationTransportHandler.PublicationContext publicationContext,
            ListenableFuture<Void> localNodeAckEvent,
            AckListener ackListener,
            ActionListener<Void> publishListener
        ) {
            super(publishRequest, new AckListener() {
                @Override
                public void onCommit(TimeValue commitTime) {
                    clusterStatePublicationEvent.setPublicationCommitElapsedMillis(commitTime.millis());
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
            }, transportService.getThreadPool()::rawRelativeTimeInMillis);
            this.clusterStatePublicationEvent = clusterStatePublicationEvent;
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
            }, publishTimeout, Names.CLUSTER_COORDINATION);

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
            }, publishInfoTimeout, Names.CLUSTER_COORDINATION);
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
            final long completionTimeMillis = transportService.getThreadPool().rawRelativeTimeInMillis();
            clusterStatePublicationEvent.setPublicationCompletionElapsedMillis(completionTimeMillis - getStartTime());

            localNodeAckEvent.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void ignore) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    assert committed;

                    receivedJoins.forEach(CoordinatorPublication.this::handleAssociatedJoin);
                    assert receivedJoinsProcessed == false;
                    receivedJoinsProcessed = true;

                    clusterApplier.onNewClusterState(
                        CoordinatorPublication.this.toString(),
                        () -> applierState,
                        new ActionListener<Void>() {
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
                            public void onResponse(Void ignored) {
                                onClusterStateApplied();
                                clusterStatePublicationEvent.setMasterApplyElapsedMillis(
                                    transportService.getThreadPool().rawRelativeTimeInMillis() - completionTimeMillis
                                );
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
                                                    completedNodes().forEach(
                                                        completedNode -> futureVoteCollection.addJoinVote(
                                                            new Join(completedNode, node, futureElectionTerm, state.term(), state.version())
                                                        )
                                                    );
                                                    return electionStrategy.isElectionQuorum(
                                                        node,
                                                        futureElectionTerm,
                                                        state.term(),
                                                        state.version(),
                                                        state.getLastCommittedConfiguration(),
                                                        state.getLastAcceptedConfiguration(),
                                                        futureVoteCollection
                                                    );
                                                })
                                                .toList();
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
                        }
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
                    removePublicationAndPossiblyBecomeCandidate("Publication.onCompletion(false)");
                    cancelTimeoutHandlers();

                    final FailedToCommitClusterStateException exception = new FailedToCommitClusterStateException(
                        Strings.format(
                            "publication of cluster state version [%d] in term [%d] failed [committed={}]",
                            publishRequest.getAcceptedState().version(),
                            publishRequest.getAcceptedState().term(),
                            committed
                        ),
                        e
                    );
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
        protected Optional<SubscribableListener<ApplyCommitRequest>> handlePublishResponse(
            DiscoveryNode sourceNode,
            PublishResponse publishResponse
        ) {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            assert getCurrentTerm() >= publishResponse.getTerm();
            return coordinationState.get().handlePublishResponse(sourceNode, publishResponse).map(applyCommitRequest -> {
                final var future = new SubscribableListener<ApplyCommitRequest>();
                beforeCommit(applyCommitRequest.getTerm(), applyCommitRequest.getVersion(), future.map(ignored -> applyCommitRequest));
                future.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(ApplyCommitRequest applyCommitRequest) {}

                    @Override
                    public void onFailure(Exception e) {
                        logger.log(
                            e instanceof CoordinationStateRejectedException ? Level.DEBUG : Level.WARN,
                            Strings.format(
                                "publication of cluster state version [%d] in term [%d] failed to commit after reaching quorum",
                                publishRequest.getAcceptedState().version(),
                                publishRequest.getAcceptedState().term()
                            ),
                            e
                        );
                    }
                });
                return future;
            });
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
        protected void sendPublishRequest(
            DiscoveryNode destination,
            PublishRequest publishRequest,
            ActionListener<PublishWithJoinResponse> responseActionListener
        ) {
            publicationContext.sendPublishRequest(destination, publishRequest, wrapWithMutex(responseActionListener));
        }

        private static final TransportRequestOptions COMMIT_STATE_REQUEST_OPTIONS = TransportRequestOptions.of(
            null,
            TransportRequestOptions.Type.STATE
        );

        @Override
        protected void sendApplyCommit(
            DiscoveryNode destination,
            ApplyCommitRequest applyCommit,
            ActionListener<Empty> responseActionListener
        ) {
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            try {
                transportService.sendRequest(
                    destination,
                    COMMIT_STATE_ACTION_NAME,
                    applyCommit,
                    COMMIT_STATE_REQUEST_OPTIONS,
                    new ActionListenerResponseHandler<>(
                        wrapWithMutex(responseActionListener),
                        in -> Empty.INSTANCE,
                        Names.CLUSTER_COORDINATION
                    )
                );
            } catch (Exception e) {
                responseActionListener.onFailure(e);
            }
        }

        @Override
        protected <T> ActionListener<T> wrapListener(ActionListener<T> listener) {
            return wrapWithMutex(listener);
        }

        @Override
        boolean publicationCompletedIffAllTargetsInactiveOrCancelled() {
            assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
            return super.publicationCompletedIffAllTargetsInactiveOrCancelled();
        }
    }

    public interface PeerFinderListener {
        void onFoundPeersUpdated();
    }
}
