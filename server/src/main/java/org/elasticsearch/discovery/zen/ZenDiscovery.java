/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.coordination.JoinTaskExecutor;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.zen.PublishClusterStateAction.IncomingClusterStateListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class ZenDiscovery extends AbstractLifecycleComponent implements Discovery, PingContextProvider, IncomingClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ZenDiscovery.class);

    public static final Setting<TimeValue> PING_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.zen.ping_timeout", timeValueSeconds(3), Property.NodeScope);
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.join_timeout",
            settings -> TimeValue.timeValueMillis(PING_TIMEOUT_SETTING.get(settings).millis() * 20),
            TimeValue.timeValueMillis(0), Property.NodeScope, Property.Deprecated);
    public static final Setting<Integer> JOIN_RETRY_ATTEMPTS_SETTING =
        Setting.intSetting("discovery.zen.join_retry_attempts", 3, 1, Property.NodeScope, Property.Deprecated);
    public static final Setting<TimeValue> JOIN_RETRY_DELAY_SETTING =
        Setting.positiveTimeSetting("discovery.zen.join_retry_delay", TimeValue.timeValueMillis(100),
            Property.NodeScope, Property.Deprecated);
    public static final Setting<Integer> MAX_PINGS_FROM_ANOTHER_MASTER_SETTING =
        Setting.intSetting("discovery.zen.max_pings_from_another_master", 3, 1, Property.NodeScope, Property.Deprecated);
    public static final Setting<Boolean> SEND_LEAVE_REQUEST_SETTING =
        Setting.boolSetting("discovery.zen.send_leave_request", true, Property.NodeScope, Property.Deprecated);
    public static final Setting<TimeValue> MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.master_election.wait_for_joins_timeout",
            settings -> TimeValue.timeValueMillis(JOIN_TIMEOUT_SETTING.get(settings).millis() / 2), TimeValue.timeValueMillis(0),
            Property.NodeScope, Property.Deprecated);
    public static final Setting<Boolean> MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING =
            Setting.boolSetting("discovery.zen.master_election.ignore_non_master_pings", false, Property.NodeScope, Property.Deprecated);
    public static final Setting<Integer> MAX_PENDING_CLUSTER_STATES_SETTING =
        Setting.intSetting("discovery.zen.publish.max_pending_cluster_states", 25, 1, Property.NodeScope, Property.Deprecated);

    public static final String DISCOVERY_REJOIN_ACTION_NAME = "internal:discovery/zen/rejoin";

    private final TransportService transportService;
    private final MasterService masterService;
    private final DiscoverySettings discoverySettings;
    private final NoMasterBlockService noMasterBlockService;
    protected final ZenPing zenPing; // protected to allow tests access
    private final MasterFaultDetection masterFD;
    private final NodesFaultDetection nodesFD;
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership;
    private final ClusterName clusterName;
    private final ThreadPool threadPool;

    private final TimeValue pingTimeout;
    private final TimeValue joinTimeout;

    /** how many retry attempts to perform if join request failed with an retryable error */
    private final int joinRetryAttempts;
    /** how long to wait before performing another join attempt after a join request failed with an retryable error */
    private final TimeValue joinRetryDelay;

    /** how many pings from *another* master to tolerate before forcing a rejoin on other or local master */
    private final int maxPingsFromAnotherMaster;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;

    private final boolean masterElectionIgnoreNonMasters;
    private final TimeValue masterElectionWaitForJoinsTimeout;

    private final JoinThreadControl joinThreadControl;

    private final PendingClusterStatesQueue pendingStatesQueue;

    private final NodeJoinController nodeJoinController;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterApplier clusterApplier;
    private final AtomicReference<ClusterState> committedState; // last committed cluster state
    private final Object stateMutex = new Object();
    private final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators;

    public ZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService,
                        NamedWriteableRegistry namedWriteableRegistry, MasterService masterService, ClusterApplier clusterApplier,
                        ClusterSettings clusterSettings, SeedHostsProvider hostsProvider, AllocationService allocationService,
                        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators, RerouteService rerouteService) {
        this.onJoinValidators = JoinTaskExecutor.addBuiltInJoinValidators(onJoinValidators);
        this.masterService = masterService;
        this.clusterApplier = clusterApplier;
        this.transportService = transportService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        this.zenPing = newZenPing(settings, threadPool, transportService, hostsProvider);
        this.electMaster = new ElectMasterService(settings);
        this.pingTimeout = PING_TIMEOUT_SETTING.get(settings);
        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.joinRetryAttempts = JOIN_RETRY_ATTEMPTS_SETTING.get(settings);
        this.joinRetryDelay = JOIN_RETRY_DELAY_SETTING.get(settings);
        this.maxPingsFromAnotherMaster = MAX_PINGS_FROM_ANOTHER_MASTER_SETTING.get(settings);
        this.sendLeaveRequest = SEND_LEAVE_REQUEST_SETTING.get(settings);
        this.threadPool = threadPool;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.committedState = new AtomicReference<>();

        this.masterElectionIgnoreNonMasters = MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING.get(settings);
        this.masterElectionWaitForJoinsTimeout = MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.get(settings);

        logger.debug("using ping_timeout [{}], join.timeout [{}], master_election.ignore_non_master [{}]",
                this.pingTimeout, joinTimeout, masterElectionIgnoreNonMasters);

        clusterSettings.addSettingsUpdateConsumer(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            this::handleMinimumMasterNodesChanged, (value) -> {
                final ClusterState clusterState = this.clusterState();
                int masterNodes = clusterState.nodes().getMasterNodes().size();
                // the purpose of this validation is to make sure that the master doesn't step down
                // due to a change in master nodes, which also means that there is no way to revert
                // an accidental change. Since we validate using the current cluster state (and
                // not the one from which the settings come from) we have to be careful and only
                // validate if the local node is already a master. Doing so all the time causes
                // subtle issues. For example, a node that joins a cluster has no nodes in its
                // current cluster state. When it receives a cluster state from the master with
                // a dynamic minimum master nodes setting int it, we must make sure we don't reject
                // it.

                if (clusterState.nodes().isLocalNodeElectedMaster() && value > masterNodes) {
                    throw new IllegalArgumentException("cannot set "
                        + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() + " to more than the current" +
                        " master nodes count [" + masterNodes + "]");
                }
        });

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, this::clusterState, masterService, clusterName);
        this.masterFD.addListener(new MasterNodeFailureListener());
        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService, this::clusterState, clusterName);
        this.nodesFD.addListener(new NodeFaultDetectionListener());
        this.pendingStatesQueue = new PendingClusterStatesQueue(logger, MAX_PENDING_CLUSTER_STATES_SETTING.get(settings));

        this.publishClusterState =
                new PublishClusterStateAction(
                        transportService,
                        namedWriteableRegistry,
                        this,
                        discoverySettings);
        this.membership = new MembershipAction(transportService, new MembershipListener(), onJoinValidators);
        this.joinThreadControl = new JoinThreadControl();

        this.nodeJoinController = new NodeJoinController(settings, masterService, allocationService, electMaster, rerouteService);
        this.nodeRemovalExecutor = new ZenNodeRemovalClusterStateTaskExecutor(allocationService, electMaster, this::submitRejoin, logger);

        masterService.setClusterStateSupplier(this::clusterState);

        transportService.registerRequestHandler(
            DISCOVERY_REJOIN_ACTION_NAME, ThreadPool.Names.SAME, RejoinClusterRequest::new, new RejoinClusterRequestHandler());
    }

    // protected to allow overriding in tests
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                                 SeedHostsProvider hostsProvider) {
        return new UnicastZenPing(settings, threadPool, transportService, hostsProvider, this);
    }

    @Override
    protected void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        synchronized (stateMutex) {
            // set initial state
            assert committedState.get() == null;
            assert localNode != null;
            ClusterState.Builder builder = ClusterState.builder(clusterName);
            ClusterState initialState = builder
                .blocks(ClusterBlocks.builder()
                    .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    .addGlobalBlock(noMasterBlockService.getNoMasterBlock()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
                .build();
            committedState.set(initialState);
            clusterApplier.setInitialState(initialState);
            nodesFD.setLocalNode(localNode);
            joinThreadControl.start();
        }
        zenPing.start();
    }

    @Override
    public void startInitialJoin() {
        // start the join thread from a cluster state update. See {@link JoinThreadControl} for details.
        synchronized (stateMutex) {
            // do the join on a different thread, the caller of this method waits for 30s anyhow till it is discovered
            joinThreadControl.startNewThreadIfNotRunning();
        }
    }

    @Override
    protected void doStop() {
        joinThreadControl.stop();
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        Releasables.close(zenPing); // stop any ongoing pinging
        DiscoveryNodes nodes = clusterState().nodes();
        if (sendLeaveRequest) {
            if (nodes.getMasterNode() == null) {
                // if we don't know who the master is, nothing to do here
            } else if (nodes.isLocalNodeElectedMaster() == false) {
                try {
                    membership.sendLeaveRequestBlocking(nodes.getMasterNode(), nodes.getLocalNode(), TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug(() -> new ParameterizedMessage("failed to send leave request to master [{}]", nodes.getMasterNode()), e);
                }
            } else {
                // we're master -> let other potential master we left and start a master election now rather then wait for masterFD
                DiscoveryNode[] possibleMasters = electMaster.nextPossibleMasters(nodes.getNodes().values(), 5);
                for (DiscoveryNode possibleMaster : possibleMasters) {
                    if (nodes.getLocalNode().equals(possibleMaster)) {
                        continue;
                    }
                    try {
                        membership.sendLeaveRequest(nodes.getLocalNode(), possibleMaster);
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to send leave request from master [{}] to possible master [{}]",
                            nodes.getMasterNode(), possibleMaster), e);
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(masterFD, nodesFD);
    }

    @Override
    public ClusterState clusterState() {
        ClusterState clusterState = committedState.get();
        assert clusterState != null : "accessing cluster state before it is set";
        return clusterState;
    }

    @Override
    public void publish(
        ClusterStatePublicationEvent clusterStatePublicationEvent,
        ActionListener<Void> publishListener,
        AckListener ackListener
    ) {
        ClusterState newState = clusterStatePublicationEvent.getNewState();
        assert newState.getNodes().isLocalNodeElectedMaster()
            : "Shouldn't publish state when not master " + clusterStatePublicationEvent.getSummary();

        try {

            // state got changed locally (maybe because another master published to us)
            if (clusterStatePublicationEvent.getOldState() != this.committedState.get()) {
                throw new FailedToCommitClusterStateException("state was mutated while calculating new CS update");
            }

            pendingStatesQueue.addPending(newState);

            publishClusterState.publish(clusterStatePublicationEvent, electMaster.minimumMasterNodes(), ackListener);
        } catch (FailedToCommitClusterStateException t) {
            // cluster service logs a WARN message
            logger.debug("failed to publish cluster state version [{}] (not enough nodes acknowledged, min master nodes [{}])",
                newState.version(), electMaster.minimumMasterNodes());

            synchronized (stateMutex) {
                pendingStatesQueue.failAllStatesAndClear(
                    new ElasticsearchException("failed to publish cluster state"));

                rejoin("zen-disco-failed-to-publish");
            }

            publishListener.onFailure(t);
            return;
        }

        final DiscoveryNode localNode = newState.getNodes().getLocalNode();
        final AtomicBoolean processedOrFailed = new AtomicBoolean();
        pendingStatesQueue.markAsCommitted(newState.stateUUID(),
            new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedOrFailed.set(true);
                    publishListener.onResponse(null);
                    ackListener.onNodeAck(localNode, null);
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedOrFailed.set(true);
                    publishListener.onFailure(e);
                    ackListener.onNodeAck(localNode, e);
                    logger.warn(() -> new ParameterizedMessage(
                        "failed while applying cluster state locally [{}]", clusterStatePublicationEvent.getSummary()), e);
                }
            });

        synchronized (stateMutex) {
            if (clusterStatePublicationEvent.getOldState() != this.committedState.get()) {
                publishListener.onFailure(
                    new FailedToCommitClusterStateException("local state was mutated while CS update was published to other nodes")
                );
                return;
            }

            boolean sentToApplier = processNextCommittedClusterState("master " + newState.nodes().getMasterNode() +
                " committed version [" + newState.version() + "] source [" + clusterStatePublicationEvent.getSummary() + "]");
            if (sentToApplier == false && processedOrFailed.get() == false) {
                assert false : "cluster state published locally neither processed nor failed: " + newState;
                logger.warn("cluster state with version [{}] that is published locally has neither been processed nor failed",
                    newState.version());
                publishListener.onFailure(new FailedToCommitClusterStateException("cluster state that is published locally has neither " +
                    "been processed nor failed"));
            }
        }
    }

    /**
     * Gets the current set of nodes involved in the node fault detection.
     * NB: for testing purposes
     */
    Set<DiscoveryNode> getFaultDetectionNodes() {
        return nodesFD.getNodes();
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(pendingStatesQueue.stats(), publishClusterState.stats());
    }

    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    /**
     * returns true if zen discovery is started and there is a currently a background thread active for (re)joining
     * the cluster used for testing.
     */
    public boolean joiningCluster() {
        return joinThreadControl.joinThreadActive();
    }

    // used for testing
    public ClusterState[] pendingClusterStates() {
        return pendingStatesQueue.pendingClusterStates();
    }

    PendingClusterStatesQueue pendingClusterStatesQueue() {
        return pendingStatesQueue;
    }

    /**
     * the main function of a join thread. This function is guaranteed to join the cluster
     * or spawn a new join thread upon failure to do so.
     */
    private void innerJoinCluster() {
        DiscoveryNode masterNode = null;
        final Thread currentThread = Thread.currentThread();
        nodeJoinController.startElectionContext();
        while (masterNode == null && joinThreadControl.joinThreadActive(currentThread)) {
            masterNode = findMaster();
        }

        if (joinThreadControl.joinThreadActive(currentThread) == false) {
            logger.trace("thread is no longer in currentJoinThread. Stopping.");
            return;
        }

        if (transportService.getLocalNode().equals(masterNode)) {
            final int requiredJoins = Math.max(0, electMaster.minimumMasterNodes() - 1); // we count as one
            logger.debug("elected as master, waiting for incoming joins ([{}] needed)", requiredJoins);
            nodeJoinController.waitToBeElectedAsMaster(requiredJoins, masterElectionWaitForJoinsTimeout,
                    new NodeJoinController.ElectionCallback() {
                        @Override
                        public void onElectedAsMaster(ClusterState state) {
                            synchronized (stateMutex) {
                                joinThreadControl.markThreadAsDone(currentThread);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.trace("failed while waiting for nodes to join, rejoining", t);
                            synchronized (stateMutex) {
                                joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                            }
                        }
                    }

            );
        } else {
            // process any incoming joins (they will fail because we are not the master)
            nodeJoinController.stopElectionContext(masterNode + " elected");

            // send join request
            final boolean success = joinElectedMaster(masterNode);

            synchronized (stateMutex) {
                if (success) {
                    DiscoveryNode currentMasterNode = this.clusterState().getNodes().getMasterNode();
                    if (currentMasterNode == null) {
                        // Post 1.3.0, the master should publish a new cluster state before acking our join request. we now should have
                        // a valid master.
                        logger.debug("no master node is set, despite of join request completing. retrying pings.");
                        joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                    } else if (currentMasterNode.equals(masterNode) == false) {
                        // update cluster state
                        joinThreadControl.stopRunningThreadAndRejoin("master_switched_while_finalizing_join");
                    }

                    joinThreadControl.markThreadAsDone(currentThread);
                } else {
                    // failed to join. Try again...
                    joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                }
            }
        }
    }

    /**
     * Join a newly elected master.
     *
     * @return true if successful
     */
    private boolean joinElectedMaster(DiscoveryNode masterNode) {
        try {
            // first, make sure we can connect to the master
            transportService.connectToNode(masterNode);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to connect to master [{}], retrying...", masterNode), e);
            return false;
        }
        int joinAttempt = 0; // we retry on illegal state if the master is not yet ready
        while (true) {
            try {
                logger.trace("joining master {}", masterNode);
                membership.sendJoinRequestBlocking(masterNode, transportService.getLocalNode(), joinTimeout);
                return true;
            } catch (Exception e) {
                final Throwable unwrap = ExceptionsHelper.unwrapCause(e);
                if (unwrap instanceof NotMasterException) {
                    if (++joinAttempt == this.joinRetryAttempts) {
                        logger.info("failed to send join request to master [{}], reason [{}], tried [{}] times", masterNode,
                            ExceptionsHelper.detailedMessage(e), joinAttempt);
                        return false;
                    } else {
                        logger.trace("master {} failed with [{}]. retrying... (attempts done: [{}])", masterNode,
                            ExceptionsHelper.detailedMessage(e), joinAttempt);
                    }
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace(() -> new ParameterizedMessage("failed to send join request to master [{}]", masterNode), e);
                    } else {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode,
                            ExceptionsHelper.detailedMessage(e));
                    }
                    return false;
                }
            }

            try {
                Thread.sleep(this.joinRetryDelay.millis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void submitRejoin(String source) {
        synchronized (stateMutex) {
            rejoin(source);
        }
    }

    // visible for testing
    void setCommittedState(ClusterState clusterState) {
        synchronized (stateMutex) {
            committedState.set(clusterState);
        }
    }

    private void removeNode(final DiscoveryNode node, final String source, final String reason) {
        masterService.submitStateUpdateTask(
                source + "(" + node + "), reason(" + reason + ")",
                new NodeRemovalClusterStateTaskExecutor.Task(node, reason),
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor,
                nodeRemovalExecutor);
    }

    private void handleLeaveRequest(final DiscoveryNode node) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (localNodeMaster()) {
            removeNode(node, "zen-disco-node-left", "left");
        } else if (node.equals(clusterState().nodes().getMasterNode())) {
            handleMasterGone(node, null, "shut_down");
        }
    }

    private void handleNodeFailure(final DiscoveryNode node, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (localNodeMaster() == false) {
            // nothing to do here...
            return;
        }
        removeNode(node, "zen-disco-node-failed", reason);
    }

    private void handleMinimumMasterNodesChanged(final int minimumMasterNodes) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        final int prevMinimumMasterNode = ZenDiscovery.this.electMaster.minimumMasterNodes();
        ZenDiscovery.this.electMaster.minimumMasterNodes(minimumMasterNodes);
        if (localNodeMaster() == false) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        synchronized (stateMutex) {
            // check if we have enough master nodes, if not, we need to move into joining the cluster again
            if (electMaster.hasEnoughMasterNodes(committedState.get().nodes()) == false) {
                rejoin("not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" +
                    minimumMasterNodes + "]");
            }
        }
    }

    private void handleMasterGone(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a master failure
            return;
        }
        if (localNodeMaster()) {
            // we might get this on both a master telling us shutting down, and then the disconnect failure
            return;
        }

        logger.info(() -> new ParameterizedMessage("master_left [{}], reason [{}]", masterNode, reason), cause);

        synchronized (stateMutex) {
            if (localNodeMaster() == false && masterNode.equals(committedState.get().nodes().getMasterNode())) {
                // flush any pending cluster states from old master, so it will not be set as master again
                pendingStatesQueue.failAllStatesAndClear(new ElasticsearchException("master left [{}]", reason));
                rejoin("master left (reason = " + reason + ")");
            }
        }
    }

    // return true if state has been sent to applier
    boolean processNextCommittedClusterState(String reason) {
        assert Thread.holdsLock(stateMutex);

        final ClusterState newClusterState = pendingStatesQueue.getNextClusterStateToProcess();
        final ClusterState currentState = committedState.get();
        // all pending states have been processed
        if (newClusterState == null) {
            return false;
        }

        assert newClusterState.nodes().getMasterNode() != null : "received a cluster state without a master";
        assert newClusterState.blocks().hasGlobalBlock(noMasterBlockService.getNoMasterBlock()) == false :
            "received a cluster state with a master block";

        if (currentState.nodes().isLocalNodeElectedMaster() && newClusterState.nodes().isLocalNodeElectedMaster() == false) {
            handleAnotherMaster(currentState, newClusterState.nodes().getMasterNode(), newClusterState.version(),
                "via a new cluster state");
            return false;
        }

        try {
            if (shouldIgnoreOrRejectNewClusterState(logger, currentState, newClusterState)) {
                String message = String.format(
                    Locale.ROOT,
                    "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                    newClusterState.version(),
                    newClusterState.stateUUID(),
                    newClusterState.nodes().getMasterNodeId()
                );
                throw new IllegalStateException(message);
            }
        } catch (Exception e) {
            try {
                pendingStatesQueue.markAsFailed(newClusterState, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
            }
            return false;
        }

        if (currentState.blocks().hasGlobalBlock(noMasterBlockService.getNoMasterBlock())) {
            // its a fresh update from the master as we transition from a start of not having a master to having one
            logger.debug("got first state from fresh master [{}]", newClusterState.nodes().getMasterNodeId());
        }

        if (currentState == newClusterState) {
            return false;
        }

        committedState.set(newClusterState);

        // update failure detection only after the state has been updated to prevent race condition with handleLeaveRequest
        // and handleNodeFailure as those check the current state to determine whether the failure is to be handled by this node
        if (newClusterState.nodes().isLocalNodeElectedMaster()) {
            // update the set of nodes to ping
            nodesFD.updateNodesAndPing(newClusterState);
        } else {
            // check to see that we monitor the correct master of the cluster
            if (masterFD.masterNode() == null || masterFD.masterNode().equals(newClusterState.nodes().getMasterNode()) == false) {
                masterFD.restart(newClusterState.nodes().getMasterNode(),
                    "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
            }
        }

        clusterApplier.onNewClusterState("apply cluster state (from master [" + reason + "])",
            this::clusterState,
            new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    try {
                        pendingStatesQueue.markAsProcessed(newClusterState);
                    } catch (Exception e) {
                        onFailure(source, e);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure applying [{}]", reason), e);
                    try {
                        // TODO: use cluster state uuid instead of full cluster state so that we don't keep reference to CS around
                        // for too long.
                        pendingStatesQueue.markAsFailed(newClusterState, e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error(() -> new ParameterizedMessage("unexpected exception while failing [{}]", reason), inner);
                    }
                }
            });

        return true;
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master and
     * the new cluster state version needs to be equal or higher than our cluster state version.
     * If the first condition fails we reject the cluster state and throw an error.
     * If the second condition fails we ignore the cluster state.
     */
    public static boolean shouldIgnoreOrRejectNewClusterState(Logger logger, ClusterState currentState, ClusterState newClusterState) {
        validateStateIsFromCurrentMaster(logger, currentState.nodes(), newClusterState);

        // reject cluster states that are not new from the same master
        if (currentState.supersedes(newClusterState) ||
                (newClusterState.nodes().getMasterNodeId().equals(currentState.nodes().getMasterNodeId()) &&
                    currentState.version() == newClusterState.version())) {
            // if the new state has a smaller version, and it has the same master node, then no need to process it
            logger.debug("received a cluster state that is not newer than the current one, ignoring (received {}, current {})",
                newClusterState.version(), currentState.version());
            return true;
        }

        // reject older cluster states if we are following a master
        if (currentState.nodes().getMasterNodeId() != null && newClusterState.version() < currentState.version()) {
            logger.debug("received a cluster state that has a lower version than the current one, ignoring (received {}, current {})",
                newClusterState.version(), currentState.version());
            return true;
        }
        return false;
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master
     * This method checks for this and throws an exception if needed
     */

    public static void validateStateIsFromCurrentMaster(Logger logger, DiscoveryNodes currentNodes, ClusterState newClusterState) {
        if (currentNodes.getMasterNodeId() == null) {
            return;
        }
        if (currentNodes.getMasterNodeId().equals(newClusterState.nodes().getMasterNodeId()) == false) {
            logger.warn("received a cluster state from a different master than the current one, rejecting (received {}, current {})",
                newClusterState.nodes().getMasterNode(), currentNodes.getMasterNode());
            throw new IllegalStateException("cluster state from a different master than the current one, rejecting (received " +
                newClusterState.nodes().getMasterNode() + ", current " + currentNodes.getMasterNode() + ")");
        }
    }

    void handleJoinRequest(final DiscoveryNode node, final ClusterState state, final MembershipAction.JoinCallback callback) {
        if (nodeJoinController == null) {
            throw new IllegalStateException("discovery module is not yet started");
        } else {
            // we do this in a couple of places including the cluster update thread. This one here is really just best effort
            // to ensure we fail as fast as possible.
            onJoinValidators.stream().forEach(a -> a.accept(node, state));
            if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                JoinTaskExecutor.ensureVersionBarrier(node.getVersion(), state.getNodes().getMinNodeVersion());
            }
            // try and connect to the node, if it fails, we can raise an exception back to the client...
            transportService.connectToNode(node);

            // validate the join request, will throw a failure if it fails, which will get back to the
            // node calling the join request
            try {
                membership.sendValidateJoinRequestBlocking(node, state, joinTimeout);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to validate incoming join request from node [{}]", node),
                    e);
                callback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
                return;
            }
            nodeJoinController.handleJoinRequest(node, callback);
        }
    }

    private DiscoveryNode findMaster() {
        logger.trace("starting to ping");
        List<ZenPing.PingResponse> fullPingResponses = pingAndWait(pingTimeout).toList();
        if (fullPingResponses == null) {
            logger.trace("No full ping responses");
            return null;
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            if (fullPingResponses.size() == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : fullPingResponses) {
                    sb.append("\n\t--> ").append(pingResponse);
                }
            }
            logger.trace("full ping responses:{}", sb);
        }

        final DiscoveryNode localNode = transportService.getLocalNode();

        // add our selves
        assert fullPingResponses.stream().map(ZenPing.PingResponse::node)
            .filter(n -> n.equals(localNode)).findAny().isPresent() == false;

        fullPingResponses.add(new ZenPing.PingResponse(localNode, null, this.clusterState()));

        // filter responses
        final List<ZenPing.PingResponse> pingResponses = filterPingResponses(fullPingResponses, masterElectionIgnoreNonMasters, logger);

        List<DiscoveryNode> activeMasters = new ArrayList<>();
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            // We can't include the local node in pingMasters list, otherwise we may up electing ourselves without
            // any check / verifications from other nodes in ZenDiscover#innerJoinCluster()
            if (pingResponse.master() != null && localNode.equals(pingResponse.master()) == false) {
                activeMasters.add(pingResponse.master());
            }
        }

        // nodes discovered during pinging
        List<ElectMasterService.MasterCandidate> masterCandidates = new ArrayList<>();
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            if (pingResponse.node().isMasterNode()) {
                masterCandidates.add(new ElectMasterService.MasterCandidate(pingResponse.node(), pingResponse.getClusterStateVersion()));
            }
        }

        if (activeMasters.isEmpty()) {
            if (electMaster.hasEnoughCandidates(masterCandidates)) {
                final ElectMasterService.MasterCandidate winner = electMaster.electMaster(masterCandidates);
                logger.trace("candidate {} won election", winner);
                return winner.getNode();
            } else {
                // if we don't have enough master nodes, we bail, because there are not enough master to elect from
                logger.warn("not enough master nodes discovered during pinging (found [{}], but needed [{}]), pinging again",
                            masterCandidates, electMaster.minimumMasterNodes());
                return null;
            }
        } else {
            assert activeMasters.contains(localNode) == false :
                "local node should never be elected as master when other nodes indicate an active master";
            // lets tie break between discovered nodes
            return electMaster.tieBreakActiveMasters(activeMasters);
        }
    }

    static List<ZenPing.PingResponse> filterPingResponses(List<ZenPing.PingResponse> fullPingResponses,
                                                          boolean masterElectionIgnoreNonMasters, Logger logger) {
        List<ZenPing.PingResponse> pingResponses;
        if (masterElectionIgnoreNonMasters) {
            pingResponses = fullPingResponses.stream().filter(ping -> ping.node().isMasterNode()).collect(Collectors.toList());
        } else {
            pingResponses = fullPingResponses;
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            if (pingResponses.isEmpty()) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : pingResponses) {
                    sb.append("\n\t--> ").append(pingResponse);
                }
            }
            logger.debug("filtered ping responses: (ignore_non_masters [{}]){}", masterElectionIgnoreNonMasters, sb);
        }
        return pingResponses;
    }

    protected void rejoin(String reason) {
        assert Thread.holdsLock(stateMutex);
        ClusterState clusterState = committedState.get();

        logger.warn("{}, current nodes: {}", reason, clusterState.nodes());
        nodesFD.stop();
        masterFD.stop(reason);

        // TODO: do we want to force a new thread if we actively removed the master? this is to give a full pinging cycle
        // before a decision is made.
        joinThreadControl.startNewThreadIfNotRunning();

        if (clusterState.nodes().getMasterNodeId() != null) {
            // remove block if it already exists before adding new one
            assert clusterState.blocks().hasGlobalBlockWithId(noMasterBlockService.getNoMasterBlock().id()) == false :
                "NO_MASTER_BLOCK should only be added by ZenDiscovery";
            ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(noMasterBlockService.getNoMasterBlock())
                .build();

            DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();
            clusterState = ClusterState.builder(clusterState)
                .blocks(clusterBlocks)
                .nodes(discoveryNodes)
                .build();

            committedState.set(clusterState);
            clusterApplier.onNewClusterState(reason, this::clusterState, (source, e) -> {}); // don't wait for state to be applied
        }
    }

    private boolean localNodeMaster() {
        return clusterState().nodes().isLocalNodeElectedMaster();
    }

    private void handleAnotherMaster(ClusterState localClusterState, final DiscoveryNode otherMaster, long otherClusterStateVersion,
                                     String reason) {
        assert localClusterState.nodes().isLocalNodeElectedMaster() : "handleAnotherMaster called but current node is not a master";
        assert Thread.holdsLock(stateMutex);

        if (otherClusterStateVersion > localClusterState.version()) {
            rejoin("zen-disco-discovered another master with a new cluster_state [" + otherMaster + "][" + reason + "]");
        } else {
            // TODO: do this outside mutex
            logger.warn("discovered [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster ([{}])",
                otherMaster, otherMaster, reason);
            try {
                // make sure we're connected to this node (connect to node does nothing if we're already connected)
                // since the network connections are asymmetric, it may be that we received a state but have disconnected from the node
                // in the past (after a master failure, for example)
                transportService.connectToNode(otherMaster);
                transportService.sendRequest(otherMaster, DISCOVERY_REJOIN_ACTION_NAME,
                    new RejoinClusterRequest(localClusterState.nodes().getLocalNodeId()),
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn(() -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), exp);
                    }
                });
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to send rejoin request to [{}]", otherMaster), e);
            }
        }
    }

    private ZenPing.PingCollection pingAndWait(TimeValue timeout) {
        final CompletableFuture<ZenPing.PingCollection> response = new CompletableFuture<>();
        try {
            zenPing.ping(response::complete, timeout);
        } catch (Exception ex) {
            // logged later
            response.completeExceptionally(ex);
        }

        try {
            return response.get();
        } catch (InterruptedException e) {
            logger.trace("pingAndWait interrupted");
            return new ZenPing.PingCollection();
        } catch (ExecutionException e) {
            logger.warn("Ping execution failed", e);
            return new ZenPing.PingCollection();
        }
    }

    @Override
    public void onIncomingClusterState(ClusterState incomingState) {
        validateIncomingState(logger, incomingState, committedState.get());
        pendingStatesQueue.addPending(incomingState);
    }

    @Override
    public void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener) {
        final ClusterState state = pendingStatesQueue.markAsCommitted(stateUUID,
            new PendingClusterStatesQueue.StateProcessedListener() {
                @Override
                public void onNewClusterStateProcessed() {
                    processedListener.onResponse(null);
                }

                @Override
                public void onNewClusterStateFailed(Exception e) {
                    processedListener.onFailure(e);
                }
            });
        if (state != null) {
            synchronized (stateMutex) {
                processNextCommittedClusterState("master " + state.nodes().getMasterNode() +
                    " committed version [" + state.version() + "]");
            }
        }
    }

    /**
     * does simple sanity check of the incoming cluster state. Throws an exception on rejections.
     */
    static void validateIncomingState(Logger logger, ClusterState incomingState, ClusterState lastState) {
        final ClusterName incomingClusterName = incomingState.getClusterName();
        if (incomingClusterName.equals(lastState.getClusterName()) == false) {
            logger.warn("received cluster state from [{}] which is also master but with a different cluster name [{}]",
                incomingState.nodes().getMasterNode(), incomingClusterName);
            throw new IllegalStateException("received state from a node that is not part of the cluster");
        }
        if (lastState.nodes().getLocalNode().equals(incomingState.nodes().getLocalNode()) == false) {
            logger.warn("received a cluster state from [{}] and not part of the cluster, should not happen",
                incomingState.nodes().getMasterNode());
            throw new IllegalStateException("received state with a local node that does not match the current local node");
        }

        if (shouldIgnoreOrRejectNewClusterState(logger, lastState, incomingState)) {
            String message = String.format(
                Locale.ROOT,
                "rejecting cluster state version [%d] uuid [%s] received from [%s]",
                incomingState.version(),
                incomingState.stateUUID(),
                incomingState.nodes().getMasterNodeId()
            );
            logger.warn(message);
            throw new IllegalStateException(message);
        }

    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public void onJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            handleJoinRequest(node, ZenDiscovery.this.clusterState(), callback);
        }

        @Override
        public void onLeave(DiscoveryNode node) {
            handleLeaveRequest(node);
        }
    }

    private class NodeFaultDetectionListener extends NodesFaultDetection.Listener {

        private final AtomicInteger pingsWhileMaster = new AtomicInteger(0);

        @Override
        public void onNodeFailure(DiscoveryNode node, String reason) {
            handleNodeFailure(node, reason);
        }

        @Override
        public void onPingReceived(final NodesFaultDetection.PingRequest pingRequest) {
            // if we are master, we don't expect any fault detection from another node. If we get it
            // means we potentially have two masters in the cluster.
            if (localNodeMaster() == false) {
                pingsWhileMaster.set(0);
                return;
            }

            if (pingsWhileMaster.incrementAndGet() < maxPingsFromAnotherMaster) {
                logger.trace("got a ping from another master {}. current ping count: [{}]", pingRequest.masterNode(),
                    pingsWhileMaster.get());
                return;
            }
            logger.debug("got a ping from another master {}. resolving who should rejoin. current ping count: [{}]",
                pingRequest.masterNode(), pingsWhileMaster.get());
            synchronized (stateMutex) {
                ClusterState currentState = committedState.get();
                if (currentState.nodes().isLocalNodeElectedMaster()) {
                    pingsWhileMaster.set(0);
                    handleAnotherMaster(currentState, pingRequest.masterNode(), pingRequest.clusterStateVersion(), "node fd ping");
                }
            }
        }
    }

    private class MasterNodeFailureListener implements MasterFaultDetection.Listener {

        @Override
        public void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason) {
            handleMasterGone(masterNode, cause, reason);
        }
    }

    public static class RejoinClusterRequest extends TransportRequest {

        private String fromNodeId;

        RejoinClusterRequest(String fromNodeId) {
            this.fromNodeId = fromNodeId;
        }

        public RejoinClusterRequest(StreamInput in) throws IOException {
            super(in);
            fromNodeId = in.readOptionalString();

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(fromNodeId);
        }
    }

    class RejoinClusterRequestHandler implements TransportRequestHandler<RejoinClusterRequest> {
        @Override
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel, Task task) throws Exception {
            try {
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception e) {
                logger.warn("failed to send response on rejoin cluster request handling", e);
            }
            synchronized (stateMutex) {
                rejoin("received a request to rejoin the cluster from [" + request.fromNodeId + "]");
            }
        }
    }

    /**
     * All control of the join thread should happen under the cluster state update task thread.
     * This is important to make sure that the background joining process is always in sync with any cluster state updates
     * like master loss, failure to join, received cluster state while joining etc.
     */
    private class JoinThreadControl {

        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicReference<Thread> currentJoinThread = new AtomicReference<>();

        /** returns true if join thread control is started and there is currently an active join thread */
        public boolean joinThreadActive() {
            Thread currentThread = currentJoinThread.get();
            return running.get() && currentThread != null && currentThread.isAlive();
        }

        /** returns true if join thread control is started and the supplied thread is the currently active joinThread */
        public boolean joinThreadActive(Thread joinThread) {
            return running.get() && joinThread.equals(currentJoinThread.get());
        }

        /** cleans any running joining thread and calls {@link #rejoin} */
        public void stopRunningThreadAndRejoin(String reason) {
            assert Thread.holdsLock(stateMutex);
            currentJoinThread.set(null);
            rejoin(reason);
        }

        /** starts a new joining thread if there is no currently active one and join thread controlling is started */
        public void startNewThreadIfNotRunning() {
            assert Thread.holdsLock(stateMutex);
            if (joinThreadActive()) {
                return;
            }
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    Thread currentThread = Thread.currentThread();
                    if (currentJoinThread.compareAndSet(null, currentThread) == false) {
                        return;
                    }
                    while (running.get() && joinThreadActive(currentThread)) {
                        try {
                            innerJoinCluster();
                            return;
                        } catch (Exception e) {
                            logger.error("unexpected error while joining cluster, trying again", e);
                            // Because we catch any exception here, we want to know in
                            // tests if an uncaught exception got to this point and the test infra uncaught exception
                            // leak detection can catch this. In practise no uncaught exception should leak
                            assert ExceptionsHelper.reThrowIfNotNull(e);
                        }
                    }
                    // cleaning the current thread from currentJoinThread is done by explicit calls.
                }
            });
        }

        /**
         * marks the given joinThread as completed and makes sure another thread is running (starting one if needed)
         * If the given thread is not the currently running join thread, the command is ignored.
         */
        public void markThreadAsDoneAndStartNew(Thread joinThread) {
            assert Thread.holdsLock(stateMutex);
            if (markThreadAsDone(joinThread) == false) {
                return;
            }
            startNewThreadIfNotRunning();
        }

        /** marks the given joinThread as completed. Returns false if the supplied thread is not the currently active join thread */
        public boolean markThreadAsDone(Thread joinThread) {
            assert Thread.holdsLock(stateMutex);
            return currentJoinThread.compareAndSet(joinThread, null);
        }

        public void stop() {
            running.set(false);
            Thread joinThread = currentJoinThread.getAndSet(null);
            if (joinThread != null) {
                joinThread.interrupt();
            }
        }

        public void start() {
            running.set(true);
        }

    }

    public final Collection<BiConsumer<DiscoveryNode, ClusterState>> getOnJoinValidators() {
        return onJoinValidators;
    }

    static class ZenNodeRemovalClusterStateTaskExecutor extends NodeRemovalClusterStateTaskExecutor {

        private final ElectMasterService electMasterService;
        private final Consumer<String> rejoin;

        ZenNodeRemovalClusterStateTaskExecutor(
            final AllocationService allocationService,
            final ElectMasterService electMasterService,
            final Consumer<String> rejoin,
            final Logger logger) {
            super(allocationService, logger);
            this.electMasterService = electMasterService;
            this.rejoin = rejoin;
        }

        @Override
        protected ClusterTasksResult<Task> getTaskClusterTasksResult(ClusterState currentState, List<Task> tasks,
                                                                     ClusterState remainingNodesClusterState) {
            if (electMasterService.hasEnoughMasterNodes(remainingNodesClusterState.nodes()) == false) {
                final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
                final int masterNodes = electMasterService.countMasterNodes(remainingNodesClusterState.nodes());
                rejoin.accept(LoggerMessageFormat.format("not enough master nodes (has [{}], but needed [{}])",
                    masterNodes, electMasterService.minimumMasterNodes()));
                return resultBuilder.build(currentState);
            } else {
                return super.getTaskClusterTasksResult(currentState, tasks, remainingNodesClusterState);
            }
        }
    }
}
