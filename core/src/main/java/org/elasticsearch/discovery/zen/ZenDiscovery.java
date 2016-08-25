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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.MasterFaultDetection;
import org.elasticsearch.discovery.zen.fd.NodesFaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.PingContextProvider;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.publish.PendingClusterStateStats;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class ZenDiscovery extends AbstractLifecycleComponent implements Discovery, PingContextProvider {

    public static final Setting<TimeValue> PING_TIMEOUT_SETTING =
        Setting.positiveTimeSetting("discovery.zen.ping_timeout", timeValueSeconds(3), Property.NodeScope);
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.join_timeout",
            settings -> TimeValue.timeValueMillis(PING_TIMEOUT_SETTING.get(settings).millis() * 20).toString(),
            TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> JOIN_RETRY_ATTEMPTS_SETTING =
        Setting.intSetting("discovery.zen.join_retry_attempts", 3, 1, Property.NodeScope);
    public static final Setting<TimeValue> JOIN_RETRY_DELAY_SETTING =
        Setting.positiveTimeSetting("discovery.zen.join_retry_delay", TimeValue.timeValueMillis(100), Property.NodeScope);
    public static final Setting<Integer> MAX_PINGS_FROM_ANOTHER_MASTER_SETTING =
        Setting.intSetting("discovery.zen.max_pings_from_another_master", 3, 1, Property.NodeScope);
    public static final Setting<Boolean> SEND_LEAVE_REQUEST_SETTING =
        Setting.boolSetting("discovery.zen.send_leave_request", true, Property.NodeScope);
    public static final Setting<TimeValue> MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.master_election.wait_for_joins_timeout",
            settings -> TimeValue.timeValueMillis(JOIN_TIMEOUT_SETTING.get(settings).millis() / 2).toString(), TimeValue.timeValueMillis(0),
            Property.NodeScope);
    public static final Setting<Boolean> MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING =
            Setting.boolSetting("discovery.zen.master_election.ignore_non_master_pings", false, Property.NodeScope);

    public static final String DISCOVERY_REJOIN_ACTION_NAME = "internal:discovery/zen/rejoin";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private AllocationService allocationService;
    private final ClusterName clusterName;
    private final DiscoverySettings discoverySettings;
    private final ZenPingService pingService;
    private final MasterFaultDetection masterFD;
    private final NodesFaultDetection nodesFD;
    private final PublishClusterStateAction publishClusterState;
    private final MembershipAction membership;

    private final TimeValue pingTimeout;
    private final TimeValue joinTimeout;

    /** how many retry attempts to perform if join request failed with an retriable error */
    private final int joinRetryAttempts;
    /** how long to wait before performing another join attempt after a join request failed with an retriable error */
    private final TimeValue joinRetryDelay;

    /** how many pings from *another* master to tolerate before forcing a rejoin on other or local master */
    private final int maxPingsFromAnotherMaster;

    // a flag that should be used only for testing
    private final boolean sendLeaveRequest;

    private final ElectMasterService electMaster;

    private final boolean masterElectionIgnoreNonMasters;
    private final TimeValue masterElectionWaitForJoinsTimeout;

    private final JoinThreadControl joinThreadControl;

    /** counts the time this node has joined the cluster or have elected it self as master */
    private final AtomicLong clusterJoinsCounter = new AtomicLong();

    // must initialized in doStart(), when we have the allocationService set
    private volatile NodeJoinController nodeJoinController;
    private volatile NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;

    @Inject
    public ZenDiscovery(Settings settings, ThreadPool threadPool,
                        TransportService transportService, final ClusterService clusterService, ClusterSettings clusterSettings,
                        ZenPingService pingService, ElectMasterService electMasterService) {
        super(settings);
        this.clusterService = clusterService;
        this.clusterName = clusterService.getClusterName();
        this.transportService = transportService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
        this.pingService = pingService;
        this.electMaster = electMasterService;
        this.pingTimeout = PING_TIMEOUT_SETTING.get(settings);

        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.joinRetryAttempts = JOIN_RETRY_ATTEMPTS_SETTING.get(settings);
        this.joinRetryDelay = JOIN_RETRY_DELAY_SETTING.get(settings);
        this.maxPingsFromAnotherMaster = MAX_PINGS_FROM_ANOTHER_MASTER_SETTING.get(settings);
        this.sendLeaveRequest = SEND_LEAVE_REQUEST_SETTING.get(settings);

        this.masterElectionIgnoreNonMasters = MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING.get(settings);
        this.masterElectionWaitForJoinsTimeout = MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.get(settings);

        logger.debug("using ping_timeout [{}], join.timeout [{}], master_election.ignore_non_master [{}]",
                this.pingTimeout, joinTimeout, masterElectionIgnoreNonMasters);

        clusterSettings.addSettingsUpdateConsumer(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING, this::handleMinimumMasterNodesChanged, (value) -> {
            final ClusterState clusterState = clusterService.state();
            int masterNodes = clusterState.nodes().getMasterNodes().size();
            if (value > masterNodes) {
                throw new IllegalArgumentException("cannot set " + ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey() + " to more than the current master nodes count [" + masterNodes + "]");
            }
        });

        this.masterFD = new MasterFaultDetection(settings, threadPool, transportService, clusterService);
        this.masterFD.addListener(new MasterNodeFailureListener());

        this.nodesFD = new NodesFaultDetection(settings, threadPool, transportService, clusterService.getClusterName());
        this.nodesFD.addListener(new NodeFaultDetectionListener());

        this.publishClusterState =
                new PublishClusterStateAction(
                        settings,
                        transportService,
                        clusterService::state,
                        new NewPendingClusterStateListener(),
                        discoverySettings,
                        clusterService.getClusterName());
        this.pingService.setPingContextProvider(this);
        this.membership = new MembershipAction(settings, transportService, this, new MembershipListener());

        this.joinThreadControl = new JoinThreadControl(threadPool);

        transportService.registerRequestHandler(
            DISCOVERY_REJOIN_ACTION_NAME, RejoinClusterRequest::new, ThreadPool.Names.SAME, new RejoinClusterRequestHandler());
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    protected void doStart() {
        nodesFD.setLocalNode(clusterService.localNode());
        joinThreadControl.start();
        pingService.start();
        this.nodeJoinController = new NodeJoinController(clusterService, allocationService, electMaster, discoverySettings, settings);
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, electMaster, this::rejoin, logger);
    }

    @Override
    public void startInitialJoin() {
        // start the join thread from a cluster state update. See {@link JoinThreadControl} for details.
        clusterService.submitStateUpdateTask("initial_join", new ClusterStateUpdateTask() {

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // do the join on a different thread, the DiscoveryService waits for 30s anyhow till it is discovered
                joinThreadControl.startNewThreadIfNotRunning();
                return currentState;
            }

            @Override
            public void onFailure(String source, @org.elasticsearch.common.Nullable Exception e) {
                logger.warn("failed to start initial join process", e);
            }
        });
    }

    @Override
    protected void doStop() {
        joinThreadControl.stop();
        pingService.stop();
        masterFD.stop("zen disco stop");
        nodesFD.stop();
        DiscoveryNodes nodes = nodes();
        if (sendLeaveRequest) {
            if (nodes.getMasterNode() == null) {
                // if we don't know who the master is, nothing to do here
            } else if (!nodes.isLocalNodeElectedMaster()) {
                try {
                    membership.sendLeaveRequestBlocking(nodes.getMasterNode(), nodes.getLocalNode(), TimeValue.timeValueSeconds(1));
                } catch (Exception e) {
                    logger.debug("failed to send leave request to master [{}]", e, nodes.getMasterNode());
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
                        logger.debug("failed to send leave request from master [{}] to possible master [{}]", e, nodes.getMasterNode(), possibleMaster);
                    }
                }
            }
        }
    }

    @Override
    protected void doClose() {
        masterFD.close();
        nodesFD.close();
        publishClusterState.close();
        membership.close();
        pingService.close();
    }

    @Override
    public DiscoveryNode localNode() {
        return clusterService.localNode();
    }

    @Override
    public String nodeDescription() {
        return clusterName.value() + "/" + clusterService.localNode().getId();
    }

    /** start of {@link org.elasticsearch.discovery.zen.ping.PingContextProvider } implementation */
    @Override
    public DiscoveryNodes nodes() {
        return clusterService.state().nodes();
    }

    @Override
    public boolean nodeHasJoinedClusterOnce() {
        return clusterJoinsCounter.get() > 0;
    }

    /** end of {@link org.elasticsearch.discovery.zen.ping.PingContextProvider } implementation */


    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        if (!clusterChangedEvent.state().getNodes().isLocalNodeElectedMaster()) {
            throw new IllegalStateException("Shouldn't publish state when not master");
        }
        nodesFD.updateNodesAndPing(clusterChangedEvent.state());
        try {
            publishClusterState.publish(clusterChangedEvent, electMaster.minimumMasterNodes(), ackListener);
        } catch (FailedToCommitClusterStateException t) {
            // cluster service logs a WARN message
            logger.debug("failed to publish cluster state version [{}] (not enough nodes acknowledged, min master nodes [{}])", clusterChangedEvent.state().version(), electMaster.minimumMasterNodes());
            clusterService.submitStateUpdateTask("zen-disco-failed-to-publish", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return rejoin(currentState, "failed to publish to min_master_nodes");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("unexpected failure during [{}]", e, source);
                }

            });
            throw t;
        }
    }

    @Override
    public DiscoveryStats stats() {
        PendingClusterStateStats queueStats = publishClusterState.pendingStatesQueue().stats();
        return new DiscoveryStats(queueStats);
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public int getMinimumMasterNodes() {
        return electMaster.minimumMasterNodes();
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
        return publishClusterState.pendingStatesQueue().pendingClusterStates();
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

        if (!joinThreadControl.joinThreadActive(currentThread)) {
            logger.trace("thread is no longer in currentJoinThread. Stopping.");
            return;
        }

        if (clusterService.localNode().equals(masterNode)) {
            final int requiredJoins = Math.max(0, electMaster.minimumMasterNodes() - 1); // we count as one
            logger.debug("elected as master, waiting for incoming joins ([{}] needed)", requiredJoins);
            nodeJoinController.waitToBeElectedAsMaster(requiredJoins, masterElectionWaitForJoinsTimeout,
                    new NodeJoinController.ElectionCallback() {
                        @Override
                        public void onElectedAsMaster(ClusterState state) {
                            joinThreadControl.markThreadAsDone(currentThread);
                            // we only starts nodesFD if we are master (it may be that we received a cluster state while pinging)
                            nodesFD.updateNodesAndPing(state); // start the nodes FD
                            long count = clusterJoinsCounter.incrementAndGet();
                            logger.trace("cluster joins counter set to [{}] (elected as master)", count);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.trace("failed while waiting for nodes to join, rejoining", t);
                            joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                        }
                    }

            );
        } else {
            // process any incoming joins (they will fail because we are not the master)
            nodeJoinController.stopElectionContext(masterNode + " elected");

            // send join request
            final boolean success = joinElectedMaster(masterNode);

            // finalize join through the cluster state update thread
            final DiscoveryNode finalMasterNode = masterNode;
            clusterService.submitStateUpdateTask("finalize_join (" + masterNode + ")", new ClusterStateUpdateTask() {
                @Override
                public boolean runOnlyOnMaster() {
                    return false;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (!success) {
                        // failed to join. Try again...
                        joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                        return currentState;
                    }

                    if (currentState.getNodes().getMasterNode() == null) {
                        // Post 1.3.0, the master should publish a new cluster state before acking our join request. we now should have
                        // a valid master.
                        logger.debug("no master node is set, despite of join request completing. retrying pings.");
                        joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                        return currentState;
                    }

                    if (!currentState.getNodes().getMasterNode().equals(finalMasterNode)) {
                        return joinThreadControl.stopRunningThreadAndRejoin(currentState, "master_switched_while_finalizing_join");
                    }

                    // Note: we do not have to start master fault detection here because it's set at {@link #processNextPendingClusterState }
                    // when the first cluster state arrives.
                    joinThreadControl.markThreadAsDone(currentThread);
                    return currentState;
                }

                @Override
                public void onFailure(String source, @Nullable Exception e) {
                    logger.error("unexpected error while trying to finalize cluster join", e);
                    joinThreadControl.markThreadAsDoneAndStartNew(currentThread);
                }
            });
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
            logger.warn("failed to connect to master [{}], retrying...", e, masterNode);
            return false;
        }
        int joinAttempt = 0; // we retry on illegal state if the master is not yet ready
        while (true) {
            try {
                logger.trace("joining master {}", masterNode);
                membership.sendJoinRequestBlocking(masterNode, clusterService.localNode(), joinTimeout);
                return true;
            } catch (Exception e) {
                final Throwable unwrap = ExceptionsHelper.unwrapCause(e);
                if (unwrap instanceof NotMasterException) {
                    if (++joinAttempt == this.joinRetryAttempts) {
                        logger.info("failed to send join request to master [{}], reason [{}], tried [{}] times", masterNode, ExceptionsHelper.detailedMessage(e), joinAttempt);
                        return false;
                    } else {
                        logger.trace("master {} failed with [{}]. retrying... (attempts done: [{}])", masterNode, ExceptionsHelper.detailedMessage(e), joinAttempt);
                    }
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("failed to send join request to master [{}]", e, masterNode);
                    } else {
                        logger.info("failed to send join request to master [{}], reason [{}]", masterNode, ExceptionsHelper.detailedMessage(e));
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

    // visible for testing
    static class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task>, ClusterStateTaskListener {

        private final AllocationService allocationService;
        private final ElectMasterService electMasterService;
        private final BiFunction<ClusterState, String, ClusterState> rejoin;
        private final ESLogger logger;

        static class Task {

            private final DiscoveryNode node;
            private final String reason;

            public Task(final DiscoveryNode node, final String reason) {
                this.node = node;
                this.reason = reason;
            }

            public DiscoveryNode node() {
                return node;
            }

            public String reason() {
                return reason;
            }

            @Override
            public String toString() {
                return node + " " + reason;
            }
        }

        NodeRemovalClusterStateTaskExecutor(
                final AllocationService allocationService,
                final ElectMasterService electMasterService,
                final BiFunction<ClusterState, String, ClusterState> rejoin,
                final ESLogger logger) {
            this.allocationService = allocationService;
            this.electMasterService = electMasterService;
            this.rejoin = rejoin;
            this.logger = logger;
        }

        @Override
        public BatchResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
            final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
            boolean removed = false;
            for (final Task task : tasks) {
                if (currentState.nodes().nodeExists(task.node())) {
                    remainingNodesBuilder.remove(task.node());
                    removed = true;
                } else {
                    logger.debug("node [{}] does not exist in cluster state, ignoring", task);
                }
            }

            if (!removed) {
                // no nodes to remove, keep the current cluster state
                return BatchResult.<Task>builder().successes(tasks).build(currentState);
            }

            final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);

            final BatchResult.Builder<Task> resultBuilder = BatchResult.<Task>builder().successes(tasks);
            if (!electMasterService.hasEnoughMasterNodes(remainingNodesClusterState.nodes())) {
                return resultBuilder.build(rejoin.apply(remainingNodesClusterState, "not enough master nodes"));
            } else {
                final RoutingAllocation.Result routingResult =
                    allocationService.deassociateDeadNodes(remainingNodesClusterState, true, describeTasks(tasks));
                return resultBuilder.build(ClusterState.builder(remainingNodesClusterState).routingResult(routingResult).build());
            }
        }

        // visible for testing
        // hook is used in testing to ensure that correct cluster state is used to test whether a
        // rejoin or reroute is needed
        ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
            return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error("unexpected failure during [{}]", e, source);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing node removal [{}]", source);
        }

    }

    private void removeNode(final DiscoveryNode node, final String source, final String reason) {
        clusterService.submitStateUpdateTask(
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
        } else if (node.equals(nodes().getMasterNode())) {
            handleMasterGone(node, null, "shut_down");
        }
    }

    private void handleNodeFailure(final DiscoveryNode node, final String reason) {
        if (lifecycleState() != Lifecycle.State.STARTED) {
            // not started, ignore a node failure
            return;
        }
        if (!localNodeMaster()) {
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
        if (!localNodeMaster()) {
            // We only set the new value. If the master doesn't see enough nodes it will revoke it's mastership.
            return;
        }
        clusterService.submitStateUpdateTask("zen-disco-mini-master-nodes-changed", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // check if we have enough master nodes, if not, we need to move into joining the cluster again
                if (!electMaster.hasEnoughMasterNodes(currentState.nodes())) {
                    return rejoin(currentState, "not enough master nodes on change of minimum_master_nodes from [" + prevMinimumMasterNode + "] to [" + minimumMasterNodes + "]");
                }
                return currentState;
            }


            @Override
            public void onNoLongerMaster(String source) {
                // ignoring (already logged)
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                electMaster.logMinimumMasterNodesWarningIfNecessary(oldState, newState);
            }
        });
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

        logger.info("master_left [{}], reason [{}]", cause, masterNode, reason);

        clusterService.submitStateUpdateTask("master_failed (" + masterNode + ")", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (!masterNode.equals(currentState.nodes().getMasterNode())) {
                    // master got switched on us, no need to send anything
                    return currentState;
                }

                DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(currentState.nodes())
                        // make sure the old master node, which has failed, is not part of the nodes we publish
                        .remove(masterNode)
                        .masterNodeId(null).build();

                // flush any pending cluster states from old master, so it will not be set as master again
                publishClusterState.pendingStatesQueue().failAllStatesAndClear(new ElasticsearchException("master left [{}]", reason));

                return rejoin(ClusterState.builder(currentState).nodes(discoveryNodes).build(), "master left (reason = " + reason + ")");
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }

        });
    }

    void processNextPendingClusterState(String reason) {
        clusterService.submitStateUpdateTask("zen-disco-receive(from master [" + reason + "])", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            ClusterState newClusterState = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                newClusterState = publishClusterState.pendingStatesQueue().getNextClusterStateToProcess();

                // all pending states have been processed
                if (newClusterState == null) {
                    return currentState;
                }

                assert newClusterState.nodes().getMasterNode() != null : "received a cluster state without a master";
                assert !newClusterState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock()) : "received a cluster state with a master block";

                if (currentState.nodes().isLocalNodeElectedMaster()) {
                    return handleAnotherMaster(currentState, newClusterState.nodes().getMasterNode(), newClusterState.version(), "via a new cluster state");
                }

                if (shouldIgnoreOrRejectNewClusterState(logger, currentState, newClusterState)) {
                    return currentState;
                }

                // check to see that we monitor the correct master of the cluster
                if (masterFD.masterNode() == null || !masterFD.masterNode().equals(newClusterState.nodes().getMasterNode())) {
                    masterFD.restart(newClusterState.nodes().getMasterNode(), "new cluster state received and we are monitoring the wrong master [" + masterFD.masterNode() + "]");
                }

                if (currentState.blocks().hasGlobalBlock(discoverySettings.getNoMasterBlock())) {
                    // its a fresh update from the master as we transition from a start of not having a master to having one
                    logger.debug("got first state from fresh master [{}]", newClusterState.nodes().getMasterNodeId());
                    long count = clusterJoinsCounter.incrementAndGet();
                    logger.trace("updated cluster join cluster to [{}]", count);

                    return newClusterState;
                }


                // some optimizations to make sure we keep old objects where possible
                ClusterState.Builder builder = ClusterState.builder(newClusterState);

                // if the routing table did not change, use the original one
                if (newClusterState.routingTable().version() == currentState.routingTable().version()) {
                    builder.routingTable(currentState.routingTable());
                }
                // same for metadata
                if (newClusterState.metaData().version() == currentState.metaData().version()) {
                    builder.metaData(currentState.metaData());
                } else {
                    // if its not the same version, only copy over new indices or ones that changed the version
                    MetaData.Builder metaDataBuilder = MetaData.builder(newClusterState.metaData()).removeAllIndices();
                    for (IndexMetaData indexMetaData : newClusterState.metaData()) {
                        IndexMetaData currentIndexMetaData = currentState.metaData().index(indexMetaData.getIndex());
                        if (currentIndexMetaData != null && currentIndexMetaData.isSameUUID(indexMetaData.getIndexUUID()) &&
                                currentIndexMetaData.getVersion() == indexMetaData.getVersion()) {
                            // safe to reuse
                            metaDataBuilder.put(currentIndexMetaData, false);
                        } else {
                            metaDataBuilder.put(indexMetaData, false);
                        }
                    }
                    builder.metaData(metaDataBuilder);
                }

                return builder.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("unexpected failure during [{}]", e, source);
                if (newClusterState != null) {
                    try {
                        publishClusterState.pendingStatesQueue().markAsFailed(newClusterState, e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.error("unexpected exception while failing [{}]", inner, source);
                    }
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                try {
                    if (newClusterState != null) {
                        publishClusterState.pendingStatesQueue().markAsProcessed(newClusterState);
                    }
                } catch (Exception e) {
                    onFailure(source, e);
                }
            }
        });
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master and
     * the new cluster state version needs to be equal or higher than our cluster state version.
     * If the first condition fails we reject the cluster state and throw an error.
     * If the second condition fails we ignore the cluster state.
     */
    public static boolean shouldIgnoreOrRejectNewClusterState(ESLogger logger, ClusterState currentState, ClusterState newClusterState) {
        validateStateIsFromCurrentMaster(logger, currentState.nodes(), newClusterState);

        // reject cluster states that are not new from the same master
        if (currentState.supersedes(newClusterState) ||
                (newClusterState.nodes().getMasterNodeId().equals(currentState.nodes().getMasterNodeId()) && currentState.version() == newClusterState.version())) {
            // if the new state has a smaller version, and it has the same master node, then no need to process it
            logger.debug("received a cluster state that is not newer than the current one, ignoring (received {}, current {})", newClusterState.version(), currentState.version());
            return true;
        }

        // reject older cluster states if we are following a master
        if (currentState.nodes().getMasterNodeId() != null && newClusterState.version() < currentState.version()) {
            logger.debug("received a cluster state that has a lower version than the current one, ignoring (received {}, current {})", newClusterState.version(), currentState.version());
            return true;
        }
        return false;
    }

    /**
     * In the case we follow an elected master the new cluster state needs to have the same elected master
     * This method checks for this and throws an exception if needed
     */

    public static void validateStateIsFromCurrentMaster(ESLogger logger, DiscoveryNodes currentNodes, ClusterState newClusterState) {
        if (currentNodes.getMasterNodeId() == null) {
            return;
        }
        if (!currentNodes.getMasterNodeId().equals(newClusterState.nodes().getMasterNodeId())) {
            logger.warn("received a cluster state from a different master than the current one, rejecting (received {}, current {})", newClusterState.nodes().getMasterNode(), currentNodes.getMasterNode());
            throw new IllegalStateException("cluster state from a different master than the current one, rejecting (received " + newClusterState.nodes().getMasterNode() + ", current " + currentNodes.getMasterNode() + ")");
        }
    }

    void handleJoinRequest(final DiscoveryNode node, final ClusterState state, final MembershipAction.JoinCallback callback) {
        if (!transportService.addressSupported(node.getAddress().getClass())) {
            // TODO, what should we do now? Maybe inform that node that its crap?
            logger.warn("received a wrong address type from [{}], ignoring...", node);
        } else if (nodeJoinController == null) {
            throw new IllegalStateException("discovery module is not yet started");
        } else {
            // The minimum supported version for a node joining a master:
            Version minimumNodeJoinVersion = localNode().getVersion().minimumCompatibilityVersion();
            // Sanity check: maybe we don't end up here, because serialization may have failed.
            if (node.getVersion().before(minimumNodeJoinVersion)) {
                callback.onFailure(
                        new IllegalStateException("Can't handle join request from a node with a version [" + node.getVersion() + "] that is lower than the minimum compatible version [" + minimumNodeJoinVersion.minimumCompatibilityVersion() + "]")
                );
                return;
            }

            // try and connect to the node, if it fails, we can raise an exception back to the client...
            transportService.connectToNode(node);

            // validate the join request, will throw a failure if it fails, which will get back to the
            // node calling the join request
            try {
                membership.sendValidateJoinRequestBlocking(node, state, joinTimeout);
            } catch (Exception e) {
                logger.warn("failed to validate incoming join request from node [{}]", e, node);
                callback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
                return;
            }
            nodeJoinController.handleJoinRequest(node, callback);
        }
    }

    private DiscoveryNode findMaster() {
        logger.trace("starting to ping");
        ZenPing.PingResponse[] fullPingResponses = pingService.pingAndWait(pingTimeout);
        if (fullPingResponses == null) {
            logger.trace("No full ping responses");
            return null;
        }
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            if (fullPingResponses.length == 0) {
                sb.append(" {none}");
            } else {
                for (ZenPing.PingResponse pingResponse : fullPingResponses) {
                    sb.append("\n\t--> ").append(pingResponse);
                }
            }
            logger.trace("full ping responses:{}", sb);
        }

        // filter responses
        final List<ZenPing.PingResponse> pingResponses = filterPingResponses(fullPingResponses, masterElectionIgnoreNonMasters, logger);

        final DiscoveryNode localNode = clusterService.localNode();
        List<DiscoveryNode> pingMasters = new ArrayList<>();
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            if (pingResponse.master() != null) {
                // We can't include the local node in pingMasters list, otherwise we may up electing ourselves without
                // any check / verifications from other nodes in ZenDiscover#innerJoinCluster()
                if (!localNode.equals(pingResponse.master())) {
                    pingMasters.add(pingResponse.master());
                }
            }
        }

        // nodes discovered during pinging
        Set<DiscoveryNode> activeNodes = new HashSet<>();
        // nodes discovered who has previously been part of the cluster and do not ping for the very first time
        Set<DiscoveryNode> joinedOnceActiveNodes = new HashSet<>();
        if (localNode.isMasterNode()) {
            activeNodes.add(localNode);
            long joinsCounter = clusterJoinsCounter.get();
            if (joinsCounter > 0) {
                logger.trace("adding local node to the list of active nodes that have previously joined the cluster (joins counter is [{}])", joinsCounter);
                joinedOnceActiveNodes.add(localNode);
            }
        }
        for (ZenPing.PingResponse pingResponse : pingResponses) {
            activeNodes.add(pingResponse.node());
            if (pingResponse.hasJoinedOnce()) {
                joinedOnceActiveNodes.add(pingResponse.node());
            }
        }

        if (pingMasters.isEmpty()) {
            if (electMaster.hasEnoughMasterNodes(activeNodes)) {
                // we give preference to nodes who have previously already joined the cluster. Those will
                // have a cluster state in memory, including an up to date routing table (which is not persistent to disk
                // by the gateway)
                DiscoveryNode master = electMaster.electMaster(joinedOnceActiveNodes);
                if (master != null) {
                    return master;
                }
                return electMaster.electMaster(activeNodes);
            } else {
                // if we don't have enough master nodes, we bail, because there are not enough master to elect from
                logger.trace("not enough master nodes [{}]", activeNodes);
                return null;
            }
        } else {

            assert !pingMasters.contains(localNode) : "local node should never be elected as master when other nodes indicate an active master";
            // lets tie break between discovered nodes
            return electMaster.electMaster(pingMasters);
        }
    }

    static List<ZenPing.PingResponse> filterPingResponses(ZenPing.PingResponse[] fullPingResponses, boolean masterElectionIgnoreNonMasters, ESLogger logger) {
        List<ZenPing.PingResponse> pingResponses;
        if (masterElectionIgnoreNonMasters) {
            pingResponses = Arrays.stream(fullPingResponses).filter(ping -> ping.node().isMasterNode()).collect(Collectors.toList());
        } else {
            pingResponses = Arrays.asList(fullPingResponses);
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

    protected ClusterState rejoin(ClusterState clusterState, String reason) {

        // *** called from within an cluster state update task *** //
        assert Thread.currentThread().getName().contains(ClusterService.UPDATE_THREAD_NAME);

        logger.warn("{}, current nodes: {}", reason, clusterState.nodes());
        nodesFD.stop();
        masterFD.stop(reason);


        ClusterBlocks clusterBlocks = ClusterBlocks.builder().blocks(clusterState.blocks())
                .addGlobalBlock(discoverySettings.getNoMasterBlock())
                .build();

        // clean the nodes, we are now not connected to anybody, since we try and reform the cluster
        DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder(clusterState.nodes()).masterNodeId(null).build();

        // TODO: do we want to force a new thread if we actively removed the master? this is to give a full pinging cycle
        // before a decision is made.
        joinThreadControl.startNewThreadIfNotRunning();

        return ClusterState.builder(clusterState)
                .blocks(clusterBlocks)
                .nodes(discoveryNodes)
                .build();
    }

    private boolean localNodeMaster() {
        return nodes().isLocalNodeElectedMaster();
    }

    private ClusterState handleAnotherMaster(ClusterState localClusterState, final DiscoveryNode otherMaster, long otherClusterStateVersion, String reason) {
        assert localClusterState.nodes().isLocalNodeElectedMaster() : "handleAnotherMaster called but current node is not a master";
        assert Thread.currentThread().getName().contains(ClusterService.UPDATE_THREAD_NAME) : "not called from the cluster state update thread";

        if (otherClusterStateVersion > localClusterState.version()) {
            return rejoin(localClusterState, "zen-disco-discovered another master with a new cluster_state [" + otherMaster + "][" + reason + "]");
        } else {
            logger.warn("discovered [{}] which is also master but with an older cluster_state, telling [{}] to rejoin the cluster ([{}])", otherMaster, otherMaster, reason);
            try {
                // make sure we're connected to this node (connect to node does nothing if we're already connected)
                // since the network connections are asymmetric, it may be that we received a state but have disconnected from the node
                // in the past (after a master failure, for example)
                transportService.connectToNode(otherMaster);
                transportService.sendRequest(otherMaster, DISCOVERY_REJOIN_ACTION_NAME, new RejoinClusterRequest(localClusterState.nodes().getLocalNodeId()), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("failed to send rejoin request to [{}]", exp, otherMaster);
                    }
                });
            } catch (Exception e) {
                logger.warn("failed to send rejoin request to [{}]", e, otherMaster);
            }
            return localClusterState;
        }
    }

    private class NewPendingClusterStateListener implements PublishClusterStateAction.NewPendingClusterStateListener {

        @Override
        public void onNewClusterState(String reason) {
            processNextPendingClusterState(reason);
        }
    }

    private class MembershipListener implements MembershipAction.MembershipListener {
        @Override
        public void onJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            handleJoinRequest(node, clusterService.state(), callback);
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
            if (!localNodeMaster()) {
                pingsWhileMaster.set(0);
                return;
            }

            if (pingsWhileMaster.incrementAndGet() < maxPingsFromAnotherMaster) {
                logger.trace("got a ping from another master {}. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
                return;
            }
            logger.debug("got a ping from another master {}. resolving who should rejoin. current ping count: [{}]", pingRequest.masterNode(), pingsWhileMaster.get());
            clusterService.submitStateUpdateTask("ping from another master", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    pingsWhileMaster.set(0);
                    return handleAnotherMaster(currentState, pingRequest.masterNode(), pingRequest.clusterStateVersion(), "node fd ping");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.debug("unexpected error during cluster state update task after pings from another master", e);
                }
            });
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

        public RejoinClusterRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
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
        public void messageReceived(final RejoinClusterRequest request, final TransportChannel channel) throws Exception {
            clusterService.submitStateUpdateTask("received a request to rejoin the cluster from [" + request.fromNodeId + "]", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

                @Override
                public boolean runOnlyOnMaster() {
                    return false;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (Exception e) {
                        logger.warn("failed to send response on rejoin cluster request handling", e);
                    }
                    return rejoin(currentState, "received a request to rejoin the cluster from [" + request.fromNodeId + "]");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("unexpected failure during [{}]", e, source);
                }
            });
        }
    }

    /**
     * All control of the join thread should happen under the cluster state update task thread.
     * This is important to make sure that the background joining process is always in sync with any cluster state updates
     * like master loss, failure to join, received cluster state while joining etc.
     */
    private class JoinThreadControl {

        private final ThreadPool threadPool;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicReference<Thread> currentJoinThread = new AtomicReference<>();

        public JoinThreadControl(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

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
        public ClusterState stopRunningThreadAndRejoin(ClusterState clusterState, String reason) {
            ClusterService.assertClusterStateThread();
            currentJoinThread.set(null);
            return rejoin(clusterState, reason);
        }

        /** starts a new joining thread if there is no currently active one and join thread controlling is started */
        public void startNewThreadIfNotRunning() {
            ClusterService.assertClusterStateThread();
            if (joinThreadActive()) {
                return;
            }
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    Thread currentThread = Thread.currentThread();
                    if (!currentJoinThread.compareAndSet(null, currentThread)) {
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
            ClusterService.assertClusterStateThread();
            if (!markThreadAsDone(joinThread)) {
                return;
            }
            startNewThreadIfNotRunning();
        }

        /** marks the given joinThread as completed. Returns false if the supplied thread is not the currently active join thread */
        public boolean markThreadAsDone(Thread joinThread) {
            ClusterService.assertClusterStateThread();
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
}
