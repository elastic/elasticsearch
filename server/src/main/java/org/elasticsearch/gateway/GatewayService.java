/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;

public class GatewayService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(GatewayService.class);

    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.expected_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING = Setting.positiveTimeSetting(
        "gateway.recover_after_time",
        TimeValue.timeValueMillis(0),
        Property.NodeScope
    );
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING = Setting.intSetting(
        "gateway.recover_after_data_nodes",
        -1,
        -1,
        Property.NodeScope
    );

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(
        1,
        "state not recovered / initialized",
        true,
        true,
        false,
        RestStatus.SERVICE_UNAVAILABLE,
        ClusterBlockLevel.ALL
    );

    static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;
    private final ThreadPool threadPool;

    private final RerouteService rerouteService;
    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final AtomicReference<PendingStateRecovery> currentPendingStateRecoveryRef = new AtomicReference<>();

    @Inject
    public GatewayService(
        final Settings settings,
        final RerouteService rerouteService,
        final ClusterService clusterService,
        final ShardRoutingRoleStrategy shardRoutingRoleStrategy,
        final ThreadPool threadPool
    ) {
        this.rerouteService = rerouteService;
        this.clusterService = clusterService;
        this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
        this.threadPool = threadPool;
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(settings);
        } else if (expectedDataNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            // use post applied so that the state will be visible to the background recovery thread we spawn in performStateRecovery
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }

        final ClusterState state = event.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster() == false) {
            if (nodes.getMasterNodeId() == null) {
                logger.debug("not recovering from gateway, no master elected yet");
                return;
            }
            // not our job to recover
            return;
        }
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }

        // At this point, we know the state is not recovered and this node is qualified for state recovery
        // But we still need to check whether a previous one is running already
        final long currentTerm = state.term();
        final PendingStateRecovery existingPendingStateRecovery = currentPendingStateRecoveryRef.get();

        // Always start a new state recovery if the master term changes
        // If there is a previous one still waiting, both will probably run but at most one of them will
        // actually make changes to cluster state because either:
        // 1. The previous recovers the cluster state and the current one will be skipped
        // 2. The previous one sees a new cluster term and skips its own execution
        if (existingPendingStateRecovery == null || existingPendingStateRecovery.term < currentTerm) {
            final PendingStateRecovery newPendingStateRecovery = new PendingStateRecovery(currentTerm);
            if (currentPendingStateRecoveryRef.compareAndSet(existingPendingStateRecovery, newPendingStateRecovery)) {
                newPendingStateRecovery.start(nodes.getDataNodes().size());
            } else {
                assert false : "compareAndSet should have succeeded since the cluster state processing is single threaded";
            }
        } else {
            logger.debug("state recovery is in progress for term [{}]", existingPendingStateRecovery.term);
        }
    }

    class PendingStateRecovery {
        private final long term;

        PendingStateRecovery(long term) {
            this.term = term;
        }

        void start(int dataNodeSize) {
            if (recoverAfterTime == null) {
                logger.debug("performing state recovery of term [{}], no delay time is configured", term);
                runRecoveryImmediately();
            } else if (expectedDataNodes != -1 && expectedDataNodes <= dataNodeSize) {
                logger.debug("performing state recovery of term [{}], expected data nodes [{}] is reached", term, expectedDataNodes);
                runRecoveryImmediately();
            } else {
                logger.info(
                    "delaying initial state recovery for [{}] of term [{}]. expecting [{}] data nodes, but only have [{}]",
                    recoverAfterTime,
                    term,
                    expectedDataNodes,
                    dataNodeSize
                );
                threadPool.schedule(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("delayed state recovery of term [" + term + "] failed", e);
                        resetState();
                    }

                    @Override
                    protected void doRun() {
                        submitUnbatchedTask(TASK_SOURCE, new RecoverStateUpdateTask(term, PendingStateRecovery.this::resetState));
                    }
                }, recoverAfterTime, threadPool.generic());
            }
        }

        void runRecoveryImmediately() {
            try {
                submitUnbatchedTask(TASK_SOURCE, new RecoverStateUpdateTask(term, this::resetState));
            } catch (Exception e) {
                logger.warn("state recovery of term [" + term + "] failed", e);
                resetState();
            }
        }

        void resetState() {
            GatewayService.this.currentPendingStateRecoveryRef.compareAndSet(this, null);
        }
    }

    private static final String TASK_SOURCE = "local-gateway-elected-state";

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        private final long expectedTerm;
        private final Runnable runAfter;

        RecoverStateUpdateTask(long expectedTerm, Runnable runAfter) {
            this.expectedTerm = expectedTerm;
            this.runAfter = runAfter;
        }

        @Override
        public ClusterState execute(final ClusterState currentState) {
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                logger.debug("cluster is already recovered");
                return currentState;
            }
            if (expectedTerm != currentState.term()) {
                logger.debug("skip state recovery since current term [{}] != expected term [{}]", currentState.term(), expectedTerm);
                return currentState;
            }
            if (recoverAfterDataNodes != -1 && currentState.nodes().getDataNodes().size() < recoverAfterDataNodes) {
                logger.debug(
                    "not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                    currentState.nodes().getDataNodes().size(),
                    recoverAfterDataNodes
                );
                return currentState;
            }
            return ClusterStateUpdaters.removeStateNotRecoveredBlock(
                ClusterStateUpdaters.updateRoutingTable(currentState, shardRoutingRoleStrategy)
            );
        }

        @Override
        public void clusterStateProcessed(final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metadata().indices().size());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            runAfter.run();
            rerouteService.reroute("state recovered", Priority.NORMAL, ActionListener.noop());
        }

        @Override
        public void onFailure(final Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.INFO,
                () -> "unexpected failure during [" + TASK_SOURCE + "]",
                e
            );
            runAfter.run();
        }
    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
