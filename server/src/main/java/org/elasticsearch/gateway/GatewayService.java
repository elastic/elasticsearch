/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

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
    volatile PendingStateRecovery currentPendingStateRecovery;

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
        final PendingStateRecovery existingPendingStateRecovery = currentPendingStateRecovery;

        // Always start a new state recovery if the master term changes
        // If there is a previous one still waiting, both will probably run but at most one of them will
        // actually make changes to cluster state because either:
        // 1. The previous recovers the cluster state and the current one will be skipped
        // 2. The previous one sees a new cluster term and skips its own execution
        if (existingPendingStateRecovery == null || existingPendingStateRecovery.expectedTerm < currentTerm) {
            currentPendingStateRecovery = new PendingStateRecovery(currentTerm);
        }
        currentPendingStateRecovery.onDataNodeSize(nodes.getDataNodes().size());
    }

    /**
     * This class manages the cluster state recovery behaviours. It has two major scenarios depending
     * on whether {@code recoverAfterDataNodes} is configured.
     *
     * <p> <b>When</b> {@code recoverAfterDataNodes} is configured:
     * <ol>
     *     <li>Nothing can happen until it is reached
     *     <li>When {@code recoverAfterDataNodes} is reached, the cluster either:
     *     <ul>
     *         <li>Recover immediately when {@code expectedDataNodes} is reached or
     *         both {@code expectedDataNodes} and {@code recoverAfterTime} are not configured
     *         <li>Or schedule a recovery with a delay of {@code recoverAfterTime}
     *     </ul>
     *     <li>The scheduled recovery can be cancelled if {@code recoverAfterDataNodes} drops below required number
     *     before the recovery can happen. When this happens, the process goes back to the beginning (step 1).
     *     <li>The recovery is scheduled only once each time {@code recoverAfterDataNodes} crosses the required number
     * </ol>
     *
     * <p> <b>When</b> {@code recoverAfterDataNodes} is <b>Not</b> configured, the cluster either:
     * <ul>
     *     <li>Recover immediately when {@code expectedDataNodes} is reached or
     *     both {@code expectedDataNodes} and {@code recoverAfterTime} are not configured
     *     <li>Or schedule a recovery with a delay of {@code recoverAfterTime}
     * </ul>
     */
    class PendingStateRecovery {
        private final long expectedTerm;
        @Nullable
        private Scheduler.ScheduledCancellable scheduledRecovery;
        private final AtomicBoolean taskSubmitted = new AtomicBoolean();

        PendingStateRecovery(long expectedTerm) {
            this.expectedTerm = expectedTerm;
        }

        void onDataNodeSize(int currentDataNodeSize) {
            if (recoverAfterDataNodes != -1 && currentDataNodeSize < recoverAfterDataNodes) {
                logger.debug(
                    "not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                    currentDataNodeSize,
                    recoverAfterDataNodes
                );
                cancelScheduledRecovery();
            } else {
                maybePerformOrScheduleRecovery(currentDataNodeSize);
            }
        }

        void maybePerformOrScheduleRecovery(int currentDataNodeSize) {
            if (expectedDataNodes != -1 && expectedDataNodes <= currentDataNodeSize) {
                logger.debug(
                    "performing state recovery of term [{}], expected data nodes [{}] is reached",
                    expectedTerm,
                    expectedDataNodes
                );
                cancelScheduledRecovery();
                runRecoveryImmediately();
            } else if (recoverAfterTime == null) {
                logger.debug("performing state recovery of term [{}], no delay time is configured", expectedTerm);
                cancelScheduledRecovery();
                runRecoveryImmediately();
            } else {
                if (scheduledRecovery == null) {
                    logger.info(
                        "delaying initial state recovery for [{}] of term [{}]. expecting [{}] data nodes, but only have [{}]",
                        recoverAfterTime,
                        expectedTerm,
                        expectedDataNodes,
                        currentDataNodeSize
                    );
                    scheduledRecovery = threadPool.schedule(new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("delayed state recovery of term [" + expectedTerm + "] failed", e);
                        }

                        @Override
                        protected void doRun() {
                            final PendingStateRecovery existingPendingStateRecovery = currentPendingStateRecovery;
                            if (PendingStateRecovery.this == existingPendingStateRecovery) {
                                runRecoveryImmediately();
                            } else {
                                logger.debug(
                                    "skip scheduled state recovery since a new one of term [{}] has started",
                                    existingPendingStateRecovery.expectedTerm
                                );
                            }
                        }
                    }, recoverAfterTime, threadPool.generic());
                } else {
                    logger.debug("state recovery is in already scheduled for term [{}]", expectedTerm);
                }
            }
        }

        void runRecoveryImmediately() {
            if (taskSubmitted.compareAndSet(false, true)) {
                submitUnbatchedTask(TASK_SOURCE, new RecoverStateUpdateTask(expectedTerm));
            } else {
                logger.debug("state recovery task is already submitted");
            }
        }

        void cancelScheduledRecovery() {
            if (scheduledRecovery != null) {
                scheduledRecovery.cancel();
                scheduledRecovery = null;
            }
        }
    }

    private static final String TASK_SOURCE = "local-gateway-elected-state";

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        private final long expectedTerm;

        RecoverStateUpdateTask(long expectedTerm) {
            this.expectedTerm = expectedTerm;
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
            return ClusterStateUpdaters.removeStateNotRecoveredBlock(
                ClusterStateUpdaters.updateRoutingTable(currentState, shardRoutingRoleStrategy)
            );
        }

        @Override
        public void clusterStateProcessed(final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metadata().getTotalNumberOfIndices());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            rerouteService.reroute("state recovered", Priority.NORMAL, ActionListener.noop());
        }

        @Override
        public void onFailure(final Exception e) {
            logger.log(
                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.INFO,
                () -> "unexpected failure during [" + TASK_SOURCE + "]",
                e
            );
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
