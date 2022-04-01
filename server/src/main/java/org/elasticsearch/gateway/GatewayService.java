/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
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

    private final ThreadPool threadPool;

    private final RerouteService rerouteService;
    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;

    private final AtomicBoolean recoveryInProgress = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(
        final Settings settings,
        final RerouteService rerouteService,
        final ClusterService clusterService,
        final ThreadPool threadPool
    ) {
        this.rerouteService = rerouteService;
        this.clusterService = clusterService;
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

        if (state.nodes().isLocalNodeElectedMaster() == false) {
            // not our job to recover
            return;
        }
        if (state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            // already recovered
            return;
        }

        final DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getMasterNodeId() == null) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug(
                "not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(),
                recoverAfterDataNodes
            );
        } else {
            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedDataNodes == -1) {
                // no expected is set, honor recover_after_data_nodes
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else if (expectedDataNodes <= nodes.getDataNodes().size()) {
                // expected is set and satisfied so recover immediately
                enforceRecoverAfterTime = false;
                reason = "";
            } else {
                // expected is set but not satisfied so wait until it is satisfied or times out
                enforceRecoverAfterTime = true;
                reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(final boolean enforceRecoverAfterTime, final String reason) {
        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("delayed state recovery failed", e);
                        resetRecoveredFlags();
                    }

                    @Override
                    protected void doRun() {
                        if (recoveryInProgress.compareAndSet(false, true)) {
                            logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                            runRecovery();
                        }
                    }
                }, recoverAfterTime, ThreadPool.Names.GENERIC);
            }
        } else {
            if (recoveryInProgress.compareAndSet(false, true)) {
                try {
                    logger.debug("performing state recovery...");
                    runRecovery();
                } catch (Exception e) {
                    logger.warn("state recovery failed", e);
                    resetRecoveredFlags();
                }
            }
        }
    }

    private void resetRecoveredFlags() {
        recoveryInProgress.set(false);
        scheduledRecovery.set(false);
    }

    private static final String TASK_SOURCE = "local-gateway-elected-state";

    class RecoverStateUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(final ClusterState currentState) {
            if (currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                logger.debug("cluster is already recovered");
                return currentState;
            }
            return ClusterStateUpdaters.removeStateNotRecoveredBlock(ClusterStateUpdaters.updateRoutingTable(currentState));
        }

        @Override
        public void clusterStateProcessed(final ClusterState oldState, final ClusterState newState) {
            logger.info("recovered [{}] indices into cluster_state", newState.metadata().indices().size());
            // reset flag even though state recovery completed, to ensure that if we subsequently become leader again based on a
            // not-recovered state, that we again do another state recovery.
            rerouteService.reroute("state recovered", Priority.NORMAL, ActionListener.wrap(GatewayService.this::resetRecoveredFlags));
        }

        @Override
        public void onNoLongerMaster() {
            logger.debug("stepped down as master before recovering state [{}]", TASK_SOURCE);
            resetRecoveredFlags();
        }

        @Override
        public void onFailure(final Exception e) {
            logger.info(() -> new ParameterizedMessage("unexpected failure during [{}]", TASK_SOURCE), e);
            resetRecoveredFlags();
        }
    }

    // used for testing
    TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

    private void runRecovery() {
        clusterService.submitStateUpdateTask(TASK_SOURCE, new RecoverStateUpdateTask(), newExecutor());
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }
}
