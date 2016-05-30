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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GatewayService extends AbstractLifecycleComponent<GatewayService> implements ClusterStateListener {

    public static final Setting<Integer> EXPECTED_NODES_SETTING =
        Setting.intSetting("gateway.expected_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_DATA_NODES_SETTING =
        Setting.intSetting("gateway.expected_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> EXPECTED_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.expected_master_nodes", -1, -1, Property.NodeScope);
    public static final Setting<TimeValue> RECOVER_AFTER_TIME_SETTING =
        Setting.positiveTimeSetting("gateway.recover_after_time", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_DATA_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_data_nodes", -1, -1, Property.NodeScope);
    public static final Setting<Integer> RECOVER_AFTER_MASTER_NODES_SETTING =
        Setting.intSetting("gateway.recover_after_master_nodes", 0, 0, Property.NodeScope);

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    public static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final Gateway gateway;

    private final ThreadPool threadPool;

    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterMasterNodes;
    private final int expectedMasterNodes;


    private final AtomicBoolean recovered = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject
    public GatewayService(Settings settings, AllocationService allocationService, ClusterService clusterService,
                          ThreadPool threadPool, NodeEnvironment nodeEnvironment, GatewayMetaState metaState,
                          TransportNodesListGatewayMetaState listGatewayMetaState, Discovery discovery,
                          NodeServicesProvider nodeServicesProvider, IndicesService indicesService) {
        super(settings);
        this.gateway = new Gateway(settings, clusterService, nodeEnvironment, metaState, listGatewayMetaState, discovery,
            nodeServicesProvider, indicesService);
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = EXPECTED_NODES_SETTING.get(this.settings);
        this.expectedDataNodes = EXPECTED_DATA_NODES_SETTING.get(this.settings);
        this.expectedMasterNodes = EXPECTED_MASTER_NODES_SETTING.get(this.settings);

        if (RECOVER_AFTER_TIME_SETTING.exists(this.settings)) {
            recoverAfterTime = RECOVER_AFTER_TIME_SETTING.get(this.settings);
        } else if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            recoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        } else {
            recoverAfterTime = null;
        }
        this.recoverAfterNodes = RECOVER_AFTER_NODES_SETTING.get(this.settings);
        this.recoverAfterDataNodes = RECOVER_AFTER_DATA_NODES_SETTING.get(this.settings);
        // default the recover after master nodes to the minimum master nodes in the discovery
        if (RECOVER_AFTER_MASTER_NODES_SETTING.exists(this.settings)) {
            recoverAfterMasterNodes = RECOVER_AFTER_MASTER_NODES_SETTING.get(this.settings);
        } else {
            // TODO: change me once the minimum_master_nodes is changed too
            recoverAfterMasterNodes = settings.getAsInt("discovery.zen.minimum_master_nodes", -1);
        }

        // Add the not recovered as initial state block, we don't allow anything until
        this.clusterService.addInitialStateBlock(STATE_NOT_RECOVERED_BLOCK);
    }

    @Override
    protected void doStart() {
        clusterService.addLast(this);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() {
    }

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

        DiscoveryNodes nodes = state.nodes();
        if (state.nodes().getMasterNodeId() == null) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.getMasterAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug("not recovering from gateway, nodes_size (data+master) [{}] < recover_after_nodes [{}]",
                nodes.getMasterAndDataNodes().size(), recoverAfterNodes);
        } else if (recoverAfterDataNodes != -1 && nodes.getDataNodes().size() < recoverAfterDataNodes) {
            logger.debug("not recovering from gateway, nodes_size (data) [{}] < recover_after_data_nodes [{}]",
                nodes.getDataNodes().size(), recoverAfterDataNodes);
        } else if (recoverAfterMasterNodes != -1 && nodes.getMasterNodes().size() < recoverAfterMasterNodes) {
            logger.debug("not recovering from gateway, nodes_size (master) [{}] < recover_after_master_nodes [{}]",
                nodes.getMasterNodes().size(), recoverAfterMasterNodes);
        } else {
            boolean enforceRecoverAfterTime;
            String reason;
            if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
                // no expected is set, honor the setting if they are there
                enforceRecoverAfterTime = true;
                reason = "recover_after_time was set to [" + recoverAfterTime + "]";
            } else {
                // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                enforceRecoverAfterTime = false;
                reason = "";
                if (expectedNodes != -1 && (nodes.getMasterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedNodes + "] nodes, but only have [" + nodes.getMasterAndDataNodes().size() + "]";
                } else if (expectedDataNodes != -1 && (nodes.getDataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.getDataNodes().size() + "]";
                } else if (expectedMasterNodes != -1 && (nodes.getMasterNodes().size() < expectedMasterNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.getMasterNodes().size() + "]";
                }
            }
            performStateRecovery(enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(boolean enforceRecoverAfterTime, String reason) {
        final Gateway.GatewayStateRecoveredListener recoveryListener = new GatewayRecoveryListener();

        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(recoverAfterTime, ThreadPool.Names.GENERIC, () -> {
                    if (recovered.compareAndSet(false, true)) {
                        logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                        gateway.performStateRecovery(recoveryListener);
                    }
                });
            }
        } else {
            if (recovered.compareAndSet(false, true)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Throwable t) {
                        logger.warn("Recovery failed", t);
                        // we reset `recovered` in the listener don't reset it here otherwise there might be a race
                        // that resets it to false while a new recover is already running?
                        recoveryListener.onFailure("state recovery failed: " + t.getMessage());
                    }

                    @Override
                    protected void doRun() throws Exception {
                        gateway.performStateRecovery(recoveryListener);
                    }
                });
            }
        }
    }

    public Gateway getGateway() {
        return gateway;
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assert currentState.metaData().indices().isEmpty();

                    // remove the block, since we recovered from gateway
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder()
                            .blocks(currentState.blocks())
                            .blocks(recoveredState.blocks())
                            .removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK);

                    MetaData.Builder metaDataBuilder = MetaData.builder(recoveredState.metaData());
                    // automatically generate a UID for the metadata if we need to
                    metaDataBuilder.generateClusterUuidIfNeeded();

                    if (MetaData.SETTING_READ_ONLY_SETTING.get(recoveredState.metaData().settings()) || MetaData.SETTING_READ_ONLY_SETTING.get(currentState.metaData().settings())) {
                        blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                    }

                    for (IndexMetaData indexMetaData : recoveredState.metaData()) {
                        metaDataBuilder.put(indexMetaData, false);
                        blocks.addBlocks(indexMetaData);
                    }

                    // update the state to reflect the new metadata and routing
                    ClusterState updatedState = ClusterState.builder(currentState)
                            .blocks(blocks)
                            .metaData(metaDataBuilder)
                            .build();

                    // initialize all index routing tables as empty
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(updatedState.routingTable());
                    for (ObjectCursor<IndexMetaData> cursor : updatedState.metaData().indices().values()) {
                        routingTableBuilder.addAsRecovery(cursor.value);
                    }
                    // start with 0 based versions for routing table
                    routingTableBuilder.version(0);

                    // now, reroute
                    RoutingAllocation.Result routingResult = allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(routingTableBuilder.build()).build(),
                            "state recovered");

                    return ClusterState.builder(updatedState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                    GatewayRecoveryListener.this.onFailure("failed to updated cluster state");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("recovered [{}] indices into cluster_state", newState.metaData().indices().size());
                }
            });
        }

        @Override
        public void onFailure(String message) {
            recovered.set(false);
            scheduledRecovery.set(false);
            // don't remove the block here, we don't want to allow anything in such a case
            logger.info("metadata state not restored, reason: {}", message);
        }

    }

    // used for testing
    public TimeValue recoverAfterTime() {
        return recoverAfterTime;
    }

}
