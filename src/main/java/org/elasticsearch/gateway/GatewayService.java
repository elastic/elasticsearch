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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GatewayService extends AbstractLifecycleComponent<GatewayService> implements ClusterStateListener {

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    public static final TimeValue DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET = TimeValue.timeValueMinutes(5);

    private final Gateway gateway;

    private final ThreadPool threadPool;

    private final AllocationService allocationService;

    private final ClusterService clusterService;

    private final DiscoveryService discoveryService;

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
    public GatewayService(Settings settings, Gateway gateway, AllocationService allocationService, ClusterService clusterService, DiscoveryService discoveryService, ThreadPool threadPool) {
        super(settings);
        this.gateway = gateway;
        this.allocationService = allocationService;
        this.clusterService = clusterService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;
        // allow to control a delay of when indices will get created
        this.expectedNodes = this.settings.getAsInt("gateway.expected_nodes", -1);
        this.expectedDataNodes = this.settings.getAsInt("gateway.expected_data_nodes", -1);
        this.expectedMasterNodes = this.settings.getAsInt("gateway.expected_master_nodes", -1);

        TimeValue defaultRecoverAfterTime = null;
        if (expectedNodes >= 0 || expectedDataNodes >= 0 || expectedMasterNodes >= 0) {
            defaultRecoverAfterTime = DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET;
        }

        this.recoverAfterTime = this.settings.getAsTime("gateway.recover_after_time", defaultRecoverAfterTime);
        this.recoverAfterNodes = this.settings.getAsInt("gateway.recover_after_nodes", -1);
        this.recoverAfterDataNodes = this.settings.getAsInt("gateway.recover_after_data_nodes", -1);
        // default the recover after master nodes to the minimum master nodes in the discovery
        this.recoverAfterMasterNodes = this.settings.getAsInt("gateway.recover_after_master_nodes", settings.getAsInt("discovery.zen.minimum_master_nodes", -1));

        // Add the not recovered as initial state block, we don't allow anything until
        this.clusterService.addInitialStateBlock(STATE_NOT_RECOVERED_BLOCK);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService.addLast(this);
        // if we received initial state, see if we can recover within the start phase, so we hold the
        // node from starting until we recovered properly
        if (discoveryService.initialStateReceived()) {
            ClusterState clusterState = clusterService.state();
            if (clusterState.nodes().localNodeMaster() && clusterState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                checkStateMeetsSettingsAndMaybeRecover(clusterState, false);
            }
        } else {
            logger.debug("can't wait on start for (possibly) reading state from gateway, will do it asynchronously");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.stoppedOrClosed()) {
            return;
        }
        if (event.localNodeMaster() && event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
            checkStateMeetsSettingsAndMaybeRecover(event.state(), true);
        }
    }

    protected void checkStateMeetsSettingsAndMaybeRecover(ClusterState state, boolean asyncRecovery) {
        DiscoveryNodes nodes = state.nodes();
        if (state.blocks().hasGlobalBlock(discoveryService.getNoMasterBlock())) {
            logger.debug("not recovering from gateway, no master elected yet");
        } else if (recoverAfterNodes != -1 && (nodes.masterAndDataNodes().size()) < recoverAfterNodes) {
            logger.debug("not recovering from gateway, nodes_size (data+master) [" + nodes.masterAndDataNodes().size() + "] < recover_after_nodes [" + recoverAfterNodes + "]");
        } else if (recoverAfterDataNodes != -1 && nodes.dataNodes().size() < recoverAfterDataNodes) {
            logger.debug("not recovering from gateway, nodes_size (data) [" + nodes.dataNodes().size() + "] < recover_after_data_nodes [" + recoverAfterDataNodes + "]");
        } else if (recoverAfterMasterNodes != -1 && nodes.masterNodes().size() < recoverAfterMasterNodes) {
            logger.debug("not recovering from gateway, nodes_size (master) [" + nodes.masterNodes().size() + "] < recover_after_master_nodes [" + recoverAfterMasterNodes + "]");
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
                if (expectedNodes != -1 && (nodes.masterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedNodes + "] nodes, but only have [" + nodes.masterAndDataNodes().size() + "]";
                } else if (expectedDataNodes != -1 && (nodes.dataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedDataNodes + "] data nodes, but only have [" + nodes.dataNodes().size() + "]";
                } else if (expectedMasterNodes != -1 && (nodes.masterNodes().size() < expectedMasterNodes)) { // does not meet the expected...
                    enforceRecoverAfterTime = true;
                    reason = "expecting [" + expectedMasterNodes + "] master nodes, but only have [" + nodes.masterNodes().size() + "]";
                }
            }
            performStateRecovery(asyncRecovery, enforceRecoverAfterTime, reason);
        }
    }

    private void performStateRecovery(boolean asyncRecovery, boolean enforceRecoverAfterTime, String reason) {
        final Gateway.GatewayStateRecoveredListener recoveryListener = new GatewayRecoveryListener(new CountDownLatch(1));

        if (enforceRecoverAfterTime && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.info("delaying initial state recovery for [{}]. {}", recoverAfterTime, reason);
                threadPool.schedule(recoverAfterTime, ThreadPool.Names.GENERIC, new Runnable() {
                    @Override
                    public void run() {
                        if (recovered.compareAndSet(false, true)) {
                            logger.info("recover_after_time [{}] elapsed. performing state recovery...", recoverAfterTime);
                            gateway.performStateRecovery(recoveryListener);
                        }
                    }
                });
            }
        } else {
            if (recovered.compareAndSet(false, true)) {
                if (asyncRecovery) {
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            gateway.performStateRecovery(recoveryListener);
                        }
                    });
                } else {
                    logger.trace("performing state recovery...");
                    gateway.performStateRecovery(recoveryListener);
                }
            }
        }
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        private final CountDownLatch latch;

        GatewayRecoveryListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onSuccess(final ClusterState recoveredState) {
            logger.trace("successful state recovery, importing cluster state...");
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new ProcessedClusterStateUpdateTask() {
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
                    metaDataBuilder.generateUuidIfNeeded();

                    if (recoveredState.metaData().settings().getAsBoolean(MetaData.SETTING_READ_ONLY, false) || currentState.metaData().settings().getAsBoolean(MetaData.SETTING_READ_ONLY, false)) {
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
                    RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(routingTableBuilder).build());

                    return ClusterState.builder(updatedState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("recovered [{}] indices into cluster_state", newState.metaData().indices().size());
                    latch.countDown();
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
