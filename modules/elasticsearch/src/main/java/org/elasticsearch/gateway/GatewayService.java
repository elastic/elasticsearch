/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class GatewayService extends AbstractLifecycleComponent<GatewayService> implements ClusterStateListener {

    public static final ClusterBlock STATE_NOT_RECOVERED_BLOCK = new ClusterBlock(1, "state not recovered / initialized", true, true, ClusterBlockLevel.ALL);

    private final Gateway gateway;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final DiscoveryService discoveryService;

    private final MetaDataCreateIndexService createIndexService;

    private final TimeValue initialStateTimeout;
    private final TimeValue recoverAfterTime;
    private final int recoverAfterNodes;
    private final int expectedNodes;
    private final int recoverAfterDataNodes;
    private final int expectedDataNodes;
    private final int recoverAfterMasterNodes;
    private final int expectedMasterNodes;


    private final AtomicBoolean recovered = new AtomicBoolean();
    private final AtomicBoolean scheduledRecovery = new AtomicBoolean();

    @Inject public GatewayService(Settings settings, Gateway gateway, ClusterService clusterService, DiscoveryService discoveryService, MetaDataCreateIndexService createIndexService, ThreadPool threadPool) {
        super(settings);
        this.gateway = gateway;
        this.clusterService = clusterService;
        this.discoveryService = discoveryService;
        this.createIndexService = createIndexService;
        this.threadPool = threadPool;
        this.initialStateTimeout = componentSettings.getAsTime("initial_state_timeout", TimeValue.timeValueSeconds(30));
        // allow to control a delay of when indices will get created
        this.recoverAfterTime = componentSettings.getAsTime("recover_after_time", null);
        this.recoverAfterNodes = componentSettings.getAsInt("recover_after_nodes", -1);
        this.expectedNodes = componentSettings.getAsInt("expected_nodes", -1);
        this.recoverAfterDataNodes = componentSettings.getAsInt("recover_after_data_nodes", -1);
        this.expectedDataNodes = componentSettings.getAsInt("expected_data_nodes", -1);
        this.recoverAfterMasterNodes = componentSettings.getAsInt("recover_after_master_nodes", -1);
        this.expectedMasterNodes = componentSettings.getAsInt("expected_master_nodes", -1);

        // Add the not recovered as initial state block, we don't allow anything until
        this.clusterService.addInitialStateBlock(STATE_NOT_RECOVERED_BLOCK);
    }

    @Override protected void doStart() throws ElasticSearchException {
        gateway.start();
        // if we received initial state, see if we can recover within the start phase, so we hold the
        // node from starting until we recovered properly
        if (discoveryService.initialStateReceived()) {
            ClusterState clusterState = clusterService.state();
            DiscoveryNodes nodes = clusterState.nodes();
            if (clusterState.nodes().localNodeMaster() && clusterState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                if (recoverAfterNodes != -1 && (nodes.masterAndDataNodes().size()) < recoverAfterNodes) {
                    logger.debug("not recovering from gateway, nodes_size (data+master) [" + nodes.masterAndDataNodes().size() + "] < recover_after_nodes [" + recoverAfterNodes + "]");
                } else if (recoverAfterDataNodes != -1 && nodes.dataNodes().size() < recoverAfterDataNodes) {
                    logger.debug("not recovering from gateway, nodes_size (data) [" + nodes.dataNodes().size() + "] < recover_after_data_nodes [" + recoverAfterDataNodes + "]");
                } else if (recoverAfterMasterNodes != -1 && nodes.masterNodes().size() < recoverAfterMasterNodes) {
                    logger.debug("not recovering from gateway, nodes_size (master) [" + nodes.masterNodes().size() + "] < recover_after_master_nodes [" + recoverAfterMasterNodes + "]");
                } else if (recoverAfterTime != null) {
                    logger.debug("not recovering from gateway, recover_after_time [{}]", recoverAfterTime);
                } else {
                    // first update the state that its blocked for not recovered, and then let recovery take its place
                    // that way, we can wait till it is resolved
                    performStateRecovery(initialStateTimeout);
                }
            }
        } else {
            logger.debug("can't wait on start for (possibly) reading state from gateway, will do it asynchronously");
        }
        clusterService.add(this);
    }

    @Override protected void doStop() throws ElasticSearchException {
        clusterService.remove(this);
        gateway.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        gateway.close();
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }
        if (event.localNodeMaster()) {
            if (event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                ClusterState clusterState = event.state();
                DiscoveryNodes nodes = clusterState.nodes();
                if (recoverAfterNodes != -1 && (nodes.masterAndDataNodes().size()) < recoverAfterNodes) {
                    logger.debug("not recovering from gateway, nodes_size (data+master) [" + nodes.masterAndDataNodes().size() + "] < recover_after_nodes [" + recoverAfterNodes + "]");
                } else if (recoverAfterDataNodes != -1 && nodes.dataNodes().size() < recoverAfterDataNodes) {
                    logger.debug("not recovering from gateway, nodes_size (data) [" + nodes.dataNodes().size() + "] < recover_after_data_nodes [" + recoverAfterDataNodes + "]");
                } else if (recoverAfterMasterNodes != -1 && nodes.masterNodes().size() < recoverAfterMasterNodes) {
                    logger.debug("not recovering from gateway, nodes_size (master) [" + nodes.masterNodes().size() + "] < recover_after_master_nodes [" + recoverAfterMasterNodes + "]");
                } else {
                    boolean ignoreTimeout;
                    if (expectedNodes == -1 && expectedMasterNodes == -1 && expectedDataNodes == -1) {
                        // no expected is set, don't ignore the timeout
                        ignoreTimeout = false;
                    } else {
                        // one of the expected is set, see if all of them meet the need, and ignore the timeout in this case
                        ignoreTimeout = true;
                        if (expectedNodes != -1 && (nodes.masterAndDataNodes().size() < expectedNodes)) { // does not meet the expected...
                            ignoreTimeout = false;
                        }
                        if (expectedMasterNodes != -1 && (nodes.masterNodes().size() < expectedMasterNodes)) { // does not meet the expected...
                            ignoreTimeout = false;
                        }
                        if (expectedDataNodes != -1 && (nodes.dataNodes().size() < expectedDataNodes)) { // does not meet the expected...
                            ignoreTimeout = false;
                        }
                    }
                    final boolean fIgnoreTimeout = ignoreTimeout;
                    threadPool.cached().execute(new Runnable() {
                        @Override public void run() {
                            performStateRecovery(null, fIgnoreTimeout);
                        }
                    });
                }
            }
        }
    }

    private void performStateRecovery(@Nullable TimeValue timeout) {
        performStateRecovery(null, false);
    }

    private void performStateRecovery(@Nullable TimeValue timeout, boolean ignoreTimeout) {
        final CountDownLatch latch = new CountDownLatch(1);
        final Gateway.GatewayStateRecoveredListener recoveryListener = new GatewayRecoveryListener(latch);

        if (!ignoreTimeout && recoverAfterTime != null) {
            if (scheduledRecovery.compareAndSet(false, true)) {
                logger.debug("delaying initial state recovery for [{}]", recoverAfterTime);
                threadPool.schedule(new Runnable() {
                    @Override public void run() {
                        if (recovered.compareAndSet(false, true)) {
                            gateway.performStateRecovery(recoveryListener);
                        }
                    }
                }, recoverAfterTime);
            }
        } else {
            if (recovered.compareAndSet(false, true)) {
                gateway.performStateRecovery(recoveryListener);
            }
        }

        if (timeout != null) {
            try {
                latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new ElasticSearchInterruptedException(e.getMessage(), e);
            }
        }
    }

    class GatewayRecoveryListener implements Gateway.GatewayStateRecoveredListener {

        private final CountDownLatch latch;

        GatewayRecoveryListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override public void onSuccess(final ClusterState recoveredState) {
            final AtomicInteger indicesCounter = new AtomicInteger(recoveredState.metaData().indices().size());
            clusterService.submitStateUpdateTask("local-gateway-elected-state", new ProcessedClusterStateUpdateTask() {
                @Override public ClusterState execute(ClusterState currentState) {
                    MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                            .metaData(currentState.metaData());

                    // add the index templates
                    for (Map.Entry<String, IndexTemplateMetaData> entry : recoveredState.metaData().templates().entrySet()) {
                        metaDataBuilder.put(entry.getValue());
                    }

                    return newClusterStateBuilder().state(currentState)
                            .version(recoveredState.version())
                            .metaData(metaDataBuilder).build();
                }

                @Override public void clusterStateProcessed(ClusterState clusterState) {
                    if (recoveredState.metaData().indices().isEmpty()) {
                        markMetaDataAsReadFromGateway("success");
                        latch.countDown();
                        return;
                    }
                    // go over the meta data and create indices, we don't really need to copy over
                    // the meta data per index, since we create the index and it will be added automatically
                    for (final IndexMetaData indexMetaData : recoveredState.metaData()) {
                        try {
                            createIndexService.createIndex(new MetaDataCreateIndexService.Request(MetaDataCreateIndexService.Request.Origin.GATEWAY, "gateway", indexMetaData.index())
                                    .settings(indexMetaData.settings())
                                    .mappingsMetaData(indexMetaData.mappings())
                                    .state(indexMetaData.state())
                                    .timeout(timeValueSeconds(30)),

                                    new MetaDataCreateIndexService.Listener() {
                                        @Override public void onResponse(MetaDataCreateIndexService.Response response) {
                                            if (indicesCounter.decrementAndGet() == 0) {
                                                markMetaDataAsReadFromGateway("success");
                                                latch.countDown();
                                            }
                                        }

                                        @Override public void onFailure(Throwable t) {
                                            logger.error("failed to create index [{}]", t, indexMetaData.index());
                                            // we report success on index creation failure and do nothing
                                            // should we disable writing the updated metadata?
                                            if (indicesCounter.decrementAndGet() == 0) {
                                                markMetaDataAsReadFromGateway("success");
                                                latch.countDown();
                                            }
                                        }
                                    });
                        } catch (IOException e) {
                            logger.error("failed to create index [{}]", e, indexMetaData.index());
                            // we report success on index creation failure and do nothing
                            // should we disable writing the updated metadata?
                            if (indicesCounter.decrementAndGet() == 0) {
                                markMetaDataAsReadFromGateway("success");
                                latch.countDown();
                            }
                        }
                    }
                }
            });
        }

        @Override public void onFailure(Throwable t) {
            // don't remove the block here, we don't want to allow anything in such a case
            logger.error("failed recover state, blocking...", t);
        }
    }

    private void markMetaDataAsReadFromGateway(String reason) {
        clusterService.submitStateUpdateTask("gateway (marked as read, reason=" + reason + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                if (!currentState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                    return currentState;
                }
                // remove the block, since we recovered from gateway
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(STATE_NOT_RECOVERED_BLOCK);

                return newClusterStateBuilder().state(currentState).blocks(blocks).build();
            }
        });
    }
}
