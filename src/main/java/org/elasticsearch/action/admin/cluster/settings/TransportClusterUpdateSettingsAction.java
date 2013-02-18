/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 *
 */
public class TransportClusterUpdateSettingsAction extends TransportMasterNodeOperationAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private final AllocationService allocationService;

    @Inject
    public TransportClusterUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                AllocationService allocationService) {
        super(settings, transportService, clusterService, threadPool);
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return ClusterUpdateSettingsAction.NAME;
    }

    @Override
    protected ClusterUpdateSettingsRequest newRequest() {
        return new ClusterUpdateSettingsRequest();
    }

    @Override
    protected ClusterUpdateSettingsResponse newResponse() {
        return new ClusterUpdateSettingsResponse();
    }

    @Override
    protected ClusterUpdateSettingsResponse masterOperation(final ClusterUpdateSettingsRequest request, ClusterState state) throws ElasticSearchException {
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);

        clusterService.submitStateUpdateTask("cluster_update_settings", Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    boolean changed = false;
                    ImmutableSettings.Builder transientSettings = ImmutableSettings.settingsBuilder();
                    transientSettings.put(currentState.metaData().transientSettings());
                    for (Map.Entry<String, String> entry : request.getTransientSettings().getAsMap().entrySet()) {
                        if (MetaData.hasDynamicSetting(entry.getKey()) || entry.getKey().startsWith("logger.")) {
                            transientSettings.put(entry.getKey(), entry.getValue());
                            changed = true;
                        } else {
                            logger.warn("ignoring transient setting [{}], not dynamically updateable", entry.getKey());
                        }
                    }

                    ImmutableSettings.Builder persistentSettings = ImmutableSettings.settingsBuilder();
                    persistentSettings.put(currentState.metaData().persistentSettings());
                    for (Map.Entry<String, String> entry : request.getPersistentSettings().getAsMap().entrySet()) {
                        if (MetaData.hasDynamicSetting(entry.getKey()) || entry.getKey().startsWith("logger.")) {
                            changed = true;
                            persistentSettings.put(entry.getKey(), entry.getValue());
                        } else {
                            logger.warn("ignoring persistent setting [{}], not dynamically updateable", entry.getKey());
                        }
                    }

                    if (!changed) {
                        latch.countDown();
                        return currentState;
                    }

                    MetaData.Builder metaData = MetaData.builder().metaData(currentState.metaData())
                            .persistentSettings(persistentSettings.build())
                            .transientSettings(transientSettings.build());

                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    boolean updatedReadOnly = metaData.persistentSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false) || metaData.transientSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false);
                    if (updatedReadOnly) {
                        blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                    } else {
                        blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                    }

                    return ClusterState.builder().state(currentState).metaData(metaData).blocks(blocks).build();
                } catch (Exception e) {
                    latch.countDown();
                    logger.warn("failed to update cluster settings", e);
                    return currentState;
                } finally {
                    // we don't release the latch here, only after we rerouted
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                // now, reroute
                clusterService.submitStateUpdateTask("reroute_after_cluster_update_settings", Priority.URGENT, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        try {
                            // now, reroute in case things change that require it (like number of replicas)
                            RoutingAllocation.Result routingResult = allocationService.reroute(currentState);
                            return newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            failureRef.set(e);
        }

        if (failureRef.get() != null) {
            if (failureRef.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failureRef.get();
            } else {
                throw new ElasticSearchException(failureRef.get().getMessage(), failureRef.get());
            }
        }

        return new ClusterUpdateSettingsResponse();
    }
}
