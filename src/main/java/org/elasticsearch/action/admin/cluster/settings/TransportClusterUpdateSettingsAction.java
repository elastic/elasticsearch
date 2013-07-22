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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 *
 */
public class TransportClusterUpdateSettingsAction extends TransportMasterNodeOperationAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private final AllocationService allocationService;

    private final DynamicSettings dynamicSettings;

    @Inject
    public TransportClusterUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                AllocationService allocationService, @ClusterDynamicSettings DynamicSettings dynamicSettings) {
        super(settings, transportService, clusterService, threadPool);
        this.allocationService = allocationService;
        this.dynamicSettings = dynamicSettings;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
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
    protected void masterOperation(final ClusterUpdateSettingsRequest request, final ClusterState state, final ActionListener<ClusterUpdateSettingsResponse> listener) throws ElasticSearchException {
        final ImmutableSettings.Builder transientUpdates = ImmutableSettings.settingsBuilder();
        final ImmutableSettings.Builder persistentUpdates = ImmutableSettings.settingsBuilder();

        clusterService.submitStateUpdateTask("cluster_update_settings", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to perform [{}]", t, source);
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                boolean changed = false;
                ImmutableSettings.Builder transientSettings = ImmutableSettings.settingsBuilder();
                transientSettings.put(currentState.metaData().transientSettings());
                for (Map.Entry<String, String> entry : request.transientSettings().getAsMap().entrySet()) {
                    if (dynamicSettings.hasDynamicSetting(entry.getKey()) || entry.getKey().startsWith("logger.")) {
                        String error = dynamicSettings.validateDynamicSetting(entry.getKey(), entry.getValue());
                        if (error == null) {
                            transientSettings.put(entry.getKey(), entry.getValue());
                            transientUpdates.put(entry.getKey(), entry.getValue());
                            changed = true;
                        } else {
                            logger.warn("ignoring transient setting [{}], [{}]", entry.getKey(), error);
                        }
                    } else {
                        logger.warn("ignoring transient setting [{}], not dynamically updateable", entry.getKey());
                    }
                }

                ImmutableSettings.Builder persistentSettings = ImmutableSettings.settingsBuilder();
                persistentSettings.put(currentState.metaData().persistentSettings());
                for (Map.Entry<String, String> entry : request.persistentSettings().getAsMap().entrySet()) {
                    if (dynamicSettings.hasDynamicSetting(entry.getKey()) || entry.getKey().startsWith("logger.")) {
                        String error = dynamicSettings.validateDynamicSetting(entry.getKey(), entry.getValue());
                        if (error == null) {
                            persistentSettings.put(entry.getKey(), entry.getValue());
                            persistentUpdates.put(entry.getKey(), entry.getValue());
                            changed = true;
                        } else {
                            logger.warn("ignoring persistent setting [{}], [{}]", entry.getKey(), error);
                        }
                    } else {
                        logger.warn("ignoring persistent setting [{}], not dynamically updateable", entry.getKey());
                    }
                }

                if (!changed) {
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
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState == newState) {
                    // nothing changed...
                    return;
                }
                // now, reroute
                clusterService.submitStateUpdateTask("reroute_after_cluster_update_settings", Priority.URGENT, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        try {
                            // now, reroute in case things change that require it (like number of replicas)
                            RoutingAllocation.Result routingResult = allocationService.reroute(currentState);
                            if (!routingResult.changed()) {
                                return currentState;
                            }
                            return newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
                        } finally {
                            listener.onResponse(new ClusterUpdateSettingsResponse(transientUpdates.build(), persistentUpdates.build()));
                        }
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.warn("unexpected failure during [{}]", t, source);
                        listener.onResponse(new ClusterUpdateSettingsResponse(transientUpdates.build(), persistentUpdates.build()));
                    }
                });
            }
        });
    }
}
