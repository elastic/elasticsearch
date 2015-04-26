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

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.builder;

/**
 *
 */
public class TransportClusterUpdateSettingsAction extends TransportMasterNodeOperationAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse> {

    private final AllocationService allocationService;

    private final DynamicSettings dynamicSettings;

    @Inject
    public TransportClusterUpdateSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                AllocationService allocationService, @ClusterDynamicSettings DynamicSettings dynamicSettings, ActionFilters actionFilters) {
        super(settings, ClusterUpdateSettingsAction.NAME, transportService, clusterService, threadPool, actionFilters, ClusterUpdateSettingsRequest.class);
        this.allocationService = allocationService;
        this.dynamicSettings = dynamicSettings;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterUpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        if ((request.transientSettings().getAsMap().isEmpty() && request.persistentSettings().getAsMap().size() == 1 && request.persistentSettings().get(MetaData.SETTING_READ_ONLY) != null) ||
                request.persistentSettings().getAsMap().isEmpty() && request.transientSettings().getAsMap().size() == 1 && request.transientSettings().get(MetaData.SETTING_READ_ONLY) != null) {
            return null;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }


    @Override
    protected ClusterUpdateSettingsResponse newResponse() {
        return new ClusterUpdateSettingsResponse();
    }

    @Override
    protected void masterOperation(final ClusterUpdateSettingsRequest request, final ClusterState state, final ActionListener<ClusterUpdateSettingsResponse> listener) throws ElasticsearchException {
        final ImmutableSettings.Builder transientUpdates = ImmutableSettings.settingsBuilder();
        final ImmutableSettings.Builder persistentUpdates = ImmutableSettings.settingsBuilder();

        clusterService.submitStateUpdateTask("cluster_update_settings", Priority.IMMEDIATE, new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(request, listener) {

            private volatile boolean changed = false;

            @Override
            protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                return new ClusterUpdateSettingsResponse(acknowledged, transientUpdates.build(), persistentUpdates.build());
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                if (changed) {
                    reroute(true);
                } else {
                    super.onAllNodesAcked(t);
                }
            }

            @Override
            public void onAckTimeout() {
                if (changed) {
                    reroute(false);
                } else {
                    super.onAckTimeout();
                }
            }

            private void reroute(final boolean updateSettingsAcked) {
                // We're about to send a second update task, so we need to check if we're still the elected master
                // For example the minimum_master_node could have been breached and we're no longer elected master,
                // so we should *not* execute the reroute.
                if (!clusterService.state().nodes().localNodeMaster()) {
                    logger.debug("Skipping reroute after cluster update settings, because node is no longer master");
                    listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked, transientUpdates.build(), persistentUpdates.build()));
                    return;
                }

                // The reason the reroute needs to be send as separate update task, is that all the *cluster* settings are encapsulate
                // in the components (e.g. FilterAllocationDecider), so the changes made by the first call aren't visible
                // to the components until the ClusterStateListener instances have been invoked, but are visible after
                // the first update task has been completed.
                clusterService.submitStateUpdateTask("reroute_after_cluster_update_settings", Priority.URGENT, new AckedClusterStateUpdateTask<ClusterUpdateSettingsResponse>(request, listener) {

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        //we wait for the reroute ack only if the update settings was acknowledged
                        return updateSettingsAcked;
                    }

                    @Override
                    //we return when the cluster reroute is acked or it times out but the acknowledged flag depends on whether the update settings was acknowledged
                    protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                        return new ClusterUpdateSettingsResponse(updateSettingsAcked && acknowledged, transientUpdates.build(), persistentUpdates.build());
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        logger.debug("failed to preform reroute after cluster settings were updated - current node is no longer a master");
                        listener.onResponse(new ClusterUpdateSettingsResponse(updateSettingsAcked, transientUpdates.build(), persistentUpdates.build()));
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        //if the reroute fails we only log
                        logger.debug("failed to perform [{}]", t, source);
                        listener.onFailure(new ElasticsearchException("reroute after update settings failed", t));
                    }

                    @Override
                    public ClusterState execute(final ClusterState currentState) {
                        // now, reroute in case things that require it changed (e.g. number of replicas)
                        RoutingAllocation.Result routingResult = allocationService.reroute(currentState);
                        if (!routingResult.changed()) {
                            return currentState;
                        }
                        return ClusterState.builder(currentState).routingResult(routingResult).build();
                    }
                });
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to perform [{}]", t, source);
                super.onFailure(source, t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                ImmutableSettings.Builder transientSettings = ImmutableSettings.settingsBuilder();
                transientSettings.put(currentState.metaData().transientSettings());
                for (Map.Entry<String, String> entry : request.transientSettings().getAsMap().entrySet()) {
                    if (dynamicSettings.isDynamicOrLoggingSetting(entry.getKey())) {
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
                    if (dynamicSettings.isDynamicOrLoggingSetting(entry.getKey())) {
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

                MetaData.Builder metaData = MetaData.builder(currentState.metaData())
                        .persistentSettings(persistentSettings.build())
                        .transientSettings(transientSettings.build());

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                boolean updatedReadOnly = metaData.persistentSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false) || metaData.transientSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false);
                if (updatedReadOnly) {
                    blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                } else {
                    blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                }

                return builder(currentState).metaData(metaData).blocks(blocks).build();
            }
        });
    }
}
