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

package org.elasticsearch.action.admin.cluster.settings.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.builder;

/**
 *
 */
public class TransportClusterDeleteSettingsAction extends TransportMasterNodeOperationAction<ClusterDeleteSettingsRequest, ClusterDeleteSettingsResponse> {

    private final AllocationService allocationService;

    private final DynamicSettings dynamicSettings;

    @Inject
    public TransportClusterDeleteSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
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
        return ClusterDeleteSettingsAction.NAME;
    }

    @Override
    protected ClusterDeleteSettingsRequest newRequest() {
        return new ClusterDeleteSettingsRequest();
    }

    @Override
    protected ClusterDeleteSettingsResponse newResponse() {
        return new ClusterDeleteSettingsResponse();
    }

    @Override
    protected void masterOperation(final ClusterDeleteSettingsRequest request, final ClusterState state, final ActionListener<ClusterDeleteSettingsResponse> listener) throws ElasticsearchException {
        final ImmutableSettings.Builder transientDeletes = ImmutableSettings.settingsBuilder();
        final ImmutableSettings.Builder persistentDeletes = ImmutableSettings.settingsBuilder();

        clusterService.submitStateUpdateTask("cluster_delete_settings", Priority.URGENT, new AckedClusterStateUpdateTask() {

            private volatile boolean changed = false;

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                if (changed) {
                    reroute(true);
                } else {
                    listener.onResponse(new ClusterDeleteSettingsResponse(true, transientDeletes.build(), persistentDeletes.build()));
                }

            }

            @Override
            public void onAckTimeout() {
                if (changed) {
                    reroute(false);
                } else {
                    listener.onResponse(new ClusterDeleteSettingsResponse(false, transientDeletes.build(), persistentDeletes.build()));
                }
            }

            private void reroute(final boolean deleteSettingsAcked) {
                clusterService.submitStateUpdateTask("reroute_after_cluster_delete_settings", Priority.URGENT, new AckedClusterStateUpdateTask() {

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        //we wait for the reroute ack only if the update settings was acknowledged
                        return deleteSettingsAcked;
                    }

                    @Override
                    public void onAllNodesAcked(@Nullable Throwable t) {
                        //we return when the cluster reroute is acked (the acknowledged flag depends on whether the update settings was acknowledged)
                        listener.onResponse(new ClusterDeleteSettingsResponse(deleteSettingsAcked, transientDeletes.build(), persistentDeletes.build()));
                    }

                    @Override
                    public void onAckTimeout() {
                        //we return when the cluster reroute ack times out (acknowledged false)
                        listener.onResponse(new ClusterDeleteSettingsResponse(false, transientDeletes.build(), persistentDeletes.build()));
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return request.timeout();
                    }

                    @Override
                    public TimeValue timeout() {
                        return request.masterNodeTimeout();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        //if the reroute fails we only log
                        logger.debug("failed to perform [{}]", t, source);
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

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    }
                });
            }

            @Override
            public TimeValue ackTimeout() {
                return request.timeout();
            }

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
                Settings transientSettings;
                Settings existTransientSettings = currentState.metaData().transientSettings();
                if (!request.deleteTransient()) {  // don't need to delete transient settings
                    transientSettings = existTransientSettings;
                } else {
                    transientSettings = ImmutableSettings.settingsBuilder().build();
                    if (!existTransientSettings.getAsMap().isEmpty()) {  // has exist settings, need to set the changed to true
                        changed = true;
                        for (Map.Entry<String, String> entry : existTransientSettings.getAsMap().entrySet()) {
                            transientDeletes.put(entry.getKey(), entry.getValue());  // response with current value
                        }
                    }
                }


                Settings persistentSettings;
                Settings existPersistentSettings = currentState.metaData().persistentSettings();
                if (!request.deletePersistent()) {  // don't need to delete persistent settings
                    persistentSettings = existPersistentSettings;
                } else {
                    persistentSettings = ImmutableSettings.settingsBuilder().build();
                    if (!existPersistentSettings.getAsMap().isEmpty()) {  // has exist settings, need to set the changed to true
                        changed = true;
                        for (Map.Entry<String, String> entry : existPersistentSettings.getAsMap().entrySet()) {
                            persistentDeletes.put(entry.getKey(), entry.getValue());  // response with current value
                        }
                    }
                }



                if (!changed) {
                    return currentState;
                }

                MetaData.Builder metaData = MetaData.builder(currentState.metaData())
                        .persistentSettings(persistentSettings)
                        .transientSettings(transientSettings);

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                boolean updatedReadOnly = metaData.persistentSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false) || metaData.transientSettings().getAsBoolean(MetaData.SETTING_READ_ONLY, false);
                if (updatedReadOnly) {
                    blocks.addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                } else {
                    blocks.removeGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK);
                }

                return builder(currentState).metaData(metaData).blocks(blocks).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}
