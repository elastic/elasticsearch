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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    @Inject
    public MetaDataIndexStateService(Settings settings, ClusterService clusterService, AllocationService allocationService, MetaDataIndexUpgradeService metaDataIndexUpgradeService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
    }

    public void closeIndex(final CloseIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString, Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToClose = new ArrayList<>();
                for (String index : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexNotFoundException(index);
                    }

                    if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                        IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                        for (IndexShardRoutingTable shard : indexRoutingTable) {
                            for (ShardRouting shardRouting : shard) {
                                if (shardRouting.primary() == true && shardRouting.allocatedPostIndexCreate() == false) {
                                    throw new IndexPrimaryShardNotAllocatedException(new Index(index));
                                }
                            }
                        }
                        indicesToClose.add(index);
                    }
                }

                if (indicesToClose.isEmpty()) {
                    return currentState;
                }

                logger.info("closing indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToClose) {
                    mdBuilder.put(IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.CLOSE));
                    blocksBuilder.addIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (String index : indicesToClose) {
                    rtBuilder.remove(index);
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
                        "indices closed [" + indicesAsString + "]");
                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }
        });
    }

    public void openIndex(final OpenIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString, Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToOpen = new ArrayList<>();
                for (String index : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexNotFoundException(index);
                    }
                    if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                        indicesToOpen.add(index);
                    }
                }

                if (indicesToOpen.isEmpty()) {
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToOpen) {
                    IndexMetaData indexMetaData = IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN).build();
                    // The index might be closed because we couldn't import it due to old incompatible version
                    // We need to check that this index can be upgraded to the current version
                    indexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData);
                    mdBuilder.put(indexMetaData, true);
                    blocksBuilder.removeIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
                for (String index : indicesToOpen) {
                    rtBuilder.addAsFromCloseToOpen(updatedState.metaData().index(index));
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
                        "indices opened [" + indicesAsString + "]");
                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }
        });
    }

}
