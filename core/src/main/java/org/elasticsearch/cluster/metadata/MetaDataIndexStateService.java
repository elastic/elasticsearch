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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;
    private final NodeServicesProvider nodeServiceProvider;
    private final IndicesService indicesService;

    @Inject
    public MetaDataIndexStateService(Settings settings, ClusterService clusterService, AllocationService allocationService,
                                     MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                     NodeServicesProvider nodeServicesProvider, IndicesService indicesService) {
        super(settings);
        this.nodeServiceProvider = nodeServicesProvider;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
    }

    public void closeIndex(final CloseIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                Set<IndexMetaData> indicesToClose = new HashSet<>();
                for (Index index : request.indices()) {
                    final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
                    if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                        indicesToClose.add(indexMetaData);
                    }
                }

                if (indicesToClose.isEmpty()) {
                    return currentState;
                }

                // Check if index closing conflicts with any running restores
                RestoreService.checkIndexClosing(currentState, indicesToClose);
                // Check if index closing conflicts with any running snapshots
                SnapshotsService.checkIndexClosing(currentState, indicesToClose);
                logger.info("closing indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (IndexMetaData openIndexMetadata : indicesToClose) {
                    final String indexName = openIndexMetadata.getIndex().getName();
                    mdBuilder.put(IndexMetaData.builder(openIndexMetadata).state(IndexMetaData.State.CLOSE));
                    blocksBuilder.addIndexBlock(indexName, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (IndexMetaData index : indicesToClose) {
                    rtBuilder.remove(index.getIndex().getName());
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
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<IndexMetaData> indicesToOpen = new ArrayList<>();
                for (Index index : request.indices()) {
                    final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
                    if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                        indicesToOpen.add(indexMetaData);
                    }
                }

                if (indicesToOpen.isEmpty()) {
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (IndexMetaData closedMetaData : indicesToOpen) {
                    final String indexName = closedMetaData.getIndex().getName();
                    IndexMetaData indexMetaData = IndexMetaData.builder(closedMetaData).state(IndexMetaData.State.OPEN).build();
                    // The index might be closed because we couldn't import it due to old incompatible version
                    // We need to check that this index can be upgraded to the current version
                    indexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData);
                    try {
                        indicesService.verifyIndexMetadata(nodeServiceProvider, indexMetaData);
                    } catch (Exception e) {
                        throw new ElasticsearchException("Failed to verify index " + indexMetaData.getIndex(), e);
                    }

                    mdBuilder.put(indexMetaData, true);
                    blocksBuilder.removeIndexBlock(indexName, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
                for (IndexMetaData index : indicesToOpen) {
                    rtBuilder.addAsFromCloseToOpen(updatedState.metaData().getIndexSafe(index.getIndex()));
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
