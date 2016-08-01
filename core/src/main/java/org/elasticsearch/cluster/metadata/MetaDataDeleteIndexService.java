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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
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
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetaDataDeleteIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void deleteIndices(final DeleteIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask("delete-index " + request.indices(),
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                final MetaData meta = currentState.metaData();
                final Index[] indices = request.indices();
                final Set<IndexMetaData> metaDatas = Arrays.asList(indices).stream().map(i -> meta.getIndexSafe(i)).collect(Collectors.toSet());
                // Check if index deletion conflicts with any running snapshots
                SnapshotsService.checkIndexDeletion(currentState, metaDatas);
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                MetaData.Builder metaDataBuilder = MetaData.builder(meta);
                ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

                final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metaDataBuilder.indexGraveyard());
                final int previousGraveyardSize = graveyardBuilder.tombstones().size();
                for (final Index index : indices) {
                    String indexName = index.getName();
                    logger.debug("[{}] deleting index", index);
                    routingTableBuilder.remove(indexName);
                    clusterBlocksBuilder.removeIndexBlocks(indexName);
                    metaDataBuilder.remove(indexName);
                }
                // add tombstones to the cluster state for each deleted index
                final IndexGraveyard currentGraveyard = graveyardBuilder.addTombstones(indices).build(settings);
                metaDataBuilder.indexGraveyard(currentGraveyard); // the new graveyard set on the metadata
                logger.trace("{} tombstones purged from the cluster state. Previous tombstone size: {}. Current tombstone size: {}.",
                    graveyardBuilder.getNumPurged(), previousGraveyardSize, currentGraveyard.getTombstones().size());

                MetaData newMetaData = metaDataBuilder.build();
                ClusterBlocks blocks = clusterBlocksBuilder.build();
                RoutingAllocation.Result routingResult = allocationService.reroute(
                        ClusterState.builder(currentState).routingTable(routingTableBuilder.build()).metaData(newMetaData).build(),
                        "deleted indices [" + indices + "]");
                return ClusterState.builder(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
            }
        });
    }
}
