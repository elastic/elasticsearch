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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService {
    private static final Logger logger = LogManager.getLogger(MetaDataIndexStateService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false,
        false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;
    private final IndicesService indicesService;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public MetaDataIndexStateService(ClusterService clusterService, AllocationService allocationService,
                                     MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                     IndicesService indicesService, ThreadPool threadPool) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    public void closeIndices(final CloseIndexClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString,
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                return closeIndices(currentState, request.indices(), indicesAsString);
            }
        });
    }

    public ClusterState closeIndices(ClusterState currentState, final Index[] indices, String indicesAsString) {
        Set<IndexMetaData> indicesToClose = new HashSet<>();
        for (Index index : indices) {
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

        //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
        return  allocationService.reroute(
            ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
            "indices closed [" + indicesAsString + "]");
    }

    public void openIndex(final OpenIndexClusterStateUpdateRequest request,
                          final ActionListener<OpenIndexClusterStateUpdateResponse> listener) {
        onlyOpenIndex(request, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                String[] indexNames = Arrays.stream(request.indices()).map(Index::getName).toArray(String[]::new);
                activeShardsObserver.waitForActiveShards(indexNames, request.waitForActiveShards(), request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug("[{}] indices opened, but the operation timed out while waiting for " +
                                "enough shards to be started.", Arrays.toString(indexNames));
                        }
                        listener.onResponse(new OpenIndexClusterStateUpdateResponse(response.isAcknowledged(), shardsAcknowledged));
                    }, listener::onFailure);
            } else {
                listener.onResponse(new OpenIndexClusterStateUpdateResponse(false, false));
            }
        }, listener::onFailure));
    }

    private void onlyOpenIndex(final OpenIndexClusterStateUpdateRequest request,
                               final ActionListener<ClusterStateUpdateResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices());
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString,
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {
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

                validateShardLimit(currentState, request.indices());

                if (indicesToOpen.isEmpty()) {
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                    .minimumIndexCompatibilityVersion();
                for (IndexMetaData closedMetaData : indicesToOpen) {
                    final String indexName = closedMetaData.getIndex().getName();
                    IndexMetaData indexMetaData = IndexMetaData.builder(closedMetaData).state(IndexMetaData.State.OPEN).build();
                    // The index might be closed because we couldn't import it due to old incompatible version
                    // We need to check that this index can be upgraded to the current version
                    indexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData, minIndexCompatibilityVersion);
                    try {
                        indicesService.verifyIndexMetadata(indexMetaData, indexMetaData);
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

                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return allocationService.reroute(
                        ClusterState.builder(updatedState).routingTable(rtBuilder.build()).build(),
                        "indices opened [" + indicesAsString + "]");
            }
        });
    }

    /**
     * Validates whether a list of indices can be opened without going over the cluster shard limit.  Only counts indices which are
     * currently closed and will be opened, ignores indices which are already open.
     *
     * @param currentState The current cluster state.
     * @param indices The indices which are to be opened.
     * @throws ValidationException If this operation would take the cluster over the limit and enforcement is enabled.
     */
    static void validateShardLimit(ClusterState currentState, Index[] indices) {
        int shardsToOpen = Arrays.stream(indices)
            .filter(index -> currentState.metaData().index(index).getState().equals(IndexMetaData.State.CLOSE))
            .mapToInt(index -> getTotalShardCount(currentState, index))
            .sum();

        Optional<String> error = IndicesService.checkShardLimit(shardsToOpen, currentState);
        if (error.isPresent()) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(error.get());
            throw ex;
        }

    }

    private static int getTotalShardCount(ClusterState state, Index index) {
        IndexMetaData indexMetaData = state.metaData().index(index);
        return indexMetaData.getNumberOfShards() * (1 + indexMetaData.getNumberOfReplicas());
    }

}
