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

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService {
    private static final Logger logger = LogManager.getLogger(MetaDataIndexStateService.class);

    public static final int INDEX_CLOSED_BLOCK_ID = 4;
    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false,
        false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final TransportVerifyShardBeforeCloseAction transportVerifyShardBeforeCloseAction;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public MetaDataIndexStateService(ClusterService clusterService, AllocationService allocationService,
                                     MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                     IndicesService indicesService, ThreadPool threadPool,
                                     TransportVerifyShardBeforeCloseAction transportVerifyShardBeforeCloseAction) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.threadPool = threadPool;
        this.transportVerifyShardBeforeCloseAction = transportVerifyShardBeforeCloseAction;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    /**
     * Closes one or more indices.
     *
     * Closing indices is a 3 steps process: it first adds a write block to every indices to close, then waits for the operations on shards
     * to be terminated and finally closes the indices by moving their state to CLOSE.
     */
    public void closeIndices(final CloseIndexClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask("add-block-index-to-close " + Arrays.toString(concreteIndices),
            new ClusterStateUpdateTask(Priority.URGENT) {

                private final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return addIndexClosedBlocks(concreteIndices, blockedIndices, currentState);
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    if (oldState == newState) {
                        assert blockedIndices.isEmpty() : "List of blocked indices is not empty but cluster state wasn't changed";
                        listener.onResponse(new AcknowledgedResponse(true));
                    } else {
                        assert blockedIndices.isEmpty() == false : "List of blocked indices is empty but cluster state was changed";
                        threadPool.executor(ThreadPool.Names.MANAGEMENT)
                            .execute(new WaitForClosedBlocksApplied(blockedIndices, request,
                                ActionListener.wrap(results ->
                                    clusterService.submitStateUpdateTask("close-indices", new ClusterStateUpdateTask(Priority.URGENT) {

                                        boolean acknowledged = true;

                                        @Override
                                        public ClusterState execute(final ClusterState currentState) throws Exception {
                                            final ClusterState updatedState = closeRoutingTable(currentState, blockedIndices, results);
                                            for (Map.Entry<Index, AcknowledgedResponse> result : results.entrySet()) {
                                                IndexMetaData updatedMetaData = updatedState.metaData().index(result.getKey());
                                                if (updatedMetaData != null && updatedMetaData.getState() != IndexMetaData.State.CLOSE) {
                                                    acknowledged = false;
                                                    break;
                                                }
                                            }
                                            return allocationService.reroute(updatedState, "indices closed");
                                        }

                                        @Override
                                        public void onFailure(final String source, final Exception e) {
                                            listener.onFailure(e);
                                        }

                                        @Override
                                        public void clusterStateProcessed(final String source,
                                                                          final ClusterState oldState, final ClusterState newState) {
                                            listener.onResponse(new AcknowledgedResponse(acknowledged));
                                        }
                                    }),
                                    listener::onFailure)
                                )
                            );
                    }
                }

                @Override
                public void onFailure(final String source, final Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }
            }
        );
    }

    /**
     * Step 1 - Start closing indices by adding a write block
     *
     * This step builds the list of indices to close (the ones explicitly requested that are not in CLOSE state) and adds a unique cluster
     * block (or reuses an existing one) to every index to close in the cluster state. After the cluster state is published, the shards
     * should start to reject writing operations and we can proceed with step 2.
     */
    static ClusterState addIndexClosedBlocks(final Index[] indices, final Map<Index, ClusterBlock> blockedIndices,
                                             final ClusterState currentState) {
        final MetaData.Builder metadata = MetaData.builder(currentState.metaData());

        final Set<IndexMetaData> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetaData indexMetaData = metadata.getSafe(index);
            if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                indicesToClose.add(indexMetaData);
            } else {
                logger.debug("index {} is already closed, ignoring", index);
                assert currentState.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
            }
        }

        if (indicesToClose.isEmpty()) {
            return currentState;
        }

        // Check if index closing conflicts with any running restores
        RestoreService.checkIndexClosing(currentState, indicesToClose);
        // Check if index closing conflicts with any running snapshots
        SnapshotsService.checkIndexClosing(currentState, indicesToClose);

        // If the cluster is in a mixed version that does not support the shard close action,
        // we use the previous way to close indices and directly close them without sanity checks
        final boolean useDirectClose = currentState.nodes().getMinNodeVersion().before(Version.V_7_0_0);

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        for (IndexMetaData indexToClose : indicesToClose) {
            final Index index = indexToClose.getIndex();

            ClusterBlock indexBlock = null;
            final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices().get(index.getName());
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == INDEX_CLOSED_BLOCK_ID) {
                        // Reuse the existing index closed block
                        indexBlock = clusterBlock;
                        break;
                    }
                }
            }
            if (useDirectClose) {
                logger.debug("closing index {} directly", index);
                metadata.put(IndexMetaData.builder(indexToClose).state(IndexMetaData.State.CLOSE)); // increment version?
                blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
                routingTable.remove(index.getName());
                indexBlock = INDEX_CLOSED_BLOCK;
            } else {
                if (indexBlock == null) {
                    // Create a new index closed block
                    indexBlock = createIndexClosingBlock();
                }
                assert Strings.hasLength(indexBlock.uuid()) : "Closing block should have a UUID";
            }
            blocks.addIndexBlock(index.getName(), indexBlock);
            blockedIndices.put(index, indexBlock);
        }

        logger.info(() -> new ParameterizedMessage("closing indices {}",
            blockedIndices.keySet().stream().map(Object::toString).collect(Collectors.joining(","))));
        return ClusterState.builder(currentState).blocks(blocks).metaData(metadata).routingTable(routingTable.build()).build();
    }

    /**
     * Step 2 - Wait for indices to be ready for closing
     * <p>
     * This step iterates over the indices previously blocked and sends a {@link TransportVerifyShardBeforeCloseAction} to each shard. If
     * this action succeed then the shard is considered to be ready for closing. When all shards of a given index are ready for closing,
     * the index is considered ready to be closed.
     */
    class WaitForClosedBlocksApplied extends AbstractRunnable {

        private final Map<Index, ClusterBlock> blockedIndices;
        private final CloseIndexClusterStateUpdateRequest request;
        private final ActionListener<Map<Index, AcknowledgedResponse>> listener;

        private WaitForClosedBlocksApplied(final Map<Index, ClusterBlock> blockedIndices,
                                           final CloseIndexClusterStateUpdateRequest request,
                                           final ActionListener<Map<Index, AcknowledgedResponse>> listener) {
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
            this.request = request;
            this.listener = listener;
        }

        @Override
        public void onFailure(final Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, AcknowledgedResponse> results = ConcurrentCollections.newConcurrentMap();
            final CountDown countDown = new CountDown(blockedIndices.size());
            final ClusterState state = clusterService.state();
            blockedIndices.forEach((index, block) -> {
                waitForShardsReadyForClosing(index, block, state, response -> {
                    results.put(index, response);
                    if (countDown.countDown()) {
                        listener.onResponse(unmodifiableMap(results));
                    }
                });
            });
        }

        private void waitForShardsReadyForClosing(final Index index, final ClusterBlock closingBlock,
                                                  final ClusterState state, final Consumer<AcknowledgedResponse> onResponse) {
            final IndexMetaData indexMetaData = state.metaData().index(index);
            if (indexMetaData == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                assert state.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                logger.debug("index {} has been blocked before closing and is already closed, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }

            final ImmutableOpenIntMap<IndexShardRoutingTable> shards = indexRoutingTable.getShards();
            final AtomicArray<AcknowledgedResponse> results = new AtomicArray<>(shards.size());
            final CountDown countDown = new CountDown(shards.size());

            for (IntObjectCursor<IndexShardRoutingTable> shard : shards) {
                final IndexShardRoutingTable shardRoutingTable = shard.value;
                final ShardId shardId = shardRoutingTable.shardId();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, new NotifyOnceListener<ReplicationResponse>() {
                    @Override
                    public void innerOnResponse(final ReplicationResponse replicationResponse) {
                        ReplicationResponse.ShardInfo shardInfo = replicationResponse.getShardInfo();
                        results.setOnce(shardId.id(), new AcknowledgedResponse(shardInfo.getFailed() == 0));
                        processIfFinished();
                    }

                    @Override
                    public void innerOnFailure(final Exception e) {
                        results.setOnce(shardId.id(), new AcknowledgedResponse(false));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            final boolean acknowledged = results.asList().stream().allMatch(AcknowledgedResponse::isAcknowledged);
                            onResponse.accept(new AcknowledgedResponse(acknowledged));
                        }
                    }
                });
            }
        }

        private void sendVerifyShardBeforeCloseRequest(final IndexShardRoutingTable shardRoutingTable,
                                                       final ClusterBlock closingBlock,
                                                       final ActionListener<ReplicationResponse> listener) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), request.taskId());
            final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, closingBlock, parentTaskId);
            if (request.ackTimeout() != null) {
                shardRequest.timeout(request.ackTimeout());
            }
            transportVerifyShardBeforeCloseAction.execute(shardRequest, listener);
        }
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     */
    static ClusterState closeRoutingTable(final ClusterState currentState,
                                          final Map<Index, ClusterBlock> blockedIndices,
                                          final Map<Index, AcknowledgedResponse> results) {
        final MetaData.Builder metadata = MetaData.builder(currentState.metaData());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        final Set<String> closedIndices = new HashSet<>();
        for (Map.Entry<Index, AcknowledgedResponse> result : results.entrySet()) {
            final Index index = result.getKey();
            final boolean acknowledged = result.getValue().isAcknowledged();
            try {
                if (acknowledged == false) {
                    logger.debug("verification of shards before closing {} failed", index);
                    continue;
                }
                final IndexMetaData indexMetaData = metadata.getSafe(index);
                if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                    logger.debug("verification of shards before closing {} succeeded but index is already closed", index);
                    assert currentState.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                    continue;
                }
                final ClusterBlock closingBlock = blockedIndices.get(index);
                if (currentState.blocks().hasIndexBlock(index.getName(), closingBlock) == false) {
                    logger.debug("verification of shards before closing {} succeeded but block has been removed in the meantime", index);
                    continue;
                }

                logger.debug("closing index {} succeeded", index);
                blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID).addIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                metadata.put(IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.CLOSE));
                routingTable.remove(index.getName());
                closedIndices.add(index.getName());
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since it was blocked before closing, ignoring", index);
            }
        }
        logger.info("completed closing of indices {}", closedIndices);
        return ClusterState.builder(currentState).blocks(blocks).metaData(metadata).routingTable(routingTable.build()).build();
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
                public ClusterState execute(final ClusterState currentState) {
                    final ClusterState updatedState = openIndices(request.indices(), currentState);
                    //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                    return allocationService.reroute(updatedState, "indices opened [" + indicesAsString + "]");
                }
            }
        );
    }

    ClusterState openIndices(final Index[] indices, final ClusterState currentState) {
        final List<IndexMetaData> indicesToOpen = new ArrayList<>();
        for (Index index : indices) {
            final IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
            if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                indicesToOpen.add(indexMetaData);
            } else if (currentState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID)) {
                indicesToOpen.add(indexMetaData);
            }
        }

        validateShardLimit(currentState, indices);
        if (indicesToOpen.isEmpty()) {
            return currentState;
        }

        logger.info(() -> new ParameterizedMessage("opening indices [{}]",
            String.join(",", indicesToOpen.stream().map(i -> (CharSequence) i.getIndex().toString())::iterator)));

        final MetaData.Builder metadata = MetaData.builder(currentState.metaData());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();

        for (IndexMetaData indexMetaData : indicesToOpen) {
            final Index index = indexMetaData.getIndex();
            if (indexMetaData.getState() != IndexMetaData.State.OPEN) {
                IndexMetaData updatedIndexMetaData = IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.OPEN).build();
                // The index might be closed because we couldn't import it due to old incompatible version
                // We need to check that this index can be upgraded to the current version
                updatedIndexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(updatedIndexMetaData, minIndexCompatibilityVersion);
                try {
                    indicesService.verifyIndexMetadata(updatedIndexMetaData, updatedIndexMetaData);
                } catch (Exception e) {
                    throw new ElasticsearchException("Failed to verify index " + index, e);
                }
                metadata.put(updatedIndexMetaData, true);
            }

            // Always removes index closed blocks (note: this can fail on-going close index actions)
            blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
        }

        ClusterState updatedState = ClusterState.builder(currentState).metaData(metadata).blocks(blocks).build();

        final RoutingTable.Builder routingTable = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetaData previousIndexMetaData : indicesToOpen) {
            if (previousIndexMetaData.getState() != IndexMetaData.State.OPEN) {
                routingTable.addAsFromCloseToOpen(updatedState.metaData().getIndexSafe(previousIndexMetaData.getIndex()));
            }
        }
        return ClusterState.builder(updatedState).routingTable(routingTable.build()).build();
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

    /**
     * @return Generates a {@link ClusterBlock} that blocks read and write operations on soon-to-be-closed indices. The
     * cluster block is generated with the id value equals to {@link #INDEX_CLOSED_BLOCK_ID} and a unique UUID.
     */
    public static ClusterBlock createIndexClosingBlock() {
        return new ClusterBlock(INDEX_CLOSED_BLOCK_ID, UUIDs.randomBase64UUID(), "index preparing to close. Reopen the index to allow " +
            "writes again or retry closing the index to fully close the index.", false, false, false, RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE));
    }

}
