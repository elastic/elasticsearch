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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
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

        final TimeValue timeout = request.ackTimeout();
        final TimeValue masterTimeout = request.masterNodeTimeout();

        clusterService.submitStateUpdateTask("add-block-index-to-close " + Arrays.toString(concreteIndices),
            new ClusterStateUpdateTask(Priority.URGENT) {

                private final Set<Index> blockedIndices = new HashSet<>();

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return addIndexClosedBlocks(concreteIndices, currentState, blockedIndices);
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    if (oldState == newState) {
                        assert blockedIndices.isEmpty() : "List of blocked indices is not empty but cluster state wasn't changed";
                        listener.onResponse(new AcknowledgedResponse(true));
                    } else {
                        assert blockedIndices.isEmpty() == false : "List of blocked indices is empty but cluster state was changed";
                        threadPool.executor(ThreadPool.Names.MANAGEMENT)
                            .execute(new WaitForClosedBlocksApplied(blockedIndices, timeout,
                                ActionListener.wrap(closedBlocksResults ->
                                    clusterService.submitStateUpdateTask("close-indices", new ClusterStateUpdateTask(Priority.URGENT) {
                                        @Override
                                        public ClusterState execute(final ClusterState currentState) throws Exception {
                                            final ClusterState updatedState = closeRoutingTable(currentState, closedBlocksResults);
                                            return allocationService.reroute(updatedState, "indices closed");
                                        }

                                        @Override
                                        public void onFailure(final String source, final Exception e) {
                                            listener.onFailure(e);
                                        }

                                        @Override
                                        public void clusterStateProcessed(final String source,
                                                                          final ClusterState oldState, final ClusterState newState) {
                                            boolean acknowledged = closedBlocksResults.values().stream()
                                                .allMatch(AcknowledgedResponse::isAcknowledged);
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
                    return masterTimeout;
                }
            }
        );
    }

    /**
     * Step 1 - Start closing indices by adding a write block
     *
     * This step builds the list of indices to close (the ones explicitly requested that are not in CLOSE state) and adds the index block
     * {@link #INDEX_CLOSED_BLOCK} to every index to close in the cluster state. After the cluster state is published, the shards should
     * start to reject writing operations and we can proceed with step 2.
     */
    static ClusterState addIndexClosedBlocks(final Index[] indices, final ClusterState currentState, final Set<Index> blockedIndices) {
        final MetaData.Builder metadata = MetaData.builder(currentState.metaData());

        final Set<IndexMetaData> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetaData indexMetaData = metadata.getSafe(index);
            if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                indicesToClose.add(indexMetaData);
            } else {
                logger.debug("index {} is already closed, ignoring", index);
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
            if (currentState.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK) == false) {
                blocks.addIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
            }
            if (useDirectClose) {
                logger.debug("closing index {} directly", index);
                metadata.put(IndexMetaData.builder(indexToClose).state(IndexMetaData.State.CLOSE));
                routingTable.remove(index.getName());
            }
            blockedIndices.add(index);
        }

        logger.info(() -> new ParameterizedMessage("closing indices {}",
            blockedIndices.stream().map(Object::toString).collect(Collectors.joining(","))));
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

        private final Set<Index> blockedIndices;
        private final @Nullable TimeValue timeout;
        private final ActionListener<Map<Index, AcknowledgedResponse>> listener;

        private WaitForClosedBlocksApplied(final Set<Index> blockedIndices,
                                           final @Nullable TimeValue timeout,
                                           final ActionListener<Map<Index, AcknowledgedResponse>> listener) {
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed block to be applied to null or empty list of blocked indices");
            }
            this.blockedIndices = blockedIndices;
            this.listener = listener;
            this.timeout = timeout;
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
            for (Index blockedIndex : blockedIndices) {
                waitForShardsReadyForClosing(blockedIndex, state, timeout, response -> {
                    results.put(blockedIndex, response);
                    if (countDown.countDown()) {
                        listener.onResponse(unmodifiableMap(results));
                    }
                });
            }
        }

        private void waitForShardsReadyForClosing(final Index index, final ClusterState state, @Nullable final TimeValue timeout,
                                                  final Consumer<AcknowledgedResponse> onResponse) {
            final IndexMetaData indexMetaData = state.metaData().index(index);
            if (indexMetaData == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new AcknowledgedResponse(true));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetaData.getState() == IndexMetaData.State.CLOSE) {
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
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, timeout, new NotifyOnceListener<ReplicationResponse>() {
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

        private void sendVerifyShardBeforeCloseRequest(final IndexShardRoutingTable shardRoutingTable, @Nullable final TimeValue timeout,
                                                       final ActionListener<ReplicationResponse> listener) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId);
            if (timeout != null) {
                shardRequest.timeout(timeout);
            }
            // TODO propagate a task id from the parent CloseIndexRequest to the ShardCloseRequests
            transportVerifyShardBeforeCloseAction.execute(shardRequest, listener);
        }
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     */
    static ClusterState closeRoutingTable(final ClusterState currentState, final Map<Index, AcknowledgedResponse> results) {
        final MetaData.Builder metadata = MetaData.builder(currentState.metaData());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        final Set<String> closedIndices = new HashSet<>();
        for (Map.Entry<Index, AcknowledgedResponse> result : results.entrySet()) {
            final Index index = result.getKey();
            try {
                final IndexMetaData indexMetaData = metadata.getSafe(index);
                if (indexMetaData.getState() != IndexMetaData.State.CLOSE) {
                    if (result.getValue().isAcknowledged()) {
                        logger.debug("closing index {} succeed, removing index routing table", index);
                        metadata.put(IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.CLOSE));
                        routingTable.remove(index.getName());
                        closedIndices.add(index.getName());
                    } else {
                        logger.debug("closing index {} failed, removing index block because: {}", index, result.getValue());
                        blocks.removeIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                    }
                } else {
                    logger.debug("index {} has been closed since it was blocked before closing, ignoring", index);
                }
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
