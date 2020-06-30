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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.NotifyOnceListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.IndexResult;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse.ShardResult;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse.AddBlockResult;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse.AddBlockShardResult;
import org.elasticsearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;

/**
 * Service responsible for submitting open/close index requests as well as for adding index blocks
 */
public class MetadataIndexStateService {
    private static final Logger logger = LogManager.getLogger(MetadataIndexStateService.class);

    public static final int INDEX_CLOSED_BLOCK_ID = 4;
    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false,
        false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);
    public static final Setting<Boolean> VERIFIED_BEFORE_CLOSE_SETTING =
        Setting.boolSetting("index.verified_before_close", false, Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final MetadataIndexUpgradeService metadataIndexUpgradeService;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final ThreadPool threadPool;
    private final NodeClient client;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public MetadataIndexStateService(ClusterService clusterService, AllocationService allocationService,
                                     MetadataIndexUpgradeService metadataIndexUpgradeService,
                                     IndicesService indicesService, ShardLimitValidator shardLimitValidator,
                                     NodeClient client, ThreadPool threadPool) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.threadPool = threadPool;
        this.client = client;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.shardLimitValidator = shardLimitValidator;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    /**
     * Closes one or more indices.
     *
     * Closing indices is a 3 steps process: it first adds a write block to every indices to close, then waits for the operations on shards
     * to be terminated and finally closes the indices by moving their state to CLOSE.
     */
    public void closeIndices(final CloseIndexClusterStateUpdateRequest request, final ActionListener<CloseIndexResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }
        List<String> writeIndices = new ArrayList<>();
        SortedMap<String, IndexAbstraction> lookup = clusterService.state().metadata().getIndicesLookup();
        for (Index index : concreteIndices) {
            IndexAbstraction ia = lookup.get(index.getName());
            if (ia != null && ia.getParentDataStream() != null && ia.getParentDataStream().getWriteIndex().getIndex().equals(index)) {
                writeIndices.add(index.getName());
            }
        }
        if (writeIndices.size() > 0) {
            throw new IllegalArgumentException("cannot close the following data stream write indices [" +
                Strings.collectionToCommaDelimitedString(writeIndices) + "]");
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
                        listener.onResponse(new CloseIndexResponse(true, false, Collections.emptyList()));
                    } else {
                        assert blockedIndices.isEmpty() == false : "List of blocked indices is empty but cluster state was changed";
                        threadPool.executor(ThreadPool.Names.MANAGEMENT)
                            .execute(new WaitForClosedBlocksApplied(blockedIndices, request,
                                ActionListener.wrap(verifyResults ->
                                    clusterService.submitStateUpdateTask("close-indices", new ClusterStateUpdateTask(Priority.URGENT) {
                                        private final List<IndexResult> indices = new ArrayList<>();

                                        @Override
                                        public ClusterState execute(final ClusterState currentState) throws Exception {
                                            Tuple<ClusterState, Collection<IndexResult>> closingResult =
                                                closeRoutingTable(currentState, blockedIndices, verifyResults);
                                            assert verifyResults.size() == closingResult.v2().size();
                                            indices.addAll(closingResult.v2());
                                            return allocationService.reroute(closingResult.v1(), "indices closed");
                                        }

                                        @Override
                                        public void onFailure(final String source, final Exception e) {
                                            listener.onFailure(e);
                                        }

                                        @Override
                                        public void clusterStateProcessed(final String source,
                                                                          final ClusterState oldState, final ClusterState newState) {

                                            final boolean acknowledged = indices.stream().noneMatch(IndexResult::hasFailures);
                                            final String[] waitForIndices = indices.stream()
                                                .filter(result -> result.hasFailures() == false)
                                                .filter(result -> newState.routingTable().hasIndex(result.getIndex()))
                                                .map(result -> result.getIndex().getName())
                                                .toArray(String[]::new);

                                            if (waitForIndices.length > 0) {
                                                activeShardsObserver.waitForActiveShards(waitForIndices, request.waitForActiveShards(),
                                                    request.ackTimeout(), shardsAcknowledged -> {
                                                        if (shardsAcknowledged == false) {
                                                            logger.debug("[{}] indices closed, but the operation timed out while waiting " +
                                                                "for enough shards to be started.", Arrays.toString(waitForIndices));
                                                        }
                                                        // acknowledged maybe be false but some indices may have been correctly closed, so
                                                        // we maintain a kind of coherency by overriding the shardsAcknowledged value
                                                        // (see ShardsAcknowledgedResponse constructor)
                                                        boolean shardsAcked = acknowledged ? shardsAcknowledged : false;
                                                        listener.onResponse(new CloseIndexResponse(acknowledged, shardsAcked, indices));
                                                    }, listener::onFailure);
                                            } else {
                                                listener.onResponse(new CloseIndexResponse(acknowledged, false, indices));
                                            }
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
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        final Set<Index> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = metadata.getSafe(index);
            if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                indicesToClose.add(index);
            } else {
                logger.debug("index {} is already closed, ignoring", index);
                assert currentState.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
            }
        }

        if (indicesToClose.isEmpty()) {
            return currentState;
        }

        // Check if index closing conflicts with any running restores
        Set<Index> restoringIndices = RestoreService.restoringIndices(currentState, indicesToClose);
        if (restoringIndices.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot close indices that are being restored: " + restoringIndices);
        }

        // Check if index closing conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, indicesToClose);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException("Cannot close indices that are being snapshotted: " + snapshottingIndices +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        for (Index index : indicesToClose) {
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
            if (indexBlock == null) {
                // Create a new index closed block
                indexBlock = createIndexClosingBlock();
            }
            assert Strings.hasLength(indexBlock.uuid()) : "Closing block should have a UUID";
            blocks.addIndexBlock(index.getName(), indexBlock);
            blockedIndices.put(index, indexBlock);
        }

        logger.info(() -> new ParameterizedMessage("closing indices {}",
            blockedIndices.keySet().stream().map(Object::toString).collect(Collectors.joining(","))));
        return ClusterState.builder(currentState).blocks(blocks).metadata(metadata).routingTable(routingTable.build()).build();
    }

    /**
     * Updates the cluster state for the given indices with the given index block,
     * and also returns the updated indices (and their blocks) in a map.
     * @param indices The indices to add blocks to if needed
     * @param currentState The current cluster state
     * @param block The type of block to add
     * @return a tuple of the updated cluster state, as well as the blocks that got added
     */
    static Tuple<ClusterState, Map<Index, ClusterBlock>> addIndexBlock(final Index[] indices, final ClusterState currentState,
                                                                       final APIBlock block) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());

        final Set<Index> indicesToAddBlock = new HashSet<>();
        for (Index index : indices) {
            metadata.getSafe(index); // to check if index exists
            if (currentState.blocks().hasIndexBlock(index.getName(), block.block)) {
                logger.debug("index {} already has block {}, ignoring", index, block.block);
            } else {
                indicesToAddBlock.add(index);
            }
        }

        if (indicesToAddBlock.isEmpty()) {
            return Tuple.tuple(currentState, Collections.emptyMap());
        }

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());
        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        for (Index index : indicesToAddBlock) {
            ClusterBlock indexBlock = null;
            final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices().get(index.getName());
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == block.block.id()) {
                        // Reuse the existing UUID-based block
                        indexBlock = clusterBlock;
                        break;
                    }
                }
            }
            if (indexBlock == null) {
                // Create a new UUID-based block
                indexBlock = createUUIDBasedBlock(block.block);
            }
            assert Strings.hasLength(indexBlock.uuid()) : "Block should have a UUID";
            blocks.addIndexBlock(index.getName(), indexBlock);
            blockedIndices.put(index, indexBlock);
            // update index settings as well to match the block
            final IndexMetadata indexMetadata = metadata.getSafe(index);
            if (block.setting().get(indexMetadata.getSettings()) == false) {
                final Settings updatedSettings = Settings.builder()
                    .put(indexMetadata.getSettings()).put(block.settingName(), true).build();

                metadata.put(IndexMetadata.builder(indexMetadata)
                    .settings(updatedSettings)
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1));
            }
        }

        logger.info("adding block {} to indices {}", block.name,
            blockedIndices.keySet().stream().map(Object::toString).collect(Collectors.toList()));
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).metadata(metadata)
                .routingTable(routingTable.build()).build(), blockedIndices);
    }

    /**
     * Adds an index block based on the given request, and notifies the listener upon completion.
     * Adding blocks is done in three steps:
     * - First, a temporary UUID-based block is added to the index
     *   (see {@link #addIndexBlock(Index[], ClusterState, APIBlock)}.
     * - Second, shards are checked to have properly applied the UUID-based block.
     *   (see {@link WaitForBlocksApplied}).
     * - Third, the temporary UUID-based block is turned into a full block
     *   (see {@link #finalizeBlock(ClusterState, Map, Map, APIBlock)}.
     * Using this three-step process ensures non-interference by other operations in case where
     * we notify successful completion here.
     */
    public void addIndexBlock(AddIndexBlockClusterStateUpdateRequest request,
                              ActionListener<AddIndexBlockResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }
        List<String> writeIndices = new ArrayList<>();
        SortedMap<String, IndexAbstraction> lookup = clusterService.state().metadata().getIndicesLookup();
        for (Index index : concreteIndices) {
            IndexAbstraction ia = lookup.get(index.getName());
            if (ia != null && ia.getParentDataStream() != null && ia.getParentDataStream().getWriteIndex().getIndex().equals(index)) {
                writeIndices.add(index.getName());
            }
        }
        if (writeIndices.size() > 0) {
            throw new IllegalArgumentException("cannot add a block to the following data stream write indices [" +
                Strings.collectionToCommaDelimitedString(writeIndices) + "]");
        }

        clusterService.submitStateUpdateTask("add-index-block-[" + request.getBlock().name + "]-" + Arrays.toString(concreteIndices),
            new ClusterStateUpdateTask(Priority.URGENT) {

                private Map<Index, ClusterBlock> blockedIndices;

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    final Tuple<ClusterState, Map<Index, ClusterBlock>> tup =
                        addIndexBlock(concreteIndices, currentState, request.getBlock());
                    blockedIndices = tup.v2();
                    return tup.v1();
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    if (oldState == newState) {
                        assert blockedIndices.isEmpty() : "List of blocked indices is not empty but cluster state wasn't changed";
                        listener.onResponse(new AddIndexBlockResponse(true, false, Collections.emptyList()));
                    } else {
                        assert blockedIndices.isEmpty() == false : "List of blocked indices is empty but cluster state was changed";
                        threadPool.executor(ThreadPool.Names.MANAGEMENT)
                            .execute(new WaitForBlocksApplied(blockedIndices, request,
                                    ActionListener.wrap(verifyResults ->
                                            clusterService.submitStateUpdateTask("finalize-index-block-[" + request.getBlock().name +
                                                    "]-[" + blockedIndices.keySet().stream().map(Index::getName)
                                                        .collect(Collectors.joining(", ")) + "]",
                                                new ClusterStateUpdateTask(Priority.URGENT) {
                                                private final List<AddBlockResult> indices = new ArrayList<>();

                                                @Override
                                                public ClusterState execute(final ClusterState currentState) throws Exception {
                                                    Tuple<ClusterState, Collection<AddBlockResult>> addBlockResult =
                                                        finalizeBlock(currentState, blockedIndices, verifyResults, request.getBlock());
                                                    assert verifyResults.size() == addBlockResult.v2().size();
                                                    indices.addAll(addBlockResult.v2());
                                                    return addBlockResult.v1();
                                                }

                                                @Override
                                                public void onFailure(final String source, final Exception e) {
                                                    listener.onFailure(e);
                                                }

                                                @Override
                                                public void clusterStateProcessed(final String source,
                                                                                  final ClusterState oldState,
                                                                                  final ClusterState newState) {

                                                    final boolean acknowledged = indices.stream().noneMatch(
                                                        AddBlockResult::hasFailures);
                                                    listener.onResponse(new AddIndexBlockResponse(acknowledged, acknowledged, indices));
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
     * Step 2 - Wait for indices to be ready for closing
     * <p>
     * This step iterates over the indices previously blocked and sends a {@link TransportVerifyShardBeforeCloseAction} to each shard. If
     * this action succeed then the shard is considered to be ready for closing. When all shards of a given index are ready for closing,
     * the index is considered ready to be closed.
     */
    class WaitForClosedBlocksApplied extends ActionRunnable<Map<Index, IndexResult>> {

        private final Map<Index, ClusterBlock> blockedIndices;
        private final CloseIndexClusterStateUpdateRequest request;

        private WaitForClosedBlocksApplied(final Map<Index, ClusterBlock> blockedIndices,
                                           final CloseIndexClusterStateUpdateRequest request,
                                           final ActionListener<Map<Index, IndexResult>> listener) {
                super(listener);
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for closed blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
            this.request = request;
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, IndexResult> results = ConcurrentCollections.newConcurrentMap();
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

        private void waitForShardsReadyForClosing(final Index index,
                                                  final ClusterBlock closingBlock,
                                                  final ClusterState state,
                                                  final Consumer<IndexResult> onResponse) {
            final IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                assert state.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                logger.debug("index {} has been blocked before closing and is already closed, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }

            final ImmutableOpenIntMap<IndexShardRoutingTable> shards = indexRoutingTable.getShards();
            final AtomicArray<ShardResult> results = new AtomicArray<>(shards.size());
            final CountDown countDown = new CountDown(shards.size());

            for (IntObjectCursor<IndexShardRoutingTable> shard : shards) {
                final IndexShardRoutingTable shardRoutingTable = shard.value;
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, new NotifyOnceListener<ReplicationResponse>() {
                    @Override
                    public void innerOnResponse(final ReplicationResponse replicationResponse) {
                        ShardResult.Failure[] failures = Arrays.stream(replicationResponse.getShardInfo().getFailures())
                            .map(f -> new ShardResult.Failure(f.index(), f.shardId(), f.getCause(), f.nodeId()))
                            .toArray(ShardResult.Failure[]::new);
                        results.setOnce(shardId, new ShardResult(shardId, failures));
                        processIfFinished();
                    }

                    @Override
                    public void innerOnFailure(final Exception e) {
                        ShardResult.Failure failure = new ShardResult.Failure(index.getName(), shardId, e);
                        results.setOnce(shardId, new ShardResult(shardId, new ShardResult.Failure[]{failure}));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            onResponse.accept(new IndexResult(index, results.toArray(new ShardResult[results.length()])));
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
                new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, closingBlock, true, parentTaskId);
            if (request.ackTimeout() != null) {
                shardRequest.timeout(request.ackTimeout());
            }
            client.executeLocally(TransportVerifyShardBeforeCloseAction.TYPE, shardRequest, new ActionListener<>() {
                @Override
                public void onResponse(ReplicationResponse replicationResponse) {
                    final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest =
                        new TransportVerifyShardBeforeCloseAction.ShardRequest(shardId, closingBlock, false, parentTaskId);
                    if (request.ackTimeout() != null) {
                        shardRequest.timeout(request.ackTimeout());
                    }
                    client.executeLocally(TransportVerifyShardBeforeCloseAction.TYPE, shardRequest, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }

    /**
     * Helper class that coordinates with shards to ensure that blocks have been properly applied to all shards using
     * {@link TransportVerifyShardIndexBlockAction}.
     */
    class WaitForBlocksApplied extends ActionRunnable<Map<Index, AddBlockResult>> {

        private final Map<Index, ClusterBlock> blockedIndices;
        private final AddIndexBlockClusterStateUpdateRequest request;

        private WaitForBlocksApplied(final Map<Index, ClusterBlock> blockedIndices,
                                     final AddIndexBlockClusterStateUpdateRequest request,
                                     final ActionListener<Map<Index, AddBlockResult>> listener) {
            super(listener);
            if (blockedIndices == null || blockedIndices.isEmpty()) {
                throw new IllegalArgumentException("Cannot wait for blocks to be applied, list of blocked indices is empty or null");
            }
            this.blockedIndices = blockedIndices;
            this.request = request;
        }

        @Override
        protected void doRun() throws Exception {
            final Map<Index, AddBlockResult> results = ConcurrentCollections.newConcurrentMap();
            final CountDown countDown = new CountDown(blockedIndices.size());
            final ClusterState state = clusterService.state();
            blockedIndices.forEach((index, block) -> {
                waitForShardsReady(index, block, state, response -> {
                    results.put(index, response);
                    if (countDown.countDown()) {
                        listener.onResponse(unmodifiableMap(results));
                    }
                });
            });
        }

        private void waitForShardsReady(final Index index,
                                        final ClusterBlock clusterBlock,
                                        final ClusterState state,
                                        final Consumer<AddBlockResult> onResponse) {
            final IndexMetadata indexMetadata = state.metadata().index(index);
            if (indexMetadata == null) {
                logger.debug("index {} has since been deleted, ignoring", index);
                onResponse.accept(new AddBlockResult(index));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                logger.debug("index {} is closed, no need to wait for shards, ignoring", index);
                onResponse.accept(new AddBlockResult(index));
                return;
            }

            final ImmutableOpenIntMap<IndexShardRoutingTable> shards = indexRoutingTable.getShards();
            final AtomicArray<AddBlockShardResult> results = new AtomicArray<>(shards.size());
            final CountDown countDown = new CountDown(shards.size());

            for (IntObjectCursor<IndexShardRoutingTable> shard : shards) {
                final IndexShardRoutingTable shardRoutingTable = shard.value;
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBlockRequest(shardRoutingTable, clusterBlock, new NotifyOnceListener<ReplicationResponse>() {
                    @Override
                    public void innerOnResponse(final ReplicationResponse replicationResponse) {
                        AddBlockShardResult.Failure[] failures = Arrays.stream(replicationResponse.getShardInfo().getFailures())
                            .map(f -> new AddBlockShardResult.Failure(f.index(), f.shardId(), f.getCause(), f.nodeId()))
                            .toArray(AddBlockShardResult.Failure[]::new);
                        results.setOnce(shardId, new AddBlockShardResult(shardId, failures));
                        processIfFinished();
                    }

                    @Override
                    public void innerOnFailure(final Exception e) {
                        AddBlockShardResult.Failure failure = new AddBlockShardResult.Failure(index.getName(), shardId, e);
                        results.setOnce(shardId, new AddBlockShardResult(shardId, new AddBlockShardResult.Failure[]{failure}));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            onResponse.accept(new AddBlockResult(index, results.toArray(new AddBlockShardResult[results.length()])));
                        }
                    }
                });
            }
        }

        private void sendVerifyShardBlockRequest(final IndexShardRoutingTable shardRoutingTable,
                                                 final ClusterBlock block,
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
            final TransportVerifyShardIndexBlockAction.ShardRequest shardRequest =
                new TransportVerifyShardIndexBlockAction.ShardRequest(shardId, block, parentTaskId);
            if (request.ackTimeout() != null) {
                shardRequest.timeout(request.ackTimeout());
            }
            client.executeLocally(TransportVerifyShardIndexBlockAction.TYPE, shardRequest, listener);
        }
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     */
    static Tuple<ClusterState, Collection<IndexResult>> closeRoutingTable(final ClusterState currentState,
                                                                          final Map<Index, ClusterBlock> blockedIndices,
                                                                          final Map<Index, IndexResult> verifyResult) {

        // Remove the index routing table of closed indices if the cluster is in a mixed version
        // that does not support the replication of closed indices
        final boolean removeRoutingTable = currentState.nodes().getMinNodeVersion().before(Version.V_7_2_0);

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        final Set<String> closedIndices = new HashSet<>();
        Map<Index, IndexResult> closingResults = new HashMap<>(verifyResult);
        for (Map.Entry<Index, IndexResult> result : verifyResult.entrySet()) {
            final Index index = result.getKey();
            final boolean acknowledged = result.getValue().hasFailures() == false;
            try {
                if (acknowledged == false) {
                    logger.debug("verification of shards before closing {} failed [{}]", index, result);
                    continue;
                }
                final IndexMetadata indexMetadata = metadata.getSafe(index);
                if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    logger.debug("verification of shards before closing {} succeeded but index is already closed", index);
                    assert currentState.blocks().hasIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                    continue;
                }
                final ClusterBlock closingBlock = blockedIndices.get(index);
                assert closingBlock != null;
                if (currentState.blocks().hasIndexBlock(index.getName(), closingBlock) == false) {
                    // we should report error in this case as the index can be left as open.
                    closingResults.put(result.getKey(), new IndexResult(result.getKey(), new IllegalStateException(
                        "verification of shards before closing " + index + " succeeded but block has been removed in the meantime")));
                    logger.debug("verification of shards before closing {} succeeded but block has been removed in the meantime", index);
                    continue;
                }

                // Check if index closing conflicts with any running restores
                Set<Index> restoringIndices = RestoreService.restoringIndices(currentState, singleton(index));
                if (restoringIndices.isEmpty() == false) {
                    closingResults.put(result.getKey(), new IndexResult(result.getKey(), new IllegalStateException(
                        "verification of shards before closing " + index + " succeeded but index is being restored in the meantime")));
                    logger.debug("verification of shards before closing {} succeeded but index is being restored in the meantime", index);
                    continue;
                }

                // Check if index closing conflicts with any running snapshots
                Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, singleton(index));
                if (snapshottingIndices.isEmpty() == false) {
                    closingResults.put(result.getKey(), new IndexResult(result.getKey(), new IllegalStateException(
                        "verification of shards before closing " + index + " succeeded but index is being snapshot in the meantime")));
                    logger.debug("verification of shards before closing {} succeeded but index is being snapshot in the meantime", index);
                    continue;
                }

                blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
                blocks.addIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE);
                if (removeRoutingTable) {
                    metadata.put(updatedMetadata);
                    routingTable.remove(index.getName());
                } else {
                    metadata.put(updatedMetadata
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)));
                    routingTable.addAsFromOpenToClose(metadata.getSafe(index));
                }

                logger.debug("closing index {} succeeded", index);
                closedIndices.add(index.getName());
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since it was blocked before closing, ignoring", index);
            }
        }
        logger.info("completed closing of indices {}", closedIndices);
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).metadata(metadata).routingTable(routingTable.build()).build(),
            closingResults.values());
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
        final List<IndexMetadata> indicesToOpen = new ArrayList<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
            if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
                indicesToOpen.add(indexMetadata);
            } else if (currentState.blocks().hasIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID)) {
                indicesToOpen.add(indexMetadata);
            }
        }

        shardLimitValidator.validateShardLimit(currentState, indices);
        if (indicesToOpen.isEmpty()) {
            return currentState;
        }

        logger.info(() -> new ParameterizedMessage("opening indices [{}]",
            String.join(",", indicesToOpen.stream().map(i -> (CharSequence) i.getIndex().toString())::iterator)));

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();

        for (IndexMetadata indexMetadata : indicesToOpen) {
            final Index index = indexMetadata.getIndex();
            if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
                final Settings.Builder updatedSettings = Settings.builder().put(indexMetadata.getSettings());
                updatedSettings.remove(VERIFIED_BEFORE_CLOSE_SETTING.getKey());

                IndexMetadata updatedIndexMetadata = IndexMetadata.builder(indexMetadata)
                    .state(IndexMetadata.State.OPEN)
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                    .settings(updatedSettings)
                    .build();

                // The index might be closed because we couldn't import it due to old incompatible version
                // We need to check that this index can be upgraded to the current version
                updatedIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(updatedIndexMetadata, minIndexCompatibilityVersion);
                try {
                    indicesService.verifyIndexMetadata(updatedIndexMetadata, updatedIndexMetadata);
                } catch (Exception e) {
                    throw new ElasticsearchException("Failed to verify index " + index, e);
                }
                metadata.put(updatedIndexMetadata, true);
            }

            // Always removes index closed blocks (note: this can fail on-going close index actions)
            blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
        }

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadata).blocks(blocks).build();

        final RoutingTable.Builder routingTable = RoutingTable.builder(updatedState.routingTable());
        for (IndexMetadata previousIndexMetadata : indicesToOpen) {
            if (previousIndexMetadata.getState() != IndexMetadata.State.OPEN) {
                routingTable.addAsFromCloseToOpen(updatedState.metadata().getIndexSafe(previousIndexMetadata.getIndex()));
            }
        }
        return ClusterState.builder(updatedState).routingTable(routingTable.build()).build();
    }

    /**
     * Finalizes the addition of blocks by turning the temporary UUID-based blocks into full blocks.
     * @param currentState the cluster state to update
     * @param blockedIndices the indices and their temporary UUID-based blocks to convert
     * @param verifyResult the index-level results for adding the block
     * @param block the full block to convert to
     * @return the updated cluster state, as well as the (failed and successful) index-level results for adding the block
     */
    static Tuple<ClusterState, Collection<AddBlockResult>> finalizeBlock(final ClusterState currentState,
                                                                         final Map<Index, ClusterBlock> blockedIndices,
                                                                         final Map<Index, AddBlockResult> verifyResult,
                                                                         final APIBlock block) {

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());

        final Set<String> effectivelyBlockedIndices = new HashSet<>();
        Map<Index, AddBlockResult> blockingResults = new HashMap<>(verifyResult);
        for (Map.Entry<Index, AddBlockResult> result : verifyResult.entrySet()) {
            final Index index = result.getKey();
            final boolean acknowledged = result.getValue().hasFailures() == false;
            try {
                if (acknowledged == false) {
                    logger.debug("verification of shards before blocking {} failed [{}]", index, result);
                    continue;
                }
                final IndexMetadata indexMetadata = metadata.getSafe(index);
                final ClusterBlock tempBlock = blockedIndices.get(index);
                assert tempBlock != null;
                assert tempBlock.uuid() != null;
                final ClusterBlock currentBlock = currentState.blocks().getIndexBlockWithId(index.getName(), tempBlock.id());
                if (currentBlock != null && currentBlock.equals(block.block)) {
                    logger.debug("verification of shards for {} succeeded, but block finalization already occurred" +
                        " (possibly for another block) [{}]", index, result);
                    continue;
                }

                if (currentBlock == null || currentBlock.equals(tempBlock) == false) {
                    // we should report error in this case as the index can be left as open.
                    blockingResults.put(result.getKey(), new AddBlockResult(result.getKey(), new IllegalStateException(
                        "verification of shards before blocking " + index + " succeeded but block has been removed in the meantime")));
                    logger.debug("verification of shards before blocking {} succeeded but block has been removed in the meantime", index);
                    continue;
                }

                assert currentBlock != null && currentBlock.equals(tempBlock) && currentBlock.id() == block.block.id();

                blocks.removeIndexBlockWithId(index.getName(), tempBlock.id());
                blocks.addIndexBlock(index.getName(), block.block);

                logger.debug("add block {} to index {} succeeded", block.block, index);
                effectivelyBlockedIndices.add(index.getName());
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since blocking it started, ignoring", index);
            }
        }
        logger.info("completed adding block {} to indices {}", block.name, effectivelyBlockedIndices);
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).metadata(metadata).routingTable(routingTable.build()).build(),
            blockingResults.values());
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

    public static boolean isIndexVerifiedBeforeClosed(final IndexMetadata indexMetadata) {
        return indexMetadata.getState() == IndexMetadata.State.CLOSE
            && VERIFIED_BEFORE_CLOSE_SETTING.exists(indexMetadata.getSettings())
            && VERIFIED_BEFORE_CLOSE_SETTING.get(indexMetadata.getSettings());
    }

    // Create UUID based block based on non-UUID one
    public static ClusterBlock createUUIDBasedBlock(ClusterBlock clusterBlock) {
        assert clusterBlock.uuid() == null : "no UUID expected on source block";
        return new ClusterBlock(clusterBlock.id(), UUIDs.randomBase64UUID(), "moving to block " + clusterBlock.description(),
            clusterBlock.retryable(), clusterBlock.disableStatePersistence(), clusterBlock.isAllowReleaseResources(), clusterBlock.status(),
            clusterBlock.levels());
    }
}
