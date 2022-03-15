/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexLongFieldRange;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Service responsible for submitting open/close index requests as well as for adding index blocks
 */
public class MetadataIndexStateService {
    private static final Logger logger = LogManager.getLogger(MetadataIndexStateService.class);

    public static final int INDEX_CLOSED_BLOCK_ID = 4;
    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(
        4,
        "index closed",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        ClusterBlockLevel.READ_WRITE
    );
    public static final Setting<Boolean> VERIFIED_BEFORE_CLOSE_SETTING = Setting.boolSetting(
        "index.verified_before_close",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final IndexMetadataVerifier indexMetadataVerifier;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final NodeClient client;
    private final ThreadPool threadPool;
    private final ActiveShardsObserver activeShardsObserver;
    private final ClusterStateTaskExecutor<OpenIndicesTask> opensExecutor;
    private final ClusterStateTaskExecutor<AddBlocksToCloseTask> addBlocksToCloseExecutor;
    private final ClusterStateTaskExecutor<CloseIndicesTask> closesExecutor;
    private final ClusterStateTaskExecutor<AddBlocksTask> addBlocksExecutor;
    private final ClusterStateTaskExecutor<FinalizeBlocksTask> finalizeBlocksExecutor;

    @Inject
    public MetadataIndexStateService(
        ClusterService clusterService,
        AllocationService allocationService,
        IndexMetadataVerifier indexMetadataVerifier,
        IndicesService indicesService,
        ShardLimitValidator shardLimitValidator,
        NodeClient client,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indexMetadataVerifier = indexMetadataVerifier;
        this.indicesService = indicesService;
        this.shardLimitValidator = shardLimitValidator;
        this.client = client;
        this.threadPool = threadPool;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
        this.opensExecutor = new OpenIndicesExecutor();
        this.addBlocksToCloseExecutor = new AddBlocksToCloseExecutor();
        this.closesExecutor = new CloseIndicesExecutor();
        this.addBlocksExecutor = new AddBlocksExecutor();
        this.finalizeBlocksExecutor = new FinalizeBlocksExecutor();
    }

    /**
     * Closes one or more indices.
     *
     * Closing indices is a 3 steps process: it first adds a write block to every indices to close, then waits for the operations on shards
     * to be terminated and finally closes the indices by moving their state to CLOSE.
     */
    public void closeIndices(final CloseIndexClusterStateUpdateRequest request, final ActionListener<CloseIndexResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "add-block-index-to-close " + Arrays.toString(request.indices()),
            new AddBlocksToCloseTask(request, listener),
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            this.addBlocksToCloseExecutor
        );
    }

    private class AddBlocksToCloseExecutor implements ClusterStateTaskExecutor<AddBlocksToCloseTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<AddBlocksToCloseTask>> taskContexts) throws Exception {
            ClusterState state = currentState;
            for (final var taskContext : taskContexts) {
                final var task = taskContext.getTask();
                try {
                    final Map<Index, ClusterBlock> blockedIndices = new HashMap<>(task.request.indices().length);
                    state = addIndexClosedBlocks(task.request.indices(), blockedIndices, state);
                    taskContext.success(task.listener.delegateFailure((delegate1, clusterState) -> {
                        if (blockedIndices.isEmpty()) {
                            delegate1.onResponse(CloseIndexResponse.EMPTY);
                        } else {
                            threadPool.executor(ThreadPool.Names.MANAGEMENT)
                                .execute(
                                    new WaitForClosedBlocksApplied(
                                        blockedIndices,
                                        task.request,
                                        delegate1.delegateFailure((delegate2, verifyResults) -> {
                                            clusterService.submitStateUpdateTask(
                                                "close-indices",
                                                new CloseIndicesTask(task.request, blockedIndices, verifyResults, delegate2),
                                                ClusterStateTaskConfig.build(Priority.URGENT),
                                                closesExecutor
                                            );
                                        })
                                    )
                                );
                        }
                    }));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return state;
        }
    }

    private record AddBlocksToCloseTask(CloseIndexClusterStateUpdateRequest request, ActionListener<CloseIndexResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }

    private class CloseIndicesExecutor implements ClusterStateTaskExecutor<CloseIndicesTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<CloseIndicesTask>> taskContexts) throws Exception {
            ClusterState state = currentState;
            for (final var taskContext : taskContexts) {
                final var task = taskContext.getTask();
                try {
                    final Tuple<ClusterState, List<IndexResult>> closingResult = closeRoutingTable(
                        state,
                        task.blockedIndices,
                        task.verifyResults
                    );
                    state = closingResult.v1();
                    final List<IndexResult> indices = closingResult.v2();
                    assert indices.size() == task.verifyResults.size();

                    taskContext.success(task.listener.delegateFailure((delegate, clusterState) -> {
                        final boolean acknowledged = indices.stream().noneMatch(IndexResult::hasFailures);
                        final String[] waitForIndices = indices.stream()
                            .filter(result -> result.hasFailures() == false)
                            .filter(result -> clusterState.routingTable().hasIndex(result.getIndex()))
                            .map(result -> result.getIndex().getName())
                            .toArray(String[]::new);

                        if (waitForIndices.length > 0) {
                            activeShardsObserver.waitForActiveShards(
                                waitForIndices,
                                task.request.waitForActiveShards(),
                                task.request.ackTimeout(),
                                shardsAcknowledged -> {
                                    if (shardsAcknowledged == false) {
                                        logger.debug(
                                            "[{}] indices closed, but the operation timed out while "
                                                + "waiting for enough shards to be started.",
                                            Arrays.toString(waitForIndices)
                                        );
                                    }
                                    // acknowledged maybe be false but some indices may have been correctly closed,
                                    // so we maintain a kind of coherency by overriding the shardsAcknowledged value
                                    // (see ShardsAcknowledgedResponse constructor)
                                    boolean shardsAcked = acknowledged ? shardsAcknowledged : false;
                                    delegate.onResponse(new CloseIndexResponse(acknowledged, shardsAcked, indices));
                                },
                                delegate::onFailure
                            );
                        } else {
                            delegate.onResponse(new CloseIndexResponse(acknowledged, false, indices));
                        }
                    }));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            return allocationService.reroute(state, "indices closed");
        }
    }

    private record CloseIndicesTask(
        CloseIndexClusterStateUpdateRequest request,
        Map<Index, ClusterBlock> blockedIndices,
        Map<Index, CloseIndexResponse.IndexResult> verifyResults,
        ActionListener<CloseIndexResponse> listener
    ) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }

    /**
     * Step 1 - Start closing indices by adding a write block
     *
     * This step builds the list of indices to close (the ones explicitly requested that are not in CLOSE state) and adds a unique cluster
     * block (or reuses an existing one) to every index to close in the cluster state. After the cluster state is published, the shards
     * should start to reject writing operations and we can proceed with step 2.
     */
    static ClusterState addIndexClosedBlocks(
        final Index[] indices,
        final Map<Index, ClusterBlock> blockedIndices,
        final ClusterState currentState
    ) {
        final Set<Index> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
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
            throw new SnapshotInProgressException(
                "Cannot close indices that are being snapshotted: "
                    + snapshottingIndices
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());

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

        logger.info(
            () -> new ParameterizedMessage(
                "closing indices {}",
                blockedIndices.keySet().stream().map(Object::toString).collect(Collectors.joining(","))
            )
        );
        return ClusterState.builder(currentState).blocks(blocks).build();
    }

    /**
     * Updates the cluster state for the given indices with the given index block,
     * and also returns the updated indices (and their blocks) in a map.
     * @param indices The indices to add blocks to if needed
     * @param currentState The current cluster state
     * @param block The type of block to add
     * @return a tuple of the updated cluster state, as well as the blocks that got added
     */
    private static Tuple<ClusterState, Map<Index, ClusterBlock>> addIndexBlock(
        final Index[] indices,
        final ClusterState currentState,
        final APIBlock block
    ) {
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
            return Tuple.tuple(currentState, Map.of());
        }

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
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
                final Settings updatedSettings = Settings.builder().put(indexMetadata.getSettings()).put(block.settingName(), true).build();

                metadata.put(
                    IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
                );
            }
        }

        logger.info("adding block {} to indices {}", block.name, blockedIndices.keySet().stream().map(Object::toString).toList());
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).metadata(metadata).build(), blockedIndices);
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
    public void addIndexBlock(AddIndexBlockClusterStateUpdateRequest request, ActionListener<AddIndexBlockResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }
        Metadata metadata = clusterService.state().metadata();
        List<String> writeIndices = new ArrayList<>();
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        for (Index index : concreteIndices) {
            IndexAbstraction ia = lookup.get(index.getName());
            if (ia != null && ia.getParentDataStream() != null) {
                Index writeIndex = metadata.index(ia.getParentDataStream().getWriteIndex()).getIndex();
                if (writeIndex.equals(index)) {
                    writeIndices.add(index.getName());
                }
            }
        }
        if (writeIndices.size() > 0) {
            throw new IllegalArgumentException(
                "cannot add a block to the following data stream write indices ["
                    + Strings.collectionToCommaDelimitedString(writeIndices)
                    + "]"
            );
        }

        clusterService.submitStateUpdateTask(
            "add-index-block-[" + request.getBlock().name + "]-" + Arrays.toString(concreteIndices),
            new AddBlocksTask(request, listener),
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            addBlocksExecutor
        );
    }

    private class AddBlocksExecutor implements ClusterStateTaskExecutor<AddBlocksTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<AddBlocksTask>> taskContexts) throws Exception {
            ClusterState state = currentState;

            for (final var taskContext : taskContexts) {
                try {
                    final var task = taskContext.getTask();
                    final Tuple<ClusterState, Map<Index, ClusterBlock>> blockResult = addIndexBlock(
                        task.request.indices(),
                        state,
                        task.request.getBlock()
                    );
                    state = blockResult.v1();
                    final Map<Index, ClusterBlock> blockedIndices = blockResult.v2();
                    taskContext.success(task.listener.delegateFailure((delegate1, clusterState) -> {
                        if (blockedIndices.isEmpty()) {
                            delegate1.onResponse(AddIndexBlockResponse.EMPTY);
                        } else {
                            threadPool.executor(ThreadPool.Names.MANAGEMENT)
                                .execute(
                                    new WaitForBlocksApplied(
                                        blockedIndices,
                                        task.request,
                                        delegate1.delegateFailure((delegate2, verifyResults) -> {
                                            clusterService.submitStateUpdateTask(
                                                "finalize-index-block-["
                                                    + task.request.getBlock().name
                                                    + "]-["
                                                    + blockedIndices.keySet().stream().map(Index::getName).collect(Collectors.joining(", "))
                                                    + "]",
                                                new FinalizeBlocksTask(task.request, blockedIndices, verifyResults, delegate2),
                                                ClusterStateTaskConfig.build(Priority.URGENT),
                                                finalizeBlocksExecutor
                                            );
                                        })
                                    )
                                );
                        }
                    }));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            return state;
        }
    }

    private record AddBlocksTask(AddIndexBlockClusterStateUpdateRequest request, ActionListener<AddIndexBlockResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }

    private static class FinalizeBlocksExecutor implements ClusterStateTaskExecutor<FinalizeBlocksTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<FinalizeBlocksTask>> taskContexts) throws Exception {
            ClusterState state = currentState;

            for (final var taskContext : taskContexts) {
                try {
                    final var task = taskContext.getTask();
                    final Tuple<ClusterState, List<AddBlockResult>> finalizeResult = finalizeBlock(
                        state,
                        task.blockedIndices,
                        task.verifyResults,
                        task.request.getBlock()
                    );
                    state = finalizeResult.v1();
                    final List<AddBlockResult> indices = finalizeResult.v2();
                    assert indices.size() == task.verifyResults.size();

                    taskContext.success(task.listener.delegateFailure((delegate, clusterState) -> {
                        final boolean acknowledged = indices.stream().noneMatch(AddBlockResult::hasFailures);
                        delegate.onResponse(new AddIndexBlockResponse(acknowledged, acknowledged, indices));
                    }));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            return state;
        }
    }

    private record FinalizeBlocksTask(
        AddIndexBlockClusterStateUpdateRequest request,
        Map<Index, ClusterBlock> blockedIndices,
        Map<Index, AddBlockResult> verifyResults,
        ActionListener<AddIndexBlockResponse> listener
    ) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }

    /**
     * Step 2 - Wait for indices to be ready for closing
     * <p>
     * This step iterates over the indices previously blocked and sends a {@link TransportVerifyShardBeforeCloseAction} to each shard. If
     * this action succeed then the shard is considered to be ready for closing. When all shards of a given index are ready for closing,
     * the index is considered ready to be closed.
     */
    private class WaitForClosedBlocksApplied extends ActionRunnable<Map<Index, IndexResult>> {

        private final Map<Index, ClusterBlock> blockedIndices;
        private final CloseIndexClusterStateUpdateRequest request;

        private WaitForClosedBlocksApplied(
            final Map<Index, ClusterBlock> blockedIndices,
            final CloseIndexClusterStateUpdateRequest request,
            final ActionListener<Map<Index, IndexResult>> listener
        ) {
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
                        listener.onResponse(Map.copyOf(results));
                    }
                });
            });
        }

        private void waitForShardsReadyForClosing(
            final Index index,
            final ClusterBlock closingBlock,
            final ClusterState state,
            final Consumer<IndexResult> onResponse
        ) {
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

            final AtomicArray<ShardResult> results = new AtomicArray<>(indexRoutingTable.size());
            final CountDown countDown = new CountDown(indexRoutingTable.size());

            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, new NotifyOnceListener<>() {
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
                        results.setOnce(shardId, new ShardResult(shardId, new ShardResult.Failure[] { failure }));
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

        private void sendVerifyShardBeforeCloseRequest(
            final IndexShardRoutingTable shardRoutingTable,
            final ClusterBlock closingBlock,
            final ActionListener<ReplicationResponse> listener
        ) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), request.taskId());
            final TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest = new TransportVerifyShardBeforeCloseAction.ShardRequest(
                shardId,
                closingBlock,
                true,
                parentTaskId
            );
            if (request.ackTimeout() != null) {
                shardRequest.timeout(request.ackTimeout());
            }
            client.executeLocally(
                TransportVerifyShardBeforeCloseAction.TYPE,
                shardRequest,
                listener.delegateFailure((delegate, replicationResponse) -> {
                    final TransportVerifyShardBeforeCloseAction.ShardRequest req = new TransportVerifyShardBeforeCloseAction.ShardRequest(
                        shardId,
                        closingBlock,
                        false,
                        parentTaskId
                    );
                    if (request.ackTimeout() != null) {
                        req.timeout(request.ackTimeout());
                    }
                    client.executeLocally(TransportVerifyShardBeforeCloseAction.TYPE, req, delegate);
                })
            );
        }
    }

    /**
     * Helper class that coordinates with shards to ensure that blocks have been properly applied to all shards using
     * {@link TransportVerifyShardIndexBlockAction}.
     */
    private class WaitForBlocksApplied extends ActionRunnable<Map<Index, AddBlockResult>> {

        private final Map<Index, ClusterBlock> blockedIndices;
        private final AddIndexBlockClusterStateUpdateRequest request;

        private WaitForBlocksApplied(
            final Map<Index, ClusterBlock> blockedIndices,
            final AddIndexBlockClusterStateUpdateRequest request,
            final ActionListener<Map<Index, AddBlockResult>> listener
        ) {
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
                        listener.onResponse(Map.copyOf(results));
                    }
                });
            });
        }

        private void waitForShardsReady(
            final Index index,
            final ClusterBlock clusterBlock,
            final ClusterState state,
            final Consumer<AddBlockResult> onResponse
        ) {
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

            final AtomicArray<AddBlockShardResult> results = new AtomicArray<>(indexRoutingTable.size());
            final CountDown countDown = new CountDown(indexRoutingTable.size());

            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBlockRequest(shardRoutingTable, clusterBlock, new NotifyOnceListener<>() {
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
                        results.setOnce(shardId, new AddBlockShardResult(shardId, new AddBlockShardResult.Failure[] { failure }));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            AddBlockResult result = new AddBlockResult(index, results.toArray(new AddBlockShardResult[results.length()]));
                            logger.debug("result of applying block to index {}: {}", index, result);
                            onResponse.accept(result);
                        }
                    }
                });
            }
        }

        private void sendVerifyShardBlockRequest(
            final IndexShardRoutingTable shardRoutingTable,
            final ClusterBlock block,
            final ActionListener<ReplicationResponse> listener
        ) {
            final ShardId shardId = shardRoutingTable.shardId();
            if (shardRoutingTable.primaryShard().unassigned()) {
                logger.debug("primary shard {} is unassigned, ignoring", shardId);
                final ReplicationResponse response = new ReplicationResponse();
                response.setShardInfo(new ReplicationResponse.ShardInfo(shardRoutingTable.size(), shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), request.taskId());
            final TransportVerifyShardIndexBlockAction.ShardRequest shardRequest = new TransportVerifyShardIndexBlockAction.ShardRequest(
                shardId,
                block,
                parentTaskId
            );
            if (request.ackTimeout() != null) {
                shardRequest.timeout(request.ackTimeout());
            }
            client.executeLocally(TransportVerifyShardIndexBlockAction.TYPE, shardRequest, listener);
        }
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     */
    static Tuple<ClusterState, List<IndexResult>> closeRoutingTable(
        final ClusterState currentState,
        final Map<Index, ClusterBlock> blockedIndices,
        final Map<Index, IndexResult> verifyResult
    ) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
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
                    closingResults.put(
                        result.getKey(),
                        new IndexResult(
                            result.getKey(),
                            new IllegalStateException(
                                "verification of shards before closing " + index + " succeeded but block has been removed in the meantime"
                            )
                        )
                    );
                    logger.debug("verification of shards before closing {} succeeded but block has been removed in the meantime", index);
                    continue;
                }

                // Check if index closing conflicts with any running restores
                Set<Index> restoringIndices = RestoreService.restoringIndices(currentState, Set.of(index));
                if (restoringIndices.isEmpty() == false) {
                    closingResults.put(
                        result.getKey(),
                        new IndexResult(
                            result.getKey(),
                            new IllegalStateException(
                                "verification of shards before closing " + index + " succeeded but index is being restored in the meantime"
                            )
                        )
                    );
                    logger.debug("verification of shards before closing {} succeeded but index is being restored in the meantime", index);
                    continue;
                }

                // Check if index closing conflicts with any running snapshots
                Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentState, Set.of(index));
                if (snapshottingIndices.isEmpty() == false) {
                    closingResults.put(
                        result.getKey(),
                        new IndexResult(
                            result.getKey(),
                            new IllegalStateException(
                                "verification of shards before closing " + index + " succeeded but index is being snapshot in the meantime"
                            )
                        )
                    );
                    logger.debug("verification of shards before closing {} succeeded but index is being snapshot in the meantime", index);
                    continue;
                }

                blocks.removeIndexBlockWithId(index.getName(), INDEX_CLOSED_BLOCK_ID);
                blocks.addIndexBlock(index.getName(), INDEX_CLOSED_BLOCK);
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE);
                metadata.put(
                    updatedMetadata.timestampRange(IndexLongFieldRange.NO_SHARDS)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(Settings.builder().put(indexMetadata.getSettings()).put(VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true))
                );
                routingTable.addAsFromOpenToClose(metadata.getSafe(index));

                logger.debug("closing index {} succeeded", index);
                closedIndices.add(index.getName());
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since it was blocked before closing, ignoring", index);
            }
        }
        logger.info("completed closing of indices {}", closedIndices);
        return Tuple.tuple(
            ClusterState.builder(currentState).blocks(blocks).metadata(metadata).routingTable(routingTable).build(),
            List.copyOf(closingResults.values())
        );
    }

    public void openIndices(final OpenIndexClusterStateUpdateRequest request, final ActionListener<ShardsAcknowledgedResponse> listener) {
        onlyOpenIndices(request, listener.delegateFailure((delegate, response) -> {
            if (response.isAcknowledged()) {
                String[] indexNames = Arrays.stream(request.indices()).map(Index::getName).toArray(String[]::new);
                activeShardsObserver.waitForActiveShards(
                    indexNames,
                    request.waitForActiveShards(),
                    request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug(
                                "[{}] indices opened, but the operation timed out while waiting for enough shards to be started.",
                                Arrays.toString(indexNames)
                            );
                        }
                        delegate.onResponse(ShardsAcknowledgedResponse.of(true, shardsAcknowledged));
                    },
                    listener::onFailure
                );
            } else {
                delegate.onResponse(ShardsAcknowledgedResponse.NOT_ACKNOWLEDGED);
            }
        }));
    }

    private void onlyOpenIndices(final OpenIndexClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        if (request.indices() == null || request.indices().length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        clusterService.submitStateUpdateTask(
            "open-indices " + Arrays.toString(request.indices()),
            new OpenIndicesTask(request, listener),
            ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
            this.opensExecutor
        );
    }

    /**
     * Finalizes the addition of blocks by turning the temporary UUID-based blocks into full blocks.
     * @param currentState the cluster state to update
     * @param blockedIndices the indices and their temporary UUID-based blocks to convert
     * @param verifyResult the index-level results for adding the block
     * @param block the full block to convert to
     * @return the updated cluster state, as well as the (failed and successful) index-level results for adding the block
     */
    private static Tuple<ClusterState, List<AddBlockResult>> finalizeBlock(
        final ClusterState currentState,
        final Map<Index, ClusterBlock> blockedIndices,
        final Map<Index, AddBlockResult> verifyResult,
        final APIBlock block
    ) {
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());

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
                final ClusterBlock tempBlock = blockedIndices.get(index);
                assert tempBlock != null;
                assert tempBlock.uuid() != null;
                final ClusterBlock currentBlock = currentState.blocks().getIndexBlockWithId(index.getName(), tempBlock.id());
                if (currentBlock != null && currentBlock.equals(block.block)) {
                    logger.debug(
                        "verification of shards for {} succeeded, but block finalization already occurred"
                            + " (possibly for another block) [{}]",
                        index,
                        result
                    );
                    continue;
                }

                if (currentBlock == null || currentBlock.equals(tempBlock) == false) {
                    // we should report error in this case as the index can be left as open.
                    blockingResults.put(
                        result.getKey(),
                        new AddBlockResult(
                            result.getKey(),
                            new IllegalStateException(
                                "verification of shards before blocking " + index + " succeeded but block has been removed in the meantime"
                            )
                        )
                    );
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
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).build(), List.copyOf(blockingResults.values()));
    }

    /**
     * @return Generates a {@link ClusterBlock} that blocks read and write operations on soon-to-be-closed indices. The
     * cluster block is generated with the id value equals to {@link #INDEX_CLOSED_BLOCK_ID} and a unique UUID.
     */
    public static ClusterBlock createIndexClosingBlock() {
        return new ClusterBlock(
            INDEX_CLOSED_BLOCK_ID,
            UUIDs.randomBase64UUID(),
            "index preparing to close. Reopen the index to allow writes again or retry closing the index to fully close the index.",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(ClusterBlockLevel.WRITE)
        );
    }

    public static boolean isIndexVerifiedBeforeClosed(final IndexMetadata indexMetadata) {
        return indexMetadata.getState() == IndexMetadata.State.CLOSE
            && VERIFIED_BEFORE_CLOSE_SETTING.exists(indexMetadata.getSettings())
            && VERIFIED_BEFORE_CLOSE_SETTING.get(indexMetadata.getSettings());
    }

    // Create UUID based block based on non-UUID one
    public static ClusterBlock createUUIDBasedBlock(ClusterBlock clusterBlock) {
        assert clusterBlock.uuid() == null : "no UUID expected on source block";
        return new ClusterBlock(
            clusterBlock.id(),
            UUIDs.randomBase64UUID(),
            "moving to block " + clusterBlock.description(),
            clusterBlock.retryable(),
            clusterBlock.disableStatePersistence(),
            clusterBlock.isAllowReleaseResources(),
            clusterBlock.status(),
            clusterBlock.levels()
        );
    }

    private class OpenIndicesExecutor implements ClusterStateTaskExecutor<OpenIndicesTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<OpenIndicesTask>> taskContexts) {
            ClusterState state = currentState;

            try {
                // build an in-order de-duplicated array of all the indices to open
                final Set<Index> indicesToOpen = new LinkedHashSet<>(taskContexts.size());
                for (final var taskContext : taskContexts) {
                    Collections.addAll(indicesToOpen, taskContext.getTask().request.indices());
                }
                Index[] indices = indicesToOpen.toArray(Index.EMPTY_ARRAY);

                // open them
                state = openIndices(indices, state);

                // do a final reroute
                state = allocationService.reroute(state, "indices opened");

                for (final var taskContext : taskContexts) {
                    final var task = taskContext.getTask();
                    taskContext.success(task);
                }
            } catch (Exception e) {
                for (final var taskContext : taskContexts) {
                    taskContext.onFailure(e);
                }
            }

            return state;
        }

        private ClusterState openIndices(final Index[] indices, final ClusterState currentState) {
            final List<IndexMetadata> indicesToOpen = new ArrayList<>(indices.length);
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

            logger.info(() -> {
                final StringBuilder indexNames = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    indicesToOpen.stream().map(i -> (CharSequence) i.getIndex().toString()).toList(),
                    ",",
                    "",
                    "",
                    512,
                    indexNames
                );
                return new ParameterizedMessage("opening indices [{}]", indexNames);
            });

            final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
            final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();

            for (IndexMetadata indexMetadata : indicesToOpen) {
                final Index index = indexMetadata.getIndex();
                if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
                    final Settings.Builder updatedSettings = Settings.builder().put(indexMetadata.getSettings());
                    updatedSettings.remove(VERIFIED_BEFORE_CLOSE_SETTING.getKey());

                    IndexMetadata newIndexMetadata = IndexMetadata.builder(indexMetadata)
                        .state(IndexMetadata.State.OPEN)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(updatedSettings)
                        .timestampRange(IndexLongFieldRange.NO_SHARDS)
                        .build();

                    // The index might be closed because we couldn't import it due to an old incompatible
                    // version, so we need to verify its compatibility.
                    newIndexMetadata = indexMetadataVerifier.verifyIndexMetadata(newIndexMetadata, minIndexCompatibilityVersion);
                    try {
                        indicesService.verifyIndexMetadata(newIndexMetadata, newIndexMetadata);
                    } catch (Exception e) {
                        throw new ElasticsearchException("Failed to verify index " + index, e);
                    }
                    metadata.put(newIndexMetadata, true);
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
            return ClusterState.builder(updatedState).routingTable(routingTable).build();
        }
    }

    private record OpenIndicesTask(OpenIndexClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener,
            ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(AcknowledgedResponse.of(true));
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(AcknowledgedResponse.of(false));
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            assert false : "not called";
        }
    }
}
