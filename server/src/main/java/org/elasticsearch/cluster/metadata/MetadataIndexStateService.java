/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
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
import org.elasticsearch.action.admin.indices.readonly.RemoveIndexBlockClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.readonly.RemoveIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.RemoveIndexBlockResponse.RemoveBlockResult;
import org.elasticsearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexReshardService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.injection.guice.Inject;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.cluster.metadata.IndexMetadataVerifier.hasReadOnlyBlocks;
import static org.elasticsearch.core.Strings.format;

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

    public static final Setting<Boolean> VERIFIED_READ_ONLY_SETTING = Setting.boolSetting(
        "index.verified_read_only",
        false,
        Setting.Property.IndexScope,
        Setting.Property.NotCopyableOnResize,
        Setting.Property.PrivateIndex
    );

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final IndexMetadataVerifier indexMetadataVerifier;
    private final IndicesService indicesService;
    private final ShardLimitValidator shardLimitValidator;
    private final NodeClient client;
    private final ThreadPool threadPool;
    private final MasterServiceTaskQueue<OpenIndicesTask> opensQueue;
    private final MasterServiceTaskQueue<AddBlocksToCloseTask> addBlocksToCloseQueue;
    private final MasterServiceTaskQueue<CloseIndicesTask> closesQueue;
    private final MasterServiceTaskQueue<AddBlocksTask> addBlocksQueue;
    private final MasterServiceTaskQueue<FinalizeBlocksTask> finalizeBlocksQueue;
    private final MasterServiceTaskQueue<RemoveBlocksTask> removeBlocksQueue;

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

        opensQueue = clusterService.createTaskQueue("open-index", Priority.URGENT, new OpenIndicesExecutor());
        addBlocksToCloseQueue = clusterService.createTaskQueue("add-blocks-to-close", Priority.URGENT, new AddBlocksToCloseExecutor());
        closesQueue = clusterService.createTaskQueue("close-index", Priority.URGENT, new CloseIndicesExecutor());
        addBlocksQueue = clusterService.createTaskQueue("add-blocks", Priority.URGENT, new AddBlocksExecutor());
        finalizeBlocksQueue = clusterService.createTaskQueue("finalize-blocks", Priority.URGENT, new FinalizeBlocksExecutor());
        removeBlocksQueue = clusterService.createTaskQueue("remove-blocks", Priority.URGENT, new RemoveBlocksExecutor());
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

        addBlocksToCloseQueue.submitTask(
            "add-block-index-to-close " + Arrays.toString(request.indices()),
            new AddBlocksToCloseTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    private class AddBlocksToCloseExecutor extends SimpleBatchedExecutor<AddBlocksToCloseTask, Map<Index, ClusterBlock>> {

        @Override
        public Tuple<ClusterState, Map<Index, ClusterBlock>> executeTask(AddBlocksToCloseTask task, ClusterState clusterState)
            throws Exception {
            final Map<Index, ClusterBlock> blockedIndices = new HashMap<>(task.request.indices().length);
            var updatedClusterState = addIndexClosedBlocks(task.request.projectId(), task.request.indices(), blockedIndices, clusterState);
            return Tuple.tuple(updatedClusterState, blockedIndices);
        }

        @Override
        public void taskSucceeded(AddBlocksToCloseTask task, Map<Index, ClusterBlock> blockedIndices) {
            if (blockedIndices.isEmpty()) {
                task.listener().onResponse(CloseIndexResponse.EMPTY);
            } else {
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
                    .execute(
                        new WaitForClosedBlocksApplied(
                            blockedIndices,
                            task.request,
                            task.listener()
                                .delegateFailure(
                                    (delegate2, verifyResults) -> closesQueue.submitTask(
                                        "close-indices",
                                        new CloseIndicesTask(task.request, blockedIndices, verifyResults, delegate2),
                                        null
                                    )
                                )
                        )
                    );
            }
        }
    }

    private record AddBlocksToCloseTask(CloseIndexClusterStateUpdateRequest request, ActionListener<CloseIndexResponse> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private class CloseIndicesExecutor implements ClusterStateTaskExecutor<CloseIndicesTask> {

        @Override
        @SuppressForbidden(reason = "consuming published cluster state for legacy reasons")
        public ClusterState execute(BatchExecutionContext<CloseIndicesTask> batchExecutionContext) {
            var listener = new AllocationActionMultiListener<CloseIndexResponse>(threadPool.getThreadContext());
            var state = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                final var task = taskContext.getTask();
                try {
                    final ProjectId projectId = task.request.projectId();
                    final Tuple<ClusterState, List<IndexResult>> closingResult = closeRoutingTable(
                        state,
                        projectId,
                        task.blockedIndices,
                        task.verifyResults,
                        allocationService.getShardRoutingRoleStrategy()
                    );
                    state = closingResult.v1();
                    final List<IndexResult> indices = closingResult.v2();
                    assert indices.size() == task.verifyResults.size();

                    taskContext.success(clusterState -> {
                        final boolean acknowledged = indices.stream().noneMatch(IndexResult::hasFailures);
                        final String[] waitForIndices = indices.stream()
                            .filter(result -> result.hasFailures() == false)
                            .filter(result -> clusterState.routingTable(projectId).hasIndex(result.getIndex()))
                            .map(result -> result.getIndex().getName())
                            .toArray(String[]::new);

                        if (waitForIndices.length > 0) {
                            ActiveShardsObserver.waitForActiveShards(
                                clusterService,
                                projectId,
                                waitForIndices,
                                task.request.waitForActiveShards(),
                                task.request.ackTimeout(),
                                listener.delay(task.listener()).map(shardsAcknowledged -> {
                                    if (shardsAcknowledged == false) {
                                        logger.debug(
                                            () -> format(
                                                "[%s] indices closed, but the operation timed out while "
                                                    + "waiting for enough shards to be started.",
                                                Arrays.toString(waitForIndices)
                                            )
                                        );
                                    }
                                    // acknowledged maybe be false but some indices may have been correctly closed,
                                    // so we maintain a kind of coherency by overriding the shardsAcknowledged value
                                    // (see ShardsAcknowledgedResponse constructor)
                                    boolean shardsAcked = acknowledged ? shardsAcknowledged : false;
                                    return new CloseIndexResponse(acknowledged, shardsAcked, indices);
                                })
                            );
                        } else {
                            listener.delay(task.listener()).onResponse(new CloseIndexResponse(acknowledged, false, indices));
                        }
                    });
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                // reroute may encounter deprecated features but the resulting warnings are not associated with any particular task
                return allocationService.reroute(state, "indices closed", listener.reroute());
            }
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
    }

    /**
     * Step 1 - Start closing indices by adding a write block
     *
     * This step builds the list of indices to close (the ones explicitly requested that are not in CLOSE state) and adds a unique cluster
     * block (or reuses an existing one) to every index to close in the cluster state. After the cluster state is published, the shards
     * should start to reject writing operations and we can proceed with step 2.
     */
    static ClusterState addIndexClosedBlocks(
        final ProjectId projectId,
        final Index[] indices,
        final Map<Index, ClusterBlock> blockedIndices,
        final ClusterState currentState
    ) {
        final Set<Index> indicesToClose = new HashSet<>();
        for (Index index : indices) {
            final IndexMetadata indexMetadata = currentState.metadata().getProject(projectId).getIndexSafe(index);
            if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                indicesToClose.add(index);
            } else {
                logger.debug("index {} is already closed, ignoring", index);
                assert currentState.blocks().hasIndexBlock(projectId, index.getName(), INDEX_CLOSED_BLOCK);
            }
        }

        if (indicesToClose.isEmpty()) {
            return currentState;
        }

        final ProjectState currentProjectState = currentState.projectState(projectId);
        // Check if index closing conflicts with any running restores
        Set<Index> restoringIndices = RestoreService.restoringIndices(currentProjectState, indicesToClose);
        if (restoringIndices.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot close indices that are being restored: " + restoringIndices);
        }

        // Check if index closing conflicts with any running snapshots
        Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentProjectState, indicesToClose);
        if (snapshottingIndices.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "Cannot close indices that are being snapshotted: "
                    + snapshottingIndices
                    + ". Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        // Check if index closing conflicts with ongoing resharding
        Set<Index> reshardingIndices = IndexReshardService.reshardingIndices(currentProjectState, indicesToClose);
        if (reshardingIndices.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot close indices that are being resharded: " + reshardingIndices);
        }

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());

        for (Index index : indicesToClose) {
            ClusterBlock indexBlock = null;
            final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices(projectId).get(index.getName());
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
            blocks.addIndexBlock(projectId, index.getName(), indexBlock);
            blockedIndices.put(index, indexBlock);
        }

        logger.info(() -> format("closing indices %s", blockedIndices.keySet().stream().map(Object::toString).collect(joining(","))));
        return ClusterState.builder(currentState).blocks(blocks).build();
    }

    /**
     * Updates the cluster state for the given indices with the given index block,
     * and also returns the updated indices (and their blocks) in a map.
     * @param projectId The project that owns the indices
     * @param indices The indices to add blocks to if needed
     * @param currentState The current cluster state
     * @param block The type of block to add
     * @return a tuple of the updated cluster state, as well as the blocks that got added
     */
    private static Tuple<ClusterState, Map<Index, ClusterBlock>> addIndexBlock(
        final ProjectId projectId,
        final Index[] indices,
        final ClusterState currentState,
        final APIBlock block
    ) {
        final ProjectMetadata.Builder metadata = ProjectMetadata.builder(currentState.metadata().getProject(projectId));

        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
        final Set<Index> indicesToAddBlock = new HashSet<>();
        for (Index index : indices) {
            IndexMetadata indexMetadata = metadata.getSafe(index);// to check if index exists
            if (currentState.blocks().hasIndexBlock(projectId, index.getName(), block.block)) {
                if (block.block.contains(ClusterBlockLevel.WRITE) && isIndexWriteBlockVerified(indexMetadata)) {
                    logger.debug("index {} already has block {}, ignoring", index, block.block);
                } else {
                    // remove the block, we'll add a uuid based block below instead, never leaving it unblocked.
                    blocks.removeIndexBlock(projectId, index.getName(), block.block);
                    indicesToAddBlock.add(index);
                }
            } else {
                indicesToAddBlock.add(index);
            }
        }

        if (indicesToAddBlock.isEmpty()) {
            return Tuple.tuple(currentState, Map.of());
        }

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        for (Index index : indicesToAddBlock) {
            ClusterBlock indexBlock = null;
            final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices(projectId).get(index.getName());
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == block.block.id() && clusterBlock.uuid() != null) {
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
            blocks.addIndexBlock(projectId, index.getName(), indexBlock);
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

        logger.info(
            "adding [index.blocks.{}] block to indices {}",
            block.name,
            blockedIndices.keySet().stream().map(Object::toString).toList()
        );
        return Tuple.tuple(ClusterState.builder(currentState).blocks(blocks).putProjectMetadata(metadata).build(), blockedIndices);
    }

    private static boolean isIndexWriteBlockVerified(IndexMetadata indexMetadata) {
        return VERIFIED_READ_ONLY_SETTING.get(indexMetadata.getSettings());
    }

    /**
     * Adds an index block based on the given request, and notifies the listener upon completion.
     * Adding blocks is done in three steps:
     * - First, a temporary UUID-based block is added to the index
     *   (see {@link #addIndexBlock(ProjectId, Index[], ClusterState, APIBlock)}.
     * - Second, shards are checked to have properly applied the UUID-based block.
     *   (see {@link WaitForBlocksApplied}).
     * - Third, the temporary UUID-based block is turned into a full block
     *   (see {@link #finalizeBlock}.
     * Using this three-step process ensures non-interference by other operations in case where
     * we notify successful completion here.
     */
    public void addIndexBlock(AddIndexBlockClusterStateUpdateRequest request, ActionListener<AddIndexBlockResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }
        ProjectMetadata metadata = clusterService.state().metadata().getProject(request.projectId());
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

        addBlocksQueue.submitTask(
            "add-index-block-[" + request.block().name + "]-" + Arrays.toString(concreteIndices),
            new AddBlocksTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    /**
     * Removes an index block and notifies the listener upon completion.
     * Unlike adding blocks, removing blocks does not require shard verification.
     * The operation is idempotent and will succeed even if the block doesn't exist.
     */
    public void removeIndexBlock(RemoveIndexBlockClusterStateUpdateRequest request, ActionListener<RemoveIndexBlockResponse> listener) {
        final Index[] concreteIndices = request.indices();
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new IllegalArgumentException("Index name is required");
        }

        removeBlocksQueue.submitTask(
            "remove-index-block-[" + request.block().name + "]-" + Arrays.toString(concreteIndices),
            new RemoveBlocksTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    private class AddBlocksExecutor extends SimpleBatchedExecutor<AddBlocksTask, Map<Index, ClusterBlock>> {

        @Override
        public Tuple<ClusterState, Map<Index, ClusterBlock>> executeTask(AddBlocksTask task, ClusterState clusterState) {
            return addIndexBlock(task.request.projectId(), task.request.indices(), clusterState, task.request.block());
        }

        @Override
        public void taskSucceeded(AddBlocksTask task, Map<Index, ClusterBlock> blockedIndices) {
            if (blockedIndices.isEmpty()) {
                task.listener().onResponse(AddIndexBlockResponse.EMPTY);
            } else {
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
                    .execute(
                        new WaitForBlocksApplied(
                            blockedIndices,
                            task.request,
                            task.listener()
                                .delegateFailure(
                                    (delegate2, verifyResults) -> finalizeBlocksQueue.submitTask(
                                        "finalize-index-block-["
                                            + task.request.block().name
                                            + "]-["
                                            + blockedIndices.keySet().stream().map(Index::getName).collect(Collectors.joining(", "))
                                            + "]",
                                        new FinalizeBlocksTask(
                                            task.request,
                                            blockedIndices,
                                            verifyResults,
                                            task.request().markVerified()
                                                && clusterService.state()
                                                    .getMinTransportVersion()
                                                    .onOrAfter(TransportVersions.ADD_INDEX_BLOCK_TWO_PHASE),
                                            delegate2
                                        ),
                                        null
                                    )
                                )
                        )
                    );
            }
        }
    }

    private record AddBlocksTask(AddIndexBlockClusterStateUpdateRequest request, ActionListener<AddIndexBlockResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class FinalizeBlocksExecutor extends SimpleBatchedExecutor<FinalizeBlocksTask, List<AddBlockResult>> {

        @Override
        public Tuple<ClusterState, List<AddBlockResult>> executeTask(FinalizeBlocksTask task, ClusterState clusterState) throws Exception {
            final Tuple<ClusterState, List<AddBlockResult>> finalizeResult = finalizeBlock(
                clusterState,
                task.request.projectId(),
                task.blockedIndices,
                task.verifyResults,
                task.request.block(),
                task.markVerified()
            );
            assert finalizeResult.v2().size() == task.verifyResults.size();
            return finalizeResult;
        }

        @Override
        public void taskSucceeded(FinalizeBlocksTask task, List<AddBlockResult> indices) {
            final boolean acknowledged = indices.stream().noneMatch(AddBlockResult::hasFailures);
            task.listener().onResponse(new AddIndexBlockResponse(acknowledged, acknowledged, indices));
        }
    }

    private record FinalizeBlocksTask(
        AddIndexBlockClusterStateUpdateRequest request,
        Map<Index, ClusterBlock> blockedIndices,
        Map<Index, AddBlockResult> verifyResults,
        boolean markVerified,
        ActionListener<AddIndexBlockResponse> listener
    ) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private class RemoveBlocksExecutor extends SimpleBatchedExecutor<RemoveBlocksTask, List<RemoveBlockResult>> {

        @Override
        public Tuple<ClusterState, List<RemoveBlockResult>> executeTask(RemoveBlocksTask task, ClusterState clusterState) {
            return removeIndexBlock(task.request.projectId(), task.request.indices(), clusterState, task.request.block());
        }

        @Override
        public void taskSucceeded(RemoveBlocksTask task, List<RemoveBlockResult> results) {
            final boolean acknowledged = results.stream().noneMatch(RemoveBlockResult::hasFailures);
            task.listener().onResponse(new RemoveIndexBlockResponse(acknowledged, results));
        }
    }

    private record RemoveBlocksTask(RemoveIndexBlockClusterStateUpdateRequest request, ActionListener<RemoveIndexBlockResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
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
            final ProjectId projectId = request.projectId();
            final IndexMetadata indexMetadata = state.metadata().hasProject(projectId)
                ? state.metadata().getProject(projectId).index(index)
                : null;
            if (indexMetadata == null) {
                logger.debug("index {} has been blocked before closing and is now deleted, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable(projectId).index(index);
            if (indexRoutingTable == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                assert state.blocks().hasIndexBlock(projectId, index.getName(), INDEX_CLOSED_BLOCK);
                logger.debug("index {} has been blocked before closing and is already closed, ignoring", index);
                onResponse.accept(new IndexResult(index));
                return;
            }

            final AtomicArray<ShardResult> results = new AtomicArray<>(indexRoutingTable.size());
            final CountDown countDown = new CountDown(indexRoutingTable.size());

            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                final int shardId = shardRoutingTable.shardId().id();
                sendVerifyShardBeforeCloseRequest(shardRoutingTable, closingBlock, ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ReplicationResponse replicationResponse) {
                        ShardResult.Failure[] failures = Arrays.stream(replicationResponse.getShardInfo().getFailures())
                            .map(f -> new ShardResult.Failure(f.index(), f.shardId(), f.getCause(), f.nodeId()))
                            .toArray(ShardResult.Failure[]::new);
                        results.setOnce(shardId, new ShardResult(shardId, failures));
                        processIfFinished();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        ShardResult.Failure failure = new ShardResult.Failure(index.getName(), shardId, e);
                        results.setOnce(shardId, new ShardResult(shardId, new ShardResult.Failure[] { failure }));
                        processIfFinished();
                    }

                    private void processIfFinished() {
                        if (countDown.countDown()) {
                            onResponse.accept(new IndexResult(index, results.toArray(new ShardResult[results.length()])));
                        }
                    }
                }));
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
                response.setShardInfo(ReplicationResponse.ShardInfo.allSuccessful(shardRoutingTable.size()));
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
            final ProjectId projectId = request.projectId();
            final IndexMetadata indexMetadata = state.metadata().hasProject(projectId)
                ? state.metadata().getProject(projectId).index(index)
                : null;
            if (indexMetadata == null) {
                logger.debug("index {} has since been deleted, ignoring", index);
                onResponse.accept(new AddBlockResult(index));
                return;
            }
            final IndexRoutingTable indexRoutingTable = state.routingTable(projectId).index(index);
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
                sendVerifyShardBlockRequest(shardRoutingTable, clusterBlock, ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ReplicationResponse replicationResponse) {
                        AddBlockShardResult.Failure[] failures = Arrays.stream(replicationResponse.getShardInfo().getFailures())
                            .map(f -> new AddBlockShardResult.Failure(f.index(), f.shardId(), f.getCause(), f.nodeId()))
                            .toArray(AddBlockShardResult.Failure[]::new);
                        results.setOnce(shardId, new AddBlockShardResult(shardId, failures));
                        processIfFinished();
                    }

                    @Override
                    public void onFailure(Exception e) {
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
                }));
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
                response.setShardInfo(ReplicationResponse.ShardInfo.allSuccessful(shardRoutingTable.size()));
                listener.onResponse(response);
                return;
            }
            final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), request.taskId());
            final TransportVerifyShardIndexBlockAction.ShardRequest shardRequest = new TransportVerifyShardIndexBlockAction.ShardRequest(
                shardId,
                block,
                true,
                parentTaskId
            );
            shardRequest.timeout(request.ackTimeout());
            client.executeLocally(
                TransportVerifyShardIndexBlockAction.TYPE,
                shardRequest,
                listener.delegateFailure((delegate, replicationResponse) -> {
                    final var phase2 = new TransportVerifyShardIndexBlockAction.ShardRequest(shardId, block, false, parentTaskId);
                    if (request.ackTimeout() != null) {
                        phase2.timeout(request.ackTimeout());
                    }
                    client.executeLocally(TransportVerifyShardIndexBlockAction.TYPE, phase2, delegate);
                })
            );
        }
    }

    /**
     * Step 3 - Move index states from OPEN to CLOSE in cluster state for indices that are ready for closing.
     */
    static Tuple<ClusterState, List<IndexResult>> closeRoutingTable(
        final ClusterState currentState,
        final ProjectId projectId,
        final Map<Index, ClusterBlock> blockedIndices,
        final Map<Index, IndexResult> verifyResult,
        ShardRoutingRoleStrategy shardRoutingRoleStrategy
    ) {
        final ProjectMetadata.Builder metadata = ProjectMetadata.builder(currentState.metadata().getProject(projectId));
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
        final RoutingTable.Builder routingTable = RoutingTable.builder(shardRoutingRoleStrategy, currentState.routingTable(projectId));

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
                    assert currentState.blocks().hasIndexBlock(projectId, index.getName(), INDEX_CLOSED_BLOCK);
                    continue;
                }
                final ClusterBlock closingBlock = blockedIndices.get(index);
                assert closingBlock != null;
                if (currentState.blocks().hasIndexBlock(projectId, index.getName(), closingBlock) == false) {
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
                final ProjectState currentProjectState = currentState.projectState(projectId);
                Set<Index> restoringIndices = RestoreService.restoringIndices(currentProjectState, Set.of(index));
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
                Set<Index> snapshottingIndices = SnapshotsService.snapshottingIndices(currentProjectState, Set.of(index));
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

                // Check if index closing conflicts with ongoing resharding
                Set<Index> reshardingIndices = IndexReshardService.reshardingIndices(currentProjectState, Set.of(index));
                if (reshardingIndices.isEmpty() == false) {
                    closingResults.put(
                        result.getKey(),
                        new IndexResult(
                            result.getKey(),
                            new IllegalStateException(
                                "verification of shards before closing " + index + " succeeded but index is being resharded in the meantime"
                            )
                        )
                    );
                    logger.debug("verification of shards before closing {} succeeded but index is being resharded in the meantime", index);
                    continue;
                }

                blocks.removeIndexBlockWithId(projectId, index.getName(), INDEX_CLOSED_BLOCK_ID);
                blocks.addIndexBlock(projectId, index.getName(), INDEX_CLOSED_BLOCK);
                final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata).state(IndexMetadata.State.CLOSE);
                metadata.put(
                    updatedMetadata.timestampRange(IndexLongFieldRange.NO_SHARDS)
                        .eventIngestedRange(IndexLongFieldRange.NO_SHARDS)
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
            ClusterState.builder(currentState)
                .blocks(blocks)
                .putProjectMetadata(metadata)
                .putRoutingTable(projectId, routingTable.build())
                .build(),
            List.copyOf(closingResults.values())
        );
    }

    public void openIndices(final OpenIndexClusterStateUpdateRequest request, final ActionListener<ShardsAcknowledgedResponse> listener) {
        onlyOpenIndices(request, listener.delegateFailure((delegate, response) -> {
            if (response.isAcknowledged()) {
                String[] indexNames = Arrays.stream(request.indices()).map(Index::getName).toArray(String[]::new);
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    request.projectId(),
                    indexNames,
                    request.waitForActiveShards(),
                    request.ackTimeout(),
                    delegate.map(shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug(
                                () -> format(
                                    "[%s] indices opened, but the operation timed out while waiting for enough shards to be started.",
                                    Arrays.toString(indexNames)
                                )
                            );
                        }
                        return ShardsAcknowledgedResponse.of(true, shardsAcknowledged);
                    })
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

        opensQueue.submitTask(
            "open-indices " + Arrays.toString(request.indices()),
            new OpenIndicesTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    /**
     * Finalizes the addition of blocks by turning the temporary UUID-based blocks into full blocks.
     * @param currentState the cluster state to update
     * @param projectId The project that owns the indices
     * @param blockedIndices the indices and their temporary UUID-based blocks to convert
     * @param verifyResult the index-level results for adding the block
     * @param block the full block to convert to
     * @param markVerified if the index should be marked verified in case of a write-level block.
     * @return the updated cluster state, as well as the (failed and successful) index-level results for adding the block
     */
    private static Tuple<ClusterState, List<AddBlockResult>> finalizeBlock(
        final ClusterState currentState,
        final ProjectId projectId,
        final Map<Index, ClusterBlock> blockedIndices,
        final Map<Index, AddBlockResult> verifyResult,
        final APIBlock block,
        final boolean markVerified
    ) {
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
        final ProjectMetadata.Builder metadata = ProjectMetadata.builder(currentState.metadata().getProject(projectId));

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
                final ClusterBlock currentBlock = currentState.blocks().getIndexBlockWithId(projectId, index.getName(), tempBlock.id());
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

                blocks.removeIndexBlockWithId(projectId, index.getName(), tempBlock.id());
                blocks.addIndexBlock(projectId, index.getName(), block.block);

                logger.debug("add block {} to index {} succeeded", block.block, index);
                effectivelyBlockedIndices.add(index.getName());

                if (block.getBlock().contains(ClusterBlockLevel.WRITE) && markVerified) {
                    final IndexMetadata indexMetadata = metadata.getSafe(index);
                    if (VERIFIED_READ_ONLY_SETTING.get(indexMetadata.getSettings()) == false) {
                        final IndexMetadata.Builder updatedMetadata = IndexMetadata.builder(indexMetadata)
                            .settings(Settings.builder().put(indexMetadata.getSettings()).put(VERIFIED_READ_ONLY_SETTING.getKey(), true))
                            .settingsVersion(indexMetadata.getSettingsVersion() + 1);
                        metadata.put(updatedMetadata);
                    }
                }
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since blocking it started, ignoring", index);
            }
        }
        logger.info("completed adding [index.blocks.{}] block to indices {}", block.name, effectivelyBlockedIndices);
        return Tuple.tuple(
            ClusterState.builder(currentState).putProjectMetadata(metadata).blocks(blocks).build(),
            List.copyOf(blockingResults.values())
        );
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

    private static Tuple<ClusterState, List<RemoveBlockResult>> removeIndexBlock(
        final ProjectId projectId,
        final Index[] indices,
        final ClusterState currentState,
        final APIBlock block
    ) {
        final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
        final List<String> effectivelyUnblockedIndices = new ArrayList<>();
        final Map<String, RemoveBlockResult> results = new HashMap<>();

        for (Index index : indices) {
            try {
                if (currentState.metadata().hasProject(projectId) == false) {
                    results.put(index.getName(), new RemoveBlockResult(index, new IndexNotFoundException(index)));
                    continue;
                }

                final ProjectMetadata projectMetadata = currentState.metadata().getProject(projectId);
                final IndexMetadata indexMetadata = projectMetadata.getIndexSafe(index);
                if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    results.put(index.getName(), new RemoveBlockResult(index, new IndexClosedException(index)));
                    continue;
                }

                final Settings indexSettings = indexMetadata.getSettings();
                final boolean hasBlockSetting = block.setting().get(indexSettings);

                // Check for both setting-based blocks and UUID-based temporary blocks
                boolean hasAnyBlock = hasBlockSetting;
                boolean hasUUIDBlock = false;

                // Check for UUID-based blocks (temporary blocks created during add operation)
                final Set<ClusterBlock> clusterBlocks = currentState.blocks().indices(projectId).get(index.getName());
                if (clusterBlocks != null) {
                    for (ClusterBlock clusterBlock : clusterBlocks) {
                        if (clusterBlock.id() == block.block.id()) {
                            hasAnyBlock = true;
                            if (clusterBlock.uuid() != null) {
                                hasUUIDBlock = true;
                            }
                            break;
                        }
                    }
                }

                if (hasAnyBlock == false) {
                    // No block found (neither setting-based nor UUID-based)
                    results.put(index.getName(), new RemoveBlockResult(index));
                    continue;
                }

                // Remove the block setting if it exists
                if (hasBlockSetting) {
                    final Settings.Builder updatedSettings = Settings.builder().put(indexSettings);
                    updatedSettings.remove(block.settingName());

                    final IndexMetadata updatedMetadata = IndexMetadata.builder(indexMetadata)
                        .settings(updatedSettings)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .build();

                    metadata.getProject(projectId).put(updatedMetadata, true);
                }

                // Remove all blocks with the same ID (including UUID-based temporary blocks)
                if (hasUUIDBlock) {
                    blocks.removeIndexBlockWithId(projectId, index.getName(), block.block.id());
                } else {
                    blocks.removeIndexBlock(projectId, index.getName(), block.block);
                }

                effectivelyUnblockedIndices.add(index.getName());
                results.put(index.getName(), new RemoveBlockResult(index));

                logger.debug("remove block {} from index {} succeeded", block.block, index);
            } catch (final IndexNotFoundException e) {
                logger.debug("index {} has been deleted since removing block started, ignoring", index);
                results.put(index.getName(), new RemoveBlockResult(index, e));
            }
        }

        logger.info("completed removing [index.blocks.{}] block from indices {}", block.name, effectivelyUnblockedIndices);
        return Tuple.tuple(ClusterState.builder(currentState).metadata(metadata).blocks(blocks).build(), List.copyOf(results.values()));
    }

    private class OpenIndicesExecutor implements ClusterStateTaskExecutor<OpenIndicesTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<OpenIndicesTask> batchExecutionContext) {
            var listener = new AllocationActionMultiListener<AcknowledgedResponse>(threadPool.getThreadContext());
            var state = batchExecutionContext.initialState();

            try (var ignored = batchExecutionContext.dropHeadersContext()) {
                // we may encounter deprecated settings but they are not directly related to opening the indices, nor are they really
                // associated with any particular tasks, so we drop them

                // build an in-order de-duplicated array of all the indices to open
                final Set<Index> indicesToOpen = Sets.newLinkedHashSetWithExpectedSize(batchExecutionContext.taskContexts().size());
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    Collections.addAll(indicesToOpen, taskContext.getTask().request.indices());
                }
                Index[] indices = indicesToOpen.toArray(Index.EMPTY_ARRAY);

                // open them
                // NOTE the tasks are batched and the indices can be from different projects
                state = openIndices(indices, state);

                // do a final reroute
                state = allocationService.reroute(state, "indices opened", listener.reroute());

                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    final var task = taskContext.getTask();
                    taskContext.success(task.getAckListener(listener));
                }
            } catch (Exception e) {
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    taskContext.onFailure(e);
                }
            }

            return state;
        }

        private ClusterState openIndices(final Index[] indices, final ClusterState currentState) {
            final List<Tuple<ProjectId, IndexMetadata>> indicesToOpen = new ArrayList<>(indices.length);
            for (Index index : indices) {
                final ProjectMetadata projectMetadata = currentState.metadata().projectFor(index);
                final IndexMetadata indexMetadata = projectMetadata.getIndexSafe(index);
                if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
                    indicesToOpen.add(new Tuple<>(projectMetadata.id(), indexMetadata));
                } else if (currentState.blocks().hasIndexBlockWithId(projectMetadata.id(), index.getName(), INDEX_CLOSED_BLOCK_ID)) {
                    indicesToOpen.add(new Tuple<>(projectMetadata.id(), indexMetadata));
                }
            }

            shardLimitValidator.validateShardLimit(currentState.nodes(), currentState.metadata(), indices);
            if (indicesToOpen.isEmpty()) {
                return currentState;
            }

            logger.info(() -> {
                final StringBuilder indexNames = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    indicesToOpen.stream().map(i -> (CharSequence) i.v2().getIndex().toString()).toList(),
                    ",",
                    512,
                    indexNames
                );
                return "opening indices [" + indexNames + "]";
            });

            final Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder(currentState.blocks());
            final IndexVersion minIndexCompatibilityVersion = currentState.getNodes().getMinSupportedIndexVersion();
            final IndexVersion minReadOnlyIndexCompatibilityVersion = currentState.getNodes().getMinReadOnlySupportedIndexVersion();

            for (var indexToOpen : indicesToOpen) {
                final ProjectId projectId = indexToOpen.v1();
                final IndexMetadata indexMetadata = indexToOpen.v2();
                final Index index = indexMetadata.getIndex();
                if (indexMetadata.getState() != IndexMetadata.State.OPEN) {
                    final Settings.Builder updatedSettings = Settings.builder().put(indexMetadata.getSettings());
                    updatedSettings.remove(VERIFIED_BEFORE_CLOSE_SETTING.getKey());

                    // Reopening a read-only compatible index that has not been marked as read-only is possible if the index was
                    // verified-before-close in the first place.
                    var compatibilityVersion = indexMetadata.getCompatibilityVersion();
                    if (compatibilityVersion.before(minIndexCompatibilityVersion) && hasReadOnlyBlocks(indexMetadata) == false) {
                        if (isIndexVerifiedBeforeClosed(indexMetadata)) {
                            updatedSettings.put(VERIFIED_READ_ONLY_SETTING.getKey(), true);
                            // at least set a write block if the index was verified-before-close at the time the cluster was upgraded
                            blocks.addIndexBlock(projectId, index.getName(), APIBlock.WRITE.block);
                            updatedSettings.put(APIBlock.WRITE.settingName(), true);
                        } // or else, the following indexMetadataVerifier.verifyIndexMetadata() should throw.
                    }

                    IndexMetadata newIndexMetadata = IndexMetadata.builder(indexMetadata)
                        .state(IndexMetadata.State.OPEN)
                        .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                        .settings(updatedSettings)
                        .timestampRange(IndexLongFieldRange.NO_SHARDS)
                        .eventIngestedRange(IndexLongFieldRange.NO_SHARDS)
                        .build();

                    // The index might be closed because we couldn't import it due to an old incompatible
                    // version, so we need to verify its compatibility.
                    newIndexMetadata = indexMetadataVerifier.verifyIndexMetadata(
                        newIndexMetadata,
                        minIndexCompatibilityVersion,
                        minReadOnlyIndexCompatibilityVersion
                    );
                    try {
                        indicesService.verifyIndexMetadata(newIndexMetadata, newIndexMetadata);
                    } catch (Exception e) {
                        throw new ElasticsearchException("Failed to verify index " + index, e);
                    }
                    metadata.getProject(projectId).put(newIndexMetadata, true);
                }

                // Always removes index closed blocks (note: this can fail on-going close index actions)
                blocks.removeIndexBlockWithId(projectId, index.getName(), INDEX_CLOSED_BLOCK_ID);
            }

            ClusterState updatedState = ClusterState.builder(currentState).metadata(metadata).blocks(blocks).build();

            final Map<ProjectId, RoutingTable.Builder> routingTableBuilders = new HashMap<>();
            for (var indexToOpen : indicesToOpen) {
                final ProjectId projectId = indexToOpen.v1();
                final IndexMetadata previousIndexMetadata = indexToOpen.v2();
                if (previousIndexMetadata.getState() != IndexMetadata.State.OPEN) {
                    final RoutingTable.Builder routingTable = routingTableBuilders.computeIfAbsent(
                        projectId,
                        k -> RoutingTable.builder(allocationService.getShardRoutingRoleStrategy(), updatedState.routingTable(k))
                    );
                    routingTable.addAsFromCloseToOpen(
                        updatedState.metadata().getProject(projectId).getIndexSafe(previousIndexMetadata.getIndex())
                    );
                }
            }
            final GlobalRoutingTable.Builder globalRoutingTableBuilder = GlobalRoutingTable.builder(updatedState.globalRoutingTable());
            routingTableBuilders.forEach((globalRoutingTableBuilder::put));
            return ClusterState.builder(updatedState).routingTable(globalRoutingTableBuilder.build()).build();
        }
    }

    private record OpenIndicesTask(OpenIndexClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        public ClusterStateAckListener getAckListener(AllocationActionMultiListener<AcknowledgedResponse> multiListener) {
            return new ClusterStateAckListener() {
                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked() {
                    multiListener.delay(listener).onResponse(AcknowledgedResponse.of(true));
                }

                @Override
                public void onAckFailure(Exception e) {
                    multiListener.delay(listener).onResponse(AcknowledgedResponse.of(false));
                }

                @Override
                public void onAckTimeout() {
                    multiListener.delay(listener).onResponse(AcknowledgedResponse.FALSE);
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            };
        }
    }
}
