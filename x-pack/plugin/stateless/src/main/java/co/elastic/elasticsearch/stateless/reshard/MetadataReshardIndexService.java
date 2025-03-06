/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

public class MetadataReshardIndexService {
    private static final Logger logger = LogManager.getLogger(MetadataReshardIndexService.class);

    private final Settings settings;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final AllocationService allocationService;
    private final ThreadPool threadPool;

    public MetadataReshardIndexService(
        final Settings settings,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final AllocationService allocationService,
        final ThreadPool threadPool
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.threadPool = threadPool;
    }

    public static void validateIndexName(String index, Metadata metadata, RoutingTable routingTable) {
        if (routingTable.hasIndex(index) == false) {
            throw new InvalidIndexNameException(index, "index does not exist");
        }
        /* TODO: Throw an error for datastream and system indexes ?
         * Datastream indices are autosharded using a different code path.
         */
    }

    public void reshardIndex(
        final TimeValue masterNodeTimeout,
        final TimeValue ackTimeout,
        @Nullable final TimeValue waitForActiveShardsTimeout,
        final ReshardIndexClusterStateUpdateRequest request,
        final ActionListener<ShardsAcknowledgedResponse> listener
    ) {
        logger.trace("reshardIndex[{}]", request);
        onlyReshardIndex(masterNodeTimeout, ackTimeout, request, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.isAcknowledged()) {
                logger.trace(
                    "[{}] index reshard acknowledged, waiting for active shards [{}]",
                    request.index().getName(),
                    request.waitForActiveShards()
                );
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    request.projectId(),
                    new String[] { request.index().getName() },
                    request.waitForActiveShards(),
                    waitForActiveShardsTimeout,
                    delegate.map(shardsAcknowledged -> {
                        if (shardsAcknowledged == false) {
                            logger.debug(
                                "[{}] index reshard complete, but the operation timed out while waiting for enough shards to be started.",
                                request.index().getName()
                            );
                        } else {
                            logger.trace("[{}] index reshard complete and shards acknowledged", request.index().getName());
                        }
                        return ShardsAcknowledgedResponse.of(true, shardsAcknowledged);
                    })
                );
            } else {
                logger.trace("index reshard not acknowledged for [{}]", request);
                delegate.onResponse(ShardsAcknowledgedResponse.NOT_ACKNOWLEDGED);
            }
        }));
    }

    private void onlyReshardIndex(
        final TimeValue masterNodeTimeout,
        final TimeValue ackTimeout,
        final ReshardIndexClusterStateUpdateRequest request,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        var delegate = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        submitUnbatchedTask(
            "reshard-index [" + request.index().getName() + "]",
            new AckedClusterStateUpdateTask(Priority.URGENT, masterNodeTimeout, ackTimeout, delegate.clusterStateUpdate()) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return applyReshardIndexRequest(currentState, request, false, delegate.reroute());
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        logger.trace(() -> "[" + request.index().getName() + "] failed to autoshard", e);
                    } else {
                        logger.debug(() -> "[" + request.index().getName() + "] failed to autoshard", e);
                    }
                    super.onFailure(e);
                }
            }
        );
    }

    public ClusterState applyReshardIndexRequest(
        ClusterState currentState,
        ReshardIndexClusterStateUpdateRequest request,
        boolean silent,
        final ActionListener<Void> rerouteListener
    ) {
        final ProjectId projectId = request.projectId();
        final Index index = request.index();
        // TODO: Handle Missing (Index might not exist - need to handle for the batched case)
        final ProjectState projectState = currentState.projectState(projectId);
        final IndexMetadata sourceMetadata = projectState.metadata().getIndexSafe(index);
        if (sourceMetadata == null) {
            return currentState;
        }
        int sourceNumShards = sourceMetadata.getNumberOfShards();
        int targetNumShards = sourceNumShards * 2;

        // TODO: Is it possible that routingTableBuilder and newMetadata are not consistent with each other
        var routingTableBuilder = reshardUpdateNumberOfShards(
            projectState,
            allocationService.getShardRoutingRoleStrategy(),
            targetNumShards,
            index
        );

        ProjectMetadata projectMetadata = metadataUpdateNumberOfShards(projectState, targetNumShards, index).build();
        // TODO: perhaps do not allow updating metadata of a closed index (are there any other conflicting operations ?)
        ClusterState updated = ClusterState.builder(currentState)
            .putProjectMetadata(projectMetadata)
            .putRoutingTable(projectId, routingTableBuilder.build())
            .build();
        updated = allocationService.reroute(updated, "index [" + index.getName() + "] resharded", rerouteListener);
        return updated;
    }

    /**
     * Builder to update numberOfShards of an Index.
     * The new shard count must be a multiple of the original shardcount.
     * We do not support shrinking the shard count.
     * @param projectState    Current project state
     * @param numberOfShards  Target number of shards
     * @param index           Index whose shard count is being modified
     * @return
     */
    public static ProjectMetadata.Builder metadataUpdateNumberOfShards(
        final ProjectState projectState,
        final int numberOfShards,
        final Index index
    ) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());
        IndexMetadata indexMetadata = projectMetadataBuilder.getSafe(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        // Note that the IndexMetadata:version is incremented by the put operation
        projectMetadataBuilder.put(
            IndexMetadata.builder(indexMetadata).settingsVersion(indexMetadata.getSettingsVersion() + 1).reshardAddShards(numberOfShards)
        );
        return projectMetadataBuilder;
    }

    public static RoutingTable.Builder reshardUpdateNumberOfShards(
        final ProjectState projectState,
        final ShardRoutingRoleStrategy shardRoutingRoleStrategy,
        final int newShardCount,
        final Index index
    ) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(shardRoutingRoleStrategy, projectState.routingTable());

        IndexRoutingTable indexRoutingTable = routingTableBuilder.getIndexRoutingTable(index.getName());
        // TODO: Testing suggests that this is not NULL for a closed index, so when is this NULL ?
        if (indexRoutingTable == null) {
            assert false;
            throw new IllegalStateException("Index [" + index.getName() + "] missing routing table");
        }

        // Replica count
        int currentNumberOfReplicas = indexRoutingTable.shard(0).size() - 1; // remove the required primary
        int oldShardCount = indexRoutingTable.size();
        assert (newShardCount % oldShardCount == 0) : "New shard count must be multiple of old shard count";
        IndexRoutingTable.Builder builder = new IndexRoutingTable.Builder(
            routingTableBuilder.getShardRoutingRoleStrategy(),
            indexRoutingTable.getIndex()
        );
        builder.ensureShardArray(newShardCount);

        // re-add existing shards
        for (int i = 0; i < oldShardCount; i++) {
            builder.addIndexShard(new IndexShardRoutingTable.Builder(indexRoutingTable.shard(i)));
        }

        int numNewShards = newShardCount - oldShardCount;
        // Add new shards and replicas
        for (int i = 0; i < numNewShards; i++) {
            ShardId shardId = new ShardId(indexRoutingTable.getIndex(), oldShardCount + i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
            for (int j = 0; j <= currentNumberOfReplicas; j++) {
                boolean primary = j == 0;
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    primary,
                    // TODO: Will add a SPLIT recovery type for primary
                    primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.RESHARD_ADDED, null),
                    routingTableBuilder.getShardRoutingRoleStrategy().newEmptyRole(j)
                );
                indexShardRoutingBuilder.addShard(shardRouting);
            }
            builder.addIndexShard(indexShardRoutingBuilder);
        }
        routingTableBuilder.add(builder);
        return routingTableBuilder;
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    // TODO: Batch together reshard tasks only. What if there are 2 reshard requests for the same index
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
