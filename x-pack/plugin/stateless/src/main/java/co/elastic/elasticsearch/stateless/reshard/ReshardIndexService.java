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

import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardSplittingQuery;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;

import static org.elasticsearch.core.Strings.format;

public class ReshardIndexService {

    private static final Logger logger = LogManager.getLogger(ReshardIndexService.class);

    private final IndicesService indicesService;

    private final MasterServiceTaskQueue<ReshardTask> reshardQueue;
    private final MasterServiceTaskQueue<TransitionToHandoffStateTask> transitionToHandOffStateQueue;
    private final MasterServiceTaskQueue<TransitionTargetStateTask> transitionTargetStateQueue;
    private final MasterServiceTaskQueue<TransitionSourceStateTask> transitionSourceStateQueue;

    public ReshardIndexService(
        final ClusterService clusterService,
        final ShardRoutingRoleStrategy shardRoutingRoleStrategy,
        final RerouteService rerouteService,
        final IndicesService indicesService
    ) {
        this.indicesService = indicesService;

        this.reshardQueue = clusterService.createTaskQueue(
            "reshard-index",
            Priority.NORMAL,
            new ReshardIndexExecutor(shardRoutingRoleStrategy, rerouteService)
        );
        this.transitionToHandOffStateQueue = clusterService.createTaskQueue(
            "transition-reshard-target-state-to-handoff",
            // This is high priority because indexing is blocked while this updated is applied
            // and we would like to unblock it quickly.
            Priority.HIGH,
            new TransitionToHandoffStateExecutor()
        );
        this.transitionTargetStateQueue = clusterService.createTaskQueue(
            "transition-reshard-target-state",
            Priority.NORMAL,
            new TransitionTargetStateExecutor()
        );
        this.transitionSourceStateQueue = clusterService.createTaskQueue(
            "transition-reshard-source-state",
            Priority.NORMAL,
            new TransitionSourceStateExecutor()
        );
    }

    public static ValidationError validateIndex(IndexAbstraction indexAbstraction, IndexMetadata indexMetadata) {
        if (indexAbstraction == null || indexMetadata == null) {
            return ValidationError.INDEX_NOT_FOUND;
        }
        if (indexAbstraction.isSystem()) {
            return ValidationError.SYSTEM_INDEX;
        }
        if (indexAbstraction.getParentDataStream() != null) {
            return ValidationError.DATA_STREAM_INDEX;
        }
        if (indexMetadata.getReshardingMetadata() != null) {
            return ValidationError.ALREADY_RESHARDING;
        }
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            return ValidationError.CLOSED;
        }

        return null;
    }

    public enum ValidationError {
        INDEX_NOT_FOUND,
        SYSTEM_INDEX,
        DATA_STREAM_INDEX,
        ALREADY_RESHARDING,
        CLOSED;

        public RuntimeException intoException(Index index) {
            return switch (this) {
                case INDEX_NOT_FOUND -> new IndexNotFoundException(index);
                case SYSTEM_INDEX -> new IllegalArgumentException("resharding a system index " + index + " is not supported");
                case DATA_STREAM_INDEX -> new IllegalArgumentException(
                    "resharding an index " + index + " that is part of a data stream is not supported"
                );
                case ALREADY_RESHARDING -> new IllegalStateException("an existing resharding operation on " + index + " is unfinished");
                case CLOSED -> new IndexClosedException(index);
            };
        }
    }

    /* When we reshard an index, the target number of shards must be a multiple of the
     * source number of shards as well as a factor of the routingNumShards. This is so that documents from the source shards
     * route to the correct target shards.
     * Look at IndexRouting#hashToShardId to see how we route documents to shards (Math.floorMod(hash, routingNumShards) / routingFactor).
     * So if we have a source index with 2 shards and routingNumShards 1024,
     * we can reshard to an index with 4 shards and routingNumShards 1024 (because 1024 is a multiple of 4).
     * But if we want to reshard to an index with 6 shards, we would have to change the routingNumShards to 768
     * (because 1024 is not a multiple of 6). We cannot change the routingNumShards because if we do that, documents from
     * source shards might move to undesirable shards. In this example of going from 2 -> 6 shards, consider a document
     * whose id hashes to 800.
     * For source IndexMetadata, numShards = 2, routingNumShards = 1024, routing factor = 1024/2 = 512
     * For target IndexMetadata, numShards = 6, routingNumShards = 768, routing factor = 768/6 = 128
     * Now the document with hash 800 routes to shard 1 = (800 % 1024)/ 512 in the source shards
     * But the same document routes to shard 0 = (800 % 768)/ 512 in the target shards
     * We DO NOT want documents moving from shard 1 to shard 0!!
     */
    public static void validateNumTargetShards(int numTargetShards, IndexMetadata sourceIndexMetadata) {
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        IndexMetadata.assertSplitMetadata(numSourceShards, numTargetShards, sourceIndexMetadata);
    }

    public void reshardIndex(
        final TimeValue masterNodeTimeout,
        final ReshardIndexClusterStateUpdateRequest request,
        final ActionListener<ShardsAcknowledgedResponse> listener
    ) {
        logger.trace("reshardIndex[{}]", request);
        onlyReshardIndex(masterNodeTimeout, request, listener.delegateFailureAndWrap((delegate, response) -> {
            logger.trace("[{}] index reshard acknowledged", request.index().getName());
            delegate.onResponse(ShardsAcknowledgedResponse.of(true, true));
        }));
    }

    public void transitionToHandoff(SplitStateRequest splitStateRequest, ActionListener<ActionResponse> listener) {
        // TODO it is up to the caller to figure out how to create ActionResponse, we should not be dealing with it here
        transitionToHandOffStateQueue.submitTask(
            "transition-reshard-index-to-handoff [" + splitStateRequest.getShardId().getIndex().getName() + "]",
            new TransitionToHandoffStateTask(splitStateRequest, listener.map(ignored -> ActionResponse.Empty.INSTANCE)),
            splitStateRequest.masterNodeTimeout()
        );
    }

    public void transitionTargetState(SplitStateRequest splitStateRequest, ActionListener<ActionResponse> listener) {
        // TODO it is up to the caller to figure out how to create ActionResponse, we should not be dealing with it here
        transitionTargetStateQueue.submitTask(
            "transition-reshard-index-target-state [" + splitStateRequest.getShardId().getIndex().getName() + "]",
            new TransitionTargetStateTask(splitStateRequest, listener.map(ignored -> ActionResponse.Empty.INSTANCE)),
            splitStateRequest.masterNodeTimeout()
        );
    }

    public void transitionSourceState(ShardId shardId, IndexReshardingState.Split.SourceShardState state, ActionListener<Void> listener) {
        transitionSourceStateQueue.submitTask(
            "transition-reshard-index-source-state [" + shardId.getIndex().getName() + "]",
            new TransitionSourceStateTask(shardId, state, listener),
            null
        );
    }

    public void deleteUnownedDocuments(ShardId shardId, ActionListener<Void> listener) {
        // may throw if the index is deleted
        var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        // may throw if the shard id is invalid
        var indexShard = indexService.getShard(shardId.id());
        // should validate that shard state is one prior to DONE
        var unownedQuery = new ShardSplittingQuery(
            indexShard.indexSettings().getIndexMetadata(),
            indexShard.shardId().id(),
            indexShard.mapperService().hasNested()
        );

        indexShard.ensureMutable(listener.delegateFailureAndWrap((l, ignored) -> indexShard.withEngine(engine -> {
            ActionListener.run(l, runListener -> {
                assert engine instanceof IndexEngine : engine.getClass().getSimpleName();

                ((IndexEngine) engine).deleteUnownedDocuments(unownedQuery);
                // Ensure that the deletion is flushed to the object store before returning, so that the caller knows that it
                // will not need to retry this and can move a splitting shard to DONE.
                // It would also be fine to just wait for the next flush after delete completes, but assuming we don't split often
                // the cost of this flush should amortize well.
                engine.flush(/* force */ true, /* waitIfOngoing */ true, runListener.map(r -> null));
            });
            return null;
        })), false);
    }

    private void onlyReshardIndex(
        final TimeValue masterNodeTimeout,
        final ReshardIndexClusterStateUpdateRequest request,
        final ActionListener<Void> listener
    ) {
        reshardQueue.submitTask("reshard-index [" + request.index().getName() + "]", new ReshardTask(request, listener), masterNodeTimeout);
    }

    private static void handleTargetPrimaryTermAdvanced(
        long currentTargetPrimaryTerm,
        long startingTargetPrimaryTerm,
        ShardId shardId,
        SplitStateRequest splitStateRequest
    ) {
        String message = format(
            "%s cannot transition target state [%s] because target primary term advanced [%s>%s]",
            shardId,
            splitStateRequest.getNewTargetShardState(),
            startingTargetPrimaryTerm,
            currentTargetPrimaryTerm
        );
        logger.debug(message);
        assert currentTargetPrimaryTerm > startingTargetPrimaryTerm;
        throw new IllegalStateException(message);
    }

    /**
     * Builder to update numberOfShards of an Index.
     * The new shard count must be a multiple of the original shardcount.
     * We do not support shrinking the shard count.
     * @param projectState        Current project state
     * @param reshardingMetadata  Persistent metadata holding resharding state
     * @param index               Index whose shard count is being modified
     * @return project metadata builder for chaining
     */
    public static ProjectMetadata.Builder metadataUpdateNumberOfShards(
        final ProjectState projectState,
        final IndexReshardingMetadata reshardingMetadata,
        final Index index
    ) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());
        IndexMetadata indexMetadata = projectMetadataBuilder.getSafe(index);
        // Note that the IndexMetadata:version is incremented by the put operation
        return projectMetadataBuilder.put(
            IndexMetadata.builder(indexMetadata)
                .reshardingMetadata(reshardingMetadata)
                .reshardAddShards(reshardingMetadata.shardCountAfter())
                // adding shards is a settings change
                .settingsVersion(indexMetadata.getSettingsVersion() + 1)
        );
    }

    /**
     * Builder to remove resharding metadata from an index.
     * @param projectState Current project state
     * @param index        Index to clean
     * @return project metadata builder for chaining
     */
    public static ProjectMetadata.Builder metadataRemoveReshardingState(final ProjectState projectState, final Index index) {
        var projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());
        var indexMetadata = projectMetadataBuilder.getSafe(index);
        return projectMetadataBuilder.put(IndexMetadata.builder(indexMetadata).reshardingMetadata(null));
    }

    /**
     * Update a RoutingTable.Builder to change the number of shards of an existing index
     * <p>
     * Currently only supports split into a multiple of the original shard count.
     * New shards are created unassigned, with their recovery source set to a {@link RecoverySource.ReshardSplitRecoverySource}
     * that contains a reference to the source node for each new shard. We use a special recovery source instead of deriving
     * the source node from cluster state because recovery isn't given a handle on node topology when it runs, and the cluster
     * state it can retrieve from the cluster service may not be up to date with the index metadata available during recovery:
     * recovery runs during application of a new cluster state, before the changes are committed to the service.
     * This approach also mimics peer recovery, and we would like to minimize divergence between recovery implementations
     * as much as we reasonably can.
     * @param routingTableBuilder      A routing table builder to update
     * @param index                    The index to reshard
     * @param reshardingMetadata       Metadata for managing the transition from the original to the new shard count
     * @return the supplied RoutingTable.Builder updated to modify the shard count of the given index
     */
    static RoutingTable.Builder addShardsToRoutingTable(
        final RoutingTable.Builder routingTableBuilder,
        final Index index,
        final IndexReshardingMetadata reshardingMetadata
    ) {
        IndexRoutingTable indexRoutingTable = routingTableBuilder.getIndexRoutingTable(index.getName());
        // TODO: Testing suggests that this is not NULL for a closed index, so when is this NULL ?
        if (indexRoutingTable == null) {
            assert false;
            throw new IllegalStateException("Index [" + index.getName() + "] missing routing table");
        }

        // Replica count
        int currentNumberOfReplicas = indexRoutingTable.shard(0).size() - 1; // remove the required primary
        int oldShardCount = indexRoutingTable.size();
        int newShardCount = reshardingMetadata.getSplit().shardCountAfter();
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
                RecoverySource recoverySource = primary
                    ? new RecoverySource.ReshardSplitRecoverySource(
                        new ShardId(shardId.getIndex(), reshardingMetadata.getSplit().sourceShard(shardId.getId()))
                    )
                    : RecoverySource.PeerRecoverySource.INSTANCE;

                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    primary,
                    recoverySource,
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

    private record ReshardTask(ReshardIndexClusterStateUpdateRequest request, ActionListener<Void> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    // TODO investigate if this executor can be non-acked. There should be no need to wait for all nodes to ack the
    // cluster state update performed here.
    private static class ReshardIndexExecutor extends SimpleBatchedExecutor<ReshardTask, Void> {
        private final ShardRoutingRoleStrategy shardRoutingRoleStrategy;
        private final RerouteService rerouteService;

        private ReshardIndexExecutor(ShardRoutingRoleStrategy shardRoutingRoleStrategy, RerouteService rerouteService) {
            this.shardRoutingRoleStrategy = shardRoutingRoleStrategy;
            this.rerouteService = rerouteService;
        }

        @Override
        public Tuple<ClusterState, Void> executeTask(ReshardTask task, ClusterState clusterState) {
            final ProjectId projectId = task.request.projectId();
            final Index index = task.request.index();

            final ProjectState projectState = clusterState.projectState(projectId);
            final IndexAbstraction indexAbstraction = projectState.metadata().getIndicesLookup().get(index.getName());
            final IndexMetadata sourceMetadata = projectState.metadata().getIndexSafe(index);

            var validationError = validateIndex(indexAbstraction, sourceMetadata);
            if (validationError != null) {
                throw validationError.intoException(index);
            }

            final int sourceNumShards = sourceMetadata.getNumberOfShards();
            final var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(sourceNumShards, task.request.getMultiple());
            // TODO: We should do this validation in TransportReshardAction as well
            validateNumTargetShards(reshardingMetadata.shardCountAfter(), sourceMetadata);

            // TODO: Is it possible that routingTableBuilder and newMetadata are not consistent with each other
            final var routingTableBuilder = addShardsToRoutingTable(
                RoutingTable.builder(shardRoutingRoleStrategy, projectState.routingTable()),
                index,
                reshardingMetadata
            );

            ProjectMetadata projectMetadata = metadataUpdateNumberOfShards(projectState, reshardingMetadata, index).build();
            // TODO: perhaps do not allow updating metadata of a closed index (are there any other conflicting operations ?)
            final ClusterState updated = ClusterState.builder(clusterState)
                .putProjectMetadata(projectMetadata)
                .putRoutingTable(projectId, routingTableBuilder.build())
                .build();
            logger.info("resharding index [{}]", index);

            return new Tuple<>(updated, null);
        }

        @Override
        public void taskSucceeded(ReshardTask task, Void unused) {
            task.listener.onResponse(null);
        }

        @Override
        public void clusterStatePublished() {
            rerouteService.reroute("reroute after starting a resharding operation", Priority.NORMAL, rerouteListener);
        }

        private static final ActionListener<Void> rerouteListener = ActionListener.wrap(
            r -> logger.debug("reroute after resharding completed"),
            e -> logger.warn("reroute after resharding failed", e)
        );
    }

    private record TransitionTargetStateTask(SplitStateRequest splitStateRequest, ActionListener<Void> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class TransitionTargetStateExecutor extends SimpleBatchedExecutor<TransitionTargetStateTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(TransitionTargetStateTask task, ClusterState clusterState) throws Exception {
            final SplitStateRequest splitStateRequest = task.splitStateRequest;
            final ShardId shardId = splitStateRequest.getShardId();
            final Index index = shardId.getIndex();

            final ProjectMetadata project = clusterState.metadata().projectFor(index);
            final ProjectState projectState = clusterState.projectState(project.id());
            final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
            IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
            if (reshardingMetadata == null) {
                throw new IllegalStateException("no existing resharding operation on " + index + ".");
            }

            long currentTargetPrimaryTerm = indexMetadata.primaryTerm(shardId.id());
            long startingTargetPrimaryTerm = splitStateRequest.getTargetPrimaryTerm();
            if (startingTargetPrimaryTerm != currentTargetPrimaryTerm) {
                handleTargetPrimaryTermAdvanced(currentTargetPrimaryTerm, startingTargetPrimaryTerm, shardId, splitStateRequest);
            }

            assert reshardingMetadata.isSplit();
            IndexReshardingState.Split.TargetShardState currentState = reshardingMetadata.getSplit().getTargetShardState(shardId.id());
            IndexReshardingState.Split.TargetShardState targetState = task.splitStateRequest.getNewTargetShardState();

            assert currentState.ordinal() <= targetState.ordinal() : "Skipped state transition of target shard";

            if (currentState == targetState) {
                // This is possible if target shard failed after submitting this state update request.
                // Then during recovery the state transition was not done and a second request to do it was submitted,
                // and we are handling that second request now.
                logger.info(
                    format(
                        "Attempting to advance target shard %s to [%s] but it is already in [%s]. Proceeding.",
                        shardId,
                        targetState,
                        currentState
                    )
                );
                return new Tuple<>(clusterState, null);
            }

            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());

            ProjectMetadata.Builder projectMetadata = projectMetadataBuilder.put(
                IndexMetadata.builder(indexMetadata)
                    .reshardingMetadata(
                        reshardingMetadata.transitionSplitTargetToNewState(shardId, splitStateRequest.getNewTargetShardState())
                    )
            );

            return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(projectMetadata.build()).build(), null);
        }

        @Override
        public void taskSucceeded(TransitionTargetStateTask task, Void unused) {
            task.listener.onResponse(null);
        }
    }

    record TransitionSourceStateTask(ShardId shardId, IndexReshardingState.Split.SourceShardState state, ActionListener<Void> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class TransitionSourceStateExecutor extends SimpleBatchedExecutor<TransitionSourceStateTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(TransitionSourceStateTask task, ClusterState clusterState) throws Exception {
            final ShardId shardId = task.shardId();
            final Index index = shardId.getIndex();
            IndexReshardingState.Split.SourceShardState targetState = task.state;

            final ProjectMetadata project = clusterState.metadata().projectFor(index);
            final ProjectState projectState = clusterState.projectState(project.id());
            final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(index);
            IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();

            if (reshardingMetadata == null) {
                // This is possible if a source shard failed right after sending a request to move to DONE
                // and this is the last shard to move to DONE.
                // It can then recover before source shard state is DONE in cluster state
                // and will initiate an update to DONE again.
                // In the meantime the initial update to DONE it processed and resharding metadata is removed
                // by the cluster state observer waiting for all shards to be DONE.
                assert targetState == IndexReshardingState.Split.SourceShardState.DONE;
                return new Tuple<>(clusterState, null);
            }

            assert reshardingMetadata.isSplit();
            var currentState = reshardingMetadata.getSplit().getSourceShardState(shardId.id());

            assert currentState.ordinal() <= targetState.ordinal() : "Skipped state transition of source shard";

            if (currentState == targetState) {
                // This is possible if a source shard failed after submitting a state update request.
                // Then during recovery the state transition was not done and a second request to do it was submitted,
                // and we are handling that second request now.
                logger.info(
                    format(
                        "Attempting to advance source shard %s to [%s] but it is already in [%s]. Proceeding.",
                        shardId,
                        targetState,
                        currentState
                    )
                );
                return new Tuple<>(clusterState, null);
            }

            if (targetState == IndexReshardingState.Split.SourceShardState.DONE) {
                var split = reshardingMetadata.getSplit();
                long ongoingSourceShards = split.sourceStates()
                    .filter(state -> state != IndexReshardingState.Split.SourceShardState.DONE)
                    .count();
                // Last source shard to finish deletes the resharding metadata
                if (ongoingSourceShards == 1) {
                    assert currentState == IndexReshardingState.Split.SourceShardState.SOURCE;
                    assert split.targetsDone(shardId.id()) : "can only move source shard to DONE when all targets are DONE";
                    var projectMetadata = metadataRemoveReshardingState(projectState, index);
                    return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(projectMetadata).build(), null);
                }
            }

            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());

            ProjectMetadata.Builder projectMetadata = projectMetadataBuilder.put(
                IndexMetadata.builder(indexMetadata)
                    .reshardingMetadata(reshardingMetadata.transitionSplitSourceToNewState(shardId, task.state()))
            );

            return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(projectMetadata.build()).build(), null);
        }

        @Override
        public void taskSucceeded(TransitionSourceStateTask task, Void unused) {
            task.listener.onResponse(null);
        }
    }

    private record TransitionToHandoffStateTask(SplitStateRequest splitStateRequest, ActionListener<Void> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class TransitionToHandoffStateExecutor extends SimpleBatchedExecutor<TransitionToHandoffStateTask, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(TransitionToHandoffStateTask task, ClusterState clusterState) throws Exception {
            final SplitStateRequest splitStateRequest = task.splitStateRequest;
            final ShardId shardId = splitStateRequest.getShardId();
            final Index index = shardId.getIndex();

            final ProjectMetadata project = clusterState.metadata().projectFor(shardId.getIndex());
            final ProjectState projectState = clusterState.projectState(project.id());
            IndexReshardingMetadata reshardingMetadata = projectState.metadata().getIndexSafe(index).getReshardingMetadata();
            if (reshardingMetadata == null) {
                throw new IllegalStateException("no existing resharding operation on " + index + ".");
            }

            ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectState.metadata());
            IndexMetadata indexMetadata = projectMetadataBuilder.getSafe(index);
            long currentTargetPrimaryTerm = indexMetadata.primaryTerm(shardId.id());
            long startingTargetPrimaryTerm = splitStateRequest.getTargetPrimaryTerm();
            long currentSourcePrimaryTerm = indexMetadata.primaryTerm(reshardingMetadata.getSplit().sourceShard(shardId.id()));
            long startingSourcePrimaryTerm = splitStateRequest.getSourcePrimaryTerm();
            if (startingTargetPrimaryTerm != currentTargetPrimaryTerm) {
                handleTargetPrimaryTermAdvanced(currentTargetPrimaryTerm, startingTargetPrimaryTerm, shardId, splitStateRequest);
            } else if (startingSourcePrimaryTerm != currentSourcePrimaryTerm) {
                String message = format(
                    "%s cannot transition target state [%s] because source primary term advanced [%s>%s]",
                    shardId,
                    splitStateRequest.getNewTargetShardState(),
                    currentSourcePrimaryTerm,
                    startingSourcePrimaryTerm
                );
                logger.debug(message);
                assert currentSourcePrimaryTerm > startingSourcePrimaryTerm;
                throw new IllegalStateException(message);
            }

            ProjectMetadata.Builder projectMetadata = projectMetadataBuilder.put(
                IndexMetadata.builder(indexMetadata)
                    .reshardingMetadata(
                        reshardingMetadata.transitionSplitTargetToNewState(shardId, IndexReshardingState.Split.TargetShardState.HANDOFF)
                    )
            );

            return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(projectMetadata).build(), null);
        }

        @Override
        public void taskSucceeded(TransitionToHandoffStateTask task, Void unused) {
            task.listener.onResponse(null);
        }
    }
}
