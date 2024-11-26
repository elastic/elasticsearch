/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingResult;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Main class to swap the index pointed to by an alias, given some conditions
 */
public class TransportRolloverAction extends TransportMasterNodeAction<RolloverRequest, RolloverResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRolloverAction.class);

    private final Client client;
    private final MasterServiceTaskQueue<RolloverTask> rolloverTaskQueue;
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final DataStreamAutoShardingService dataStreamAutoShardingService;

    @Inject
    public TransportRolloverAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataRolloverService rolloverService,
        Client client,
        AllocationService allocationService,
        MetadataDataStreamsService metadataDataStreamsService,
        DataStreamAutoShardingService dataStreamAutoShardingService
    ) {
        this(
            RolloverAction.INSTANCE,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            rolloverService,
            client,
            allocationService,
            metadataDataStreamsService,
            dataStreamAutoShardingService
        );
    }

    TransportRolloverAction(
        ActionType<RolloverResponse> actionType,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataRolloverService rolloverService,
        Client client,
        AllocationService allocationService,
        MetadataDataStreamsService metadataDataStreamsService,
        DataStreamAutoShardingService dataStreamAutoShardingService
    ) {
        super(
            actionType.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RolloverRequest::new,
            indexNameExpressionResolver,
            RolloverResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.rolloverTaskQueue = clusterService.createTaskQueue(
            "rollover",
            Priority.NORMAL,
            new RolloverExecutor(clusterService, allocationService, rolloverService, threadPool)
        );
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.dataStreamAutoShardingService = dataStreamAutoShardingService;
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        final var indicesOptions = new IndicesOptions(
            IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS,
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(request.indicesOptions().expandWildcardsOpen())
                .matchClosed(request.indicesOptions().expandWildcardsClosed())
                .build(),
            IndicesOptions.GatekeeperOptions.DEFAULT,
            request.indicesOptions().selectorOptions()
        );

        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request)
            );
    }

    @Override
    protected void masterOperation(
        Task task,
        final RolloverRequest rolloverRequest,
        final ClusterState clusterState,
        final ActionListener<RolloverResponse> listener
    ) throws Exception {

        assert task instanceof CancellableTask;
        Metadata metadata = clusterState.metadata();
        // We evaluate the names of the index for which we should evaluate conditions, as well as what our newly created index *would* be.
        boolean targetFailureStore = rolloverRequest.targetsFailureStore();
        final MetadataRolloverService.NameResolution trialRolloverNames = MetadataRolloverService.resolveRolloverNames(
            clusterState,
            rolloverRequest.getRolloverTarget(),
            rolloverRequest.getNewIndexName(),
            rolloverRequest.getCreateIndexRequest(),
            targetFailureStore
        );
        final String trialSourceIndexName = trialRolloverNames.sourceName();
        final String trialRolloverIndexName = trialRolloverNames.rolloverName();
        MetadataCreateIndexService.validateIndexName(trialRolloverIndexName, metadata, clusterState.routingTable());

        boolean isDataStream = metadata.dataStreams().containsKey(rolloverRequest.getRolloverTarget());
        if (rolloverRequest.isLazy()) {
            if (isDataStream == false || rolloverRequest.getConditions().hasConditions()) {
                String message;
                if (isDataStream) {
                    message = "Lazy rollover can be used only without any conditions."
                        + " Please remove the conditions from the request body or the query parameter 'lazy'.";
                } else if (rolloverRequest.getConditions().hasConditions() == false) {
                    message = "Lazy rollover can be applied only on a data stream." + " Please remove the query parameter 'lazy'.";
                } else {
                    message = "Lazy rollover can be applied only on a data stream with no conditions."
                        + " Please remove the query parameter 'lazy'.";
                }
                listener.onFailure(new IllegalArgumentException(message));
                return;
            }
            if (rolloverRequest.isDryRun() == false) {
                metadataDataStreamsService.setRolloverOnWrite(
                    rolloverRequest.getRolloverTarget(),
                    true,
                    targetFailureStore,
                    rolloverRequest.ackTimeout(),
                    rolloverRequest.masterNodeTimeout(),
                    listener.map(
                        response -> new RolloverResponse(
                            trialSourceIndexName,
                            trialRolloverIndexName,
                            Map.of(),
                            false,
                            false,
                            response.isAcknowledged(),
                            false,
                            response.isAcknowledged()
                        )
                    )
                );
                return;
            }
        }

        final IndexAbstraction rolloverTargetAbstraction = clusterState.metadata()
            .getIndicesLookup()
            .get(rolloverRequest.getRolloverTarget());
        if (rolloverTargetAbstraction.getType() == IndexAbstraction.Type.ALIAS && rolloverTargetAbstraction.isDataStreamRelated()) {
            listener.onFailure(
                new IllegalStateException("Aliases to data streams cannot be rolled over. Please rollover the data stream itself.")
            );
            return;
        }
        if (targetFailureStore && rolloverTargetAbstraction.isDataStreamRelated() == false) {
            listener.onFailure(new IllegalStateException("Rolling over failure stores is only possible on data streams."));
            return;
        }

        // When we're initializing a failure store, we skip the stats request because there is no source index to retrieve stats for.
        if (targetFailureStore && ((DataStream) rolloverTargetAbstraction).getFailureIndices().getIndices().isEmpty()) {
            initializeFailureStore(rolloverRequest, listener, trialSourceIndexName, trialRolloverIndexName);
            return;
        }

        final var statsIndicesOptions = new IndicesOptions(
            IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS,
            IndicesOptions.WildcardOptions.builder().matchClosed(true).allowEmptyExpressions(false).build(),
            IndicesOptions.GatekeeperOptions.DEFAULT,
            rolloverRequest.indicesOptions().selectorOptions()
        );
        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices(rolloverRequest.getRolloverTarget())
            .clear()
            .indicesOptions(statsIndicesOptions)
            .docs(true)
            .indexing(true);
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        // Rollover can sometimes happen concurrently, to handle these cases, we treat rollover in the same way we would treat a
        // "synchronized" block, in that we have a "before" world, where we calculate naming and condition matching, we then enter our
        // synchronization (in this case, the submitStateUpdateTask which is serialized on the master node), where we then regenerate the
        // names and re-check conditions. More explanation follows inline below.
        client.execute(
            IndicesStatsAction.INSTANCE,
            statsRequest,

            listener.delegateFailureAndWrap((delegate, statsResponse) -> {

                AutoShardingResult rolloverAutoSharding = null;
                final IndexAbstraction indexAbstraction = clusterState.metadata()
                    .getIndicesLookup()
                    .get(rolloverRequest.getRolloverTarget());
                if (indexAbstraction.getType().equals(IndexAbstraction.Type.DATA_STREAM)) {
                    DataStream dataStream = (DataStream) indexAbstraction;
                    final Optional<IndexStats> indexStats = Optional.ofNullable(statsResponse)
                        .map(stats -> stats.getIndex(dataStream.getWriteIndex().getName()));

                    Double indexWriteLoad = indexStats.map(
                        stats -> Arrays.stream(stats.getShards())
                            .filter(shardStats -> shardStats.getStats().indexing != null)
                            // only take primaries into account as in stateful the replicas also index data
                            .filter(shardStats -> shardStats.getShardRouting().primary())
                            .map(shardStats -> shardStats.getStats().indexing.getTotal().getWriteLoad())
                            .reduce(0.0, Double::sum)
                    ).orElse(null);

                    rolloverAutoSharding = dataStreamAutoShardingService.calculate(clusterState, dataStream, indexWriteLoad);
                    logger.debug("auto sharding result for data stream [{}] is [{}]", dataStream.getName(), rolloverAutoSharding);

                    // if auto sharding recommends increasing the number of shards we want to trigger a rollover even if there are no
                    // other "regular" conditions matching (we want to aggressively increse the number of shards) so we're adding the
                    // automatic {@link OptimalShardCountCondition} to the rollover request conditions so it gets evaluated and triggers
                    // the rollover operation (having this condition met will also provide a useful paper trail as it'll get stored in
                    // the {@link org.elasticsearch.action.admin.indices.rollover.RolloverInfo#metConditions} )

                    // NOTE that the {@link AutoShardingType#DECREASE_SHARDS} recommendation is treated differently (i.e. added to the
                    // conditions later only if other "regular" rollover conditions match: see {@link RolloverTask#executeTask}) because we
                    // do NOT want to trigger a rollover **just** to reduce the number of shards, but we will reduce the number of shards
                    // when the rollover will naturally occur.
                    if (rolloverAutoSharding.type().equals(AutoShardingType.INCREASE_SHARDS)) {
                        RolloverConditions conditionsIncludingImplicit = RolloverConditions.newBuilder(rolloverRequest.getConditions())
                            .addOptimalShardCountCondition(rolloverAutoSharding)
                            .build();
                        rolloverRequest.setConditions(conditionsIncludingImplicit);
                    }
                }

                // Evaluate the conditions, so that we can tell without a cluster state update whether a rollover would occur.
                final Map<String, Boolean> trialConditionResults = evaluateConditions(
                    rolloverRequest.getConditionValues(),
                    buildStats(metadata.index(trialSourceIndexName), statsResponse)
                );

                final RolloverResponse trialRolloverResponse = new RolloverResponse(
                    trialSourceIndexName,
                    trialRolloverIndexName,
                    trialConditionResults,
                    rolloverRequest.isDryRun(),
                    false,
                    false,
                    false,
                    rolloverRequest.isLazy()
                );

                // If this is a dry run, return with the results without invoking a cluster state update
                if (rolloverRequest.isDryRun()) {
                    delegate.onResponse(trialRolloverResponse);
                    return;
                }

                // Pre-check the conditions to see whether we should submit a new cluster state task
                if (rolloverRequest.areConditionsMet(trialConditionResults)) {
                    String source = "rollover_index source [" + trialSourceIndexName + "] to target [" + trialRolloverIndexName + "]";
                    RolloverTask rolloverTask = new RolloverTask(
                        rolloverRequest,
                        statsResponse,
                        trialRolloverResponse,
                        rolloverAutoSharding,
                        delegate
                    );
                    rolloverTaskQueue.submitTask(source, rolloverTask, rolloverRequest.masterNodeTimeout());
                } else {
                    // conditions not met
                    delegate.onResponse(trialRolloverResponse);
                }
            })
        );
    }

    private void initializeFailureStore(
        RolloverRequest rolloverRequest,
        ActionListener<RolloverResponse> listener,
        String trialSourceIndexName,
        String trialRolloverIndexName
    ) {
        if (rolloverRequest.getConditionValues().isEmpty() == false) {
            listener.onFailure(
                new IllegalStateException("Rolling over/initializing an empty failure store is only supported without conditions.")
            );
            return;
        }
        final RolloverResponse trialRolloverResponse = new RolloverResponse(
            trialSourceIndexName,
            trialRolloverIndexName,
            Map.of(),
            rolloverRequest.isDryRun(),
            false,
            false,
            false,
            rolloverRequest.isLazy()
        );

        // If this is a dry run, return with the results without invoking a cluster state update.
        if (rolloverRequest.isDryRun()) {
            listener.onResponse(trialRolloverResponse);
            return;
        }

        String source = "initialize_failure_store with index [" + trialRolloverIndexName + "]";
        RolloverTask rolloverTask = new RolloverTask(rolloverRequest, null, trialRolloverResponse, null, listener);
        rolloverTaskQueue.submitTask(source, rolloverTask, rolloverRequest.masterNodeTimeout());
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions, @Nullable final Condition.Stats stats) {
        Objects.requireNonNull(conditions, "conditions must not be null");

        if (stats != null) {
            return conditions.stream()
                .map(condition -> condition.evaluate(stats))
                .collect(Collectors.toMap(result -> result.condition().toString(), Condition.Result::matched));
        } else {
            // no conditions matched
            return conditions.stream().collect(Collectors.toMap(Condition::toString, cond -> false));
        }
    }

    static Condition.Stats buildStats(@Nullable final IndexMetadata metadata, @Nullable final IndicesStatsResponse statsResponse) {
        if (metadata == null) {
            return null;
        } else {
            final Optional<IndexStats> indexStats = Optional.ofNullable(statsResponse)
                .map(stats -> stats.getIndex(metadata.getIndex().getName()));

            final DocsStats docsStats = indexStats.map(stats -> stats.getPrimaries().getDocs()).orElse(null);

            final long maxPrimaryShardSize = indexStats.stream()
                .map(IndexStats::getShards)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .filter(shard -> shard.getShardRouting().primary())
                .map(ShardStats::getStats)
                .mapToLong(shard -> shard.docs == null ? 0L : shard.docs.getTotalSizeInBytes())
                .max()
                .orElse(0);

            final long maxPrimaryShardDocs = indexStats.stream()
                .map(IndexStats::getShards)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .filter(shard -> shard.getShardRouting().primary())
                .map(ShardStats::getStats)
                .mapToLong(shard -> shard.docs == null ? 0L : shard.docs.getCount())
                .max()
                .orElse(0);

            return new Condition.Stats(
                docsStats == null ? 0 : docsStats.getCount(),
                metadata.getCreationDate(),
                ByteSizeValue.ofBytes(docsStats == null ? 0 : docsStats.getTotalSizeInBytes()),
                ByteSizeValue.ofBytes(maxPrimaryShardSize),
                maxPrimaryShardDocs
            );
        }
    }

    record RolloverTask(
        RolloverRequest rolloverRequest,
        IndicesStatsResponse statsResponse,
        RolloverResponse trialRolloverResponse,
        @Nullable AutoShardingResult autoShardingResult,
        ActionListener<RolloverResponse> listener
    ) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    record RolloverExecutor(
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataRolloverService rolloverService,
        ThreadPool threadPool
    ) implements ClusterStateTaskExecutor<RolloverTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<RolloverTask> batchExecutionContext) {
            final var listener = new AllocationActionMultiListener<RolloverResponse>(threadPool.getThreadContext());
            final var results = new ArrayList<MetadataRolloverService.RolloverResult>(batchExecutionContext.taskContexts().size());
            var state = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try (var ignored = taskContext.captureResponseHeaders()) {
                    state = executeTask(state, results, taskContext, listener);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            if (state != batchExecutionContext.initialState()) {
                var reason = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    (Iterable<String>) () -> Iterators.map(results.iterator(), t -> t.sourceIndexName() + "->" + t.rolloverIndexName()),
                    ",",
                    "bulk rollover [",
                    "]",
                    1024,
                    reason
                );
                try (var ignored = batchExecutionContext.dropHeadersContext()) {
                    state = allocationService.reroute(state, reason.toString(), listener.reroute());
                }
            } else {
                listener.noRerouteNeeded();
            }
            return state;
        }

        public ClusterState executeTask(
            ClusterState currentState,
            List<MetadataRolloverService.RolloverResult> results,
            TaskContext<RolloverTask> rolloverTaskContext,
            AllocationActionMultiListener<RolloverResponse> allocationActionMultiListener
        ) throws Exception {
            final var rolloverTask = rolloverTaskContext.getTask();
            final var rolloverRequest = rolloverTask.rolloverRequest();

            // Regenerate the rollover names, as a rollover could have happened in between the pre-check and the cluster state update
            final var rolloverNames = MetadataRolloverService.resolveRolloverNames(
                currentState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest(),
                rolloverRequest.targetsFailureStore()
            );

            // Re-evaluate the conditions, now with our final source index name
            IndexMetadata rolloverSourceIndex = currentState.metadata().index(rolloverNames.sourceName());
            final Map<String, Boolean> postConditionResults = evaluateConditions(
                rolloverRequest.getConditionValues(),
                buildStats(rolloverSourceIndex, rolloverTask.statsResponse())
            );

            if (rolloverRequest.getConditions().areConditionsMet(postConditionResults)) {
                Map<String, Boolean> resultsIncludingDecreaseShards = new HashMap<>(postConditionResults);
                if (rolloverTask.autoShardingResult != null
                    && rolloverTask.autoShardingResult.type().equals(AutoShardingType.DECREASE_SHARDS)) {
                    // if we're executing a rollover ("regular" conditions are met) and we're also decreasing the number of shards we'll
                    // include the decrease_shards optimal shard count condition in the response and {@link RolloverInfo#metConditions}
                    RolloverConditions conditionsIncludingDecreaseShards = RolloverConditions.newBuilder(rolloverRequest.getConditions())
                        .addOptimalShardCountCondition(rolloverTask.autoShardingResult)
                        .build();
                    rolloverRequest.setConditions(conditionsIncludingDecreaseShards);
                    resultsIncludingDecreaseShards.put(
                        new OptimalShardCountCondition(rolloverTask.autoShardingResult.targetNumberOfShards()).toString(),
                        true
                    );
                }

                final List<Condition<?>> metConditions = rolloverRequest.getConditionValues()
                    .stream()
                    .filter(condition -> resultsIncludingDecreaseShards.get(condition.toString()))
                    .toList();

                final IndexAbstraction rolloverTargetAbstraction = currentState.metadata()
                    .getIndicesLookup()
                    .get(rolloverRequest.getRolloverTarget());

                final IndexMetadataStats sourceIndexStats = rolloverTargetAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM
                    ? IndexMetadataStats.fromStatsResponse(rolloverSourceIndex, rolloverTask.statsResponse())
                    : null;

                // Perform the actual rollover
                final var rolloverResult = rolloverService.rolloverClusterState(
                    currentState,
                    rolloverRequest.getRolloverTarget(),
                    rolloverRequest.getNewIndexName(),
                    rolloverRequest.getCreateIndexRequest(),
                    metConditions,
                    Instant.now(),
                    false,
                    false,
                    sourceIndexStats,
                    rolloverTask.autoShardingResult(),
                    rolloverRequest.targetsFailureStore()
                );
                results.add(rolloverResult);
                logger.trace("rollover result [{}]", rolloverResult);

                final var rolloverIndexName = rolloverResult.rolloverIndexName();
                final var sourceIndexName = rolloverResult.sourceIndexName();

                final var waitForActiveShardsTimeout = rolloverRequest.masterNodeTimeout().millis() < 0
                    ? null
                    : rolloverRequest.masterNodeTimeout();

                rolloverTaskContext.success(() -> {
                    // Now assuming we have a new state and the name of the rolled over index, we need to wait for the configured number of
                    // active shards, as well as return the names of the indices that were rolled/created
                    ActiveShardsObserver.waitForActiveShards(
                        clusterService,
                        new String[] { rolloverIndexName },
                        rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                        waitForActiveShardsTimeout,
                        allocationActionMultiListener.delay(rolloverTask.listener())
                            .map(
                                isShardsAcknowledged -> new RolloverResponse(
                                    // Note that we use the actual rollover result for these, because even though we're single threaded,
                                    // it's possible for the rollover names generated before the actual rollover to be different due to
                                    // things like date resolution
                                    sourceIndexName,
                                    rolloverIndexName,
                                    resultsIncludingDecreaseShards,
                                    false,
                                    true,
                                    true,
                                    isShardsAcknowledged,
                                    false
                                )
                            )
                    );
                });

                // Return the new rollover cluster state, which includes the changes that create the new index
                return rolloverResult.clusterState();
            } else {
                // Upon re-evaluation of the conditions, none were met, so do not perform a rollover, returning the current
                // cluster state.
                rolloverTaskContext.success(() -> rolloverTask.listener().onResponse(rolloverTask.trialRolloverResponse()));
                return currentState;
            }
        }
    }
}
