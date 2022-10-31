/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    private final RolloverExecutor rolloverTaskExecutor;

    @Inject
    public TransportRolloverAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataRolloverService rolloverService,
        Client client,
        AllocationService allocationService
    ) {
        super(
            RolloverAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RolloverRequest::new,
            indexNameExpressionResolver,
            RolloverResponse::new,
            ThreadPool.Names.SAME
        );
        this.client = client;
        this.rolloverTaskExecutor = new RolloverExecutor(
            allocationService,
            rolloverService,
            new ActiveShardsObserver(clusterService, threadPool)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            true,
            true,
            request.indicesOptions().expandWildcardsOpen(),
            request.indicesOptions().expandWildcardsClosed()
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
        final ClusterState oldState,
        final ActionListener<RolloverResponse> listener
    ) throws Exception {

        assert task instanceof CancellableTask;
        Metadata metadata = oldState.metadata();

        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices(rolloverRequest.getRolloverTarget())
            .clear()
            .indicesOptions(IndicesOptions.fromOptions(true, false, true, true))
            .docs(true);
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        // Rollover can sometimes happen concurrently, to handle these cases, we treat rollover in the same way we would treat a
        // "synchronized" block, in that we have a "before" world, where we calculate naming and condition matching, we then enter our
        // synchronization (in this case, the submitStateUpdateTask which is serialized on the master node), where we then regenerate the
        // names and re-check conditions. More explanation follows inline below.
        client.execute(
            IndicesStatsAction.INSTANCE,
            statsRequest,

            ActionListener.wrap(statsResponse -> {
                // Now that we have the stats for the cluster, we need to know the names of the index for which we should evaluate
                // conditions, as well as what our newly created index *would* be.
                final MetadataRolloverService.NameResolution trialRolloverNames = MetadataRolloverService.resolveRolloverNames(
                    oldState,
                    rolloverRequest.getRolloverTarget(),
                    rolloverRequest.getNewIndexName(),
                    rolloverRequest.getCreateIndexRequest()
                );
                final String trialSourceIndexName = trialRolloverNames.sourceName();
                final String trialRolloverIndexName = trialRolloverNames.rolloverName();

                MetadataRolloverService.validateIndexName(oldState, trialRolloverIndexName);

                // Evaluate the conditions, so that we can tell without a cluster state update whether a rollover would occur.
                final Map<String, Boolean> trialConditionResults = evaluateConditions(
                    rolloverRequest.getConditions().values(),
                    buildStats(metadata.index(trialSourceIndexName), statsResponse)
                );

                final RolloverResponse trialRolloverResponse = new RolloverResponse(
                    trialSourceIndexName,
                    trialRolloverIndexName,
                    trialConditionResults,
                    rolloverRequest.isDryRun(),
                    false,
                    false,
                    false
                );

                // If this is a dry run, return with the results without invoking a cluster state update
                if (rolloverRequest.isDryRun()) {
                    listener.onResponse(trialRolloverResponse);
                    return;
                }

                // Pre-check the conditions to see whether we should submit a new cluster state task
                if (rolloverRequest.areConditionsMet(trialConditionResults)) {
                    String source = "rollover_index source [" + trialRolloverIndexName + "] to target [" + trialRolloverIndexName + "]";
                    RolloverTask rolloverTask = new RolloverTask(rolloverRequest, statsResponse, trialRolloverResponse, listener);
                    ClusterStateTaskConfig config = ClusterStateTaskConfig.build(Priority.NORMAL, rolloverRequest.masterNodeTimeout());
                    clusterService.submitStateUpdateTask(source, rolloverTask, config, rolloverTaskExecutor);
                } else {
                    // conditions not met
                    listener.onResponse(trialRolloverResponse);
                }
            }, listener::onFailure)
        );
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
                .mapToLong(shard -> shard.docs.getTotalSizeInBytes())
                .max()
                .orElse(0);

            final long maxPrimaryShardDocs = indexStats.stream()
                .map(IndexStats::getShards)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .filter(shard -> shard.getShardRouting().primary())
                .map(ShardStats::getStats)
                .mapToLong(shard -> shard.docs.getCount())
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
        ActionListener<RolloverResponse> listener
    ) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    record RolloverExecutor(
        AllocationService allocationService,
        MetadataRolloverService rolloverService,
        ActiveShardsObserver activeShardsObserver
    ) implements ClusterStateTaskExecutor<RolloverTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<RolloverTask> batchExecutionContext) throws Exception {
            final var results = new ArrayList<MetadataRolloverService.RolloverResult>(batchExecutionContext.taskContexts().size());
            var state = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try (var ignored = taskContext.captureResponseHeaders()) {
                    state = executeTask(state, results, taskContext);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }

            if (state != batchExecutionContext.initialState()) {
                var reason = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    (Iterable<String>) () -> results.stream().map(t -> t.sourceIndexName() + "->" + t.rolloverIndexName()).iterator(),
                    ",",
                    "bulk rollover [",
                    "]",
                    1024,
                    reason
                );
                try (var ignored = batchExecutionContext.dropHeadersContext()) {
                    state = allocationService.reroute(state, reason.toString());
                }
            }
            return state;
        }

        public ClusterState executeTask(
            ClusterState currentState,
            List<MetadataRolloverService.RolloverResult> results,
            TaskContext<RolloverTask> rolloverTaskContext
        ) throws Exception {
            final var rolloverTask = rolloverTaskContext.getTask();
            final var rolloverRequest = rolloverTask.rolloverRequest();

            // Regenerate the rollover names, as a rollover could have happened in between the pre-check and the cluster state update
            final var rolloverNames = MetadataRolloverService.resolveRolloverNames(
                currentState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest()
            );

            // Re-evaluate the conditions, now with our final source index name
            final Map<String, Boolean> postConditionResults = evaluateConditions(
                rolloverRequest.getConditions().values(),
                buildStats(currentState.metadata().index(rolloverNames.sourceName()), rolloverTask.statsResponse())
            );

            if (rolloverRequest.areConditionsMet(postConditionResults)) {
                final List<Condition<?>> metConditions = rolloverRequest.getConditions()
                    .values()
                    .stream()
                    .filter(condition -> postConditionResults.get(condition.toString()))
                    .toList();

                // Perform the actual rollover
                final var rolloverResult = rolloverService.rolloverClusterState(
                    currentState,
                    rolloverRequest.getRolloverTarget(),
                    rolloverRequest.getNewIndexName(),
                    rolloverRequest.getCreateIndexRequest(),
                    metConditions,
                    Instant.now(),
                    false,
                    false
                );
                results.add(rolloverResult);
                logger.trace("rollover result [{}]", rolloverResult);

                final var rolloverIndexName = rolloverResult.rolloverIndexName();
                final var sourceIndexName = rolloverResult.sourceIndexName();

                rolloverTaskContext.success(() -> {
                    // Now assuming we have a new state and the name of the rolled over index, we need to wait for the configured number of
                    // active shards, as well as return the names of the indices that were rolled/created
                    activeShardsObserver.waitForActiveShards(
                        new String[] { rolloverIndexName },
                        rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                        rolloverRequest.masterNodeTimeout(),
                        isShardsAcknowledged -> rolloverTask.listener()
                            .onResponse(
                                new RolloverResponse(
                                    // Note that we use the actual rollover result for these, because even though we're single threaded,
                                    // it's possible for the rollover names generated before the actual rollover to be different due to
                                    // things like date resolution
                                    sourceIndexName,
                                    rolloverIndexName,
                                    postConditionResults,
                                    false,
                                    true,
                                    true,
                                    isShardsAcknowledged
                                )
                            ),
                        rolloverTask.listener()::onFailure
                    );
                });

                // Return the new rollover cluster state, which includes the changes that create the new index
                return rolloverResult.clusterState();
            } else {
                // Upon re-evaluation of the conditions, none were met, so therefore do not perform a rollover, returning the current
                // cluster state.
                rolloverTaskContext.success(() -> rolloverTask.listener().onResponse(rolloverTask.trialRolloverResponse()));
                return currentState;
            }
        }
    }
}
