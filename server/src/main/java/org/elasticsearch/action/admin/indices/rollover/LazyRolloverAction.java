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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * API that lazily rolls over a data stream that has the flag {@link DataStream#rolloverOnWrite()} enabled. These requests always
 * originate from requests that write into the data stream.
 */
public final class LazyRolloverAction extends ActionType<RolloverResponse> {

    private static final Logger logger = LogManager.getLogger(LazyRolloverAction.class);

    public static final NodeFeature DATA_STREAM_LAZY_ROLLOVER = new NodeFeature("data_stream.rollover.lazy");

    public static final LazyRolloverAction INSTANCE = new LazyRolloverAction();
    public static final String NAME = "indices:admin/data_stream/lazy_rollover";

    private LazyRolloverAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static final class TransportLazyRolloverAction extends TransportRolloverAction {

        private final MasterServiceTaskQueue<LazyRolloverTask> lazyRolloverTaskQueue;

        @Inject
        public TransportLazyRolloverAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataRolloverService rolloverService,
            AllocationService allocationService,
            MetadataDataStreamsService metadataDataStreamsService,
            DataStreamAutoShardingService dataStreamAutoShardingService,
            Client client
        ) {
            super(
                LazyRolloverAction.INSTANCE,
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
            this.lazyRolloverTaskQueue = clusterService.createTaskQueue(
                "lazy-rollover",
                Priority.NORMAL,
                new LazyRolloverExecutor(clusterService, allocationService, rolloverService, threadPool)
            );
        }

        @Override
        protected void masterOperation(
            Task task,
            RolloverRequest rolloverRequest,
            ClusterState clusterState,
            ActionListener<RolloverResponse> listener
        ) throws Exception {
            assert task instanceof CancellableTask;

            assert rolloverRequest.getConditions().hasConditions() == false
                && rolloverRequest.isDryRun() == false
                && rolloverRequest.isLazy() == false
                : "The auto rollover action does not expect any other parameters in the request apart from the data stream name";

            Metadata metadata = clusterState.metadata();
            DataStream dataStream = metadata.dataStreams().get(rolloverRequest.getRolloverTarget());
            // Skip submitting the task if we detect that the lazy rollover has been already executed.
            if (isLazyRolloverNeeded(dataStream, rolloverRequest.targetsFailureStore()) == false) {
                DataStream.DataStreamIndices targetIndices = dataStream.getDataStreamIndices(rolloverRequest.targetsFailureStore());
                listener.onResponse(noopLazyRolloverResponse(targetIndices));
                return;
            }
            // We evaluate the names of the source index as well as what our newly created index would be.
            final MetadataRolloverService.NameResolution trialRolloverNames = MetadataRolloverService.resolveRolloverNames(
                clusterState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest(),
                rolloverRequest.targetsFailureStore()
            );
            final String trialSourceIndexName = trialRolloverNames.sourceName();
            final String trialRolloverIndexName = trialRolloverNames.rolloverName();
            MetadataRolloverService.validateIndexName(clusterState, trialRolloverIndexName);

            assert metadata.dataStreams().containsKey(rolloverRequest.getRolloverTarget()) : "Auto-rollover applies only to data streams";

            String source = "lazy_rollover source [" + trialSourceIndexName + "] to target [" + trialRolloverIndexName + "]";
            // We create a new rollover request to ensure that it doesn't contain any other parameters apart from the data stream name
            // This will provide a more resilient user experience
            var newRolloverRequest = new RolloverRequest(rolloverRequest.getRolloverTarget(), null);
            newRolloverRequest.setIndicesOptions(rolloverRequest.indicesOptions());
            LazyRolloverTask rolloverTask = new LazyRolloverTask(newRolloverRequest, listener);
            lazyRolloverTaskQueue.submitTask(source, rolloverTask, rolloverRequest.masterNodeTimeout());
        }
    }

    /**
     * A lazy rollover task holds the rollover request and the listener.
     */
    record LazyRolloverTask(RolloverRequest rolloverRequest, ActionListener<RolloverResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Performs a lazy rollover when required and notifies the listener. Due to the nature of the lazy rollover we are able
     * to perform certain optimisations like identifying duplicate requests and executing them once. This is an optimisation
     * that can work since we do not take into consideration any stats or auto-sharding conditions here.
     */
    record LazyRolloverExecutor(
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataRolloverService rolloverService,
        ThreadPool threadPool
    ) implements ClusterStateTaskExecutor<LazyRolloverTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<LazyRolloverTask> batchExecutionContext) {
            final var listener = new AllocationActionMultiListener<RolloverResponse>(threadPool.getThreadContext());
            final var results = new ArrayList<MetadataRolloverService.RolloverResult>(batchExecutionContext.taskContexts().size());
            var state = batchExecutionContext.initialState();
            Map<RolloverRequest, List<TaskContext<LazyRolloverTask>>> groupedRequests = new HashMap<>();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                groupedRequests.computeIfAbsent(taskContext.getTask().rolloverRequest(), ignored -> new ArrayList<>()).add(taskContext);
            }
            for (final var entry : groupedRequests.entrySet()) {
                List<TaskContext<LazyRolloverTask>> rolloverTaskContexts = entry.getValue();
                try {
                    RolloverRequest rolloverRequest = entry.getKey();
                    state = executeTask(state, rolloverRequest, results, rolloverTaskContexts, listener);
                } catch (Exception e) {
                    rolloverTaskContexts.forEach(taskContext -> taskContext.onFailure(e));
                } finally {
                    rolloverTaskContexts.forEach(taskContext -> taskContext.captureResponseHeaders().close());
                }
            }

            if (state != batchExecutionContext.initialState()) {
                var reason = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    (Iterable<String>) () -> Iterators.map(results.iterator(), t -> t.sourceIndexName() + "->" + t.rolloverIndexName()),
                    ",",
                    "lazy bulk rollover [",
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
            RolloverRequest rolloverRequest,
            List<MetadataRolloverService.RolloverResult> results,
            List<TaskContext<LazyRolloverTask>> rolloverTaskContexts,
            AllocationActionMultiListener<RolloverResponse> allocationActionMultiListener
        ) throws Exception {

            // If the data stream has been rolled over since it was marked for lazy rollover, this operation is a noop
            final DataStream dataStream = currentState.metadata().dataStreams().get(rolloverRequest.getRolloverTarget());
            assert dataStream != null;

            if (isLazyRolloverNeeded(dataStream, rolloverRequest.targetsFailureStore()) == false) {
                final DataStream.DataStreamIndices targetIndices = dataStream.getDataStreamIndices(rolloverRequest.targetsFailureStore());
                var noopResponse = noopLazyRolloverResponse(targetIndices);
                notifyAllListeners(rolloverTaskContexts, context -> context.getTask().listener.onResponse(noopResponse));
                return currentState;
            }

            // Perform the actual rollover
            final var rolloverResult = rolloverService.rolloverClusterState(
                currentState,
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                rolloverRequest.getCreateIndexRequest(),
                List.of(),
                Instant.now(),
                false,
                false,
                null,
                null,
                rolloverRequest.targetsFailureStore()
            );
            results.add(rolloverResult);
            logger.trace("lazy rollover result [{}]", rolloverResult);

            final var rolloverIndexName = rolloverResult.rolloverIndexName();
            final var sourceIndexName = rolloverResult.sourceIndexName();

            final var waitForActiveShardsTimeout = rolloverRequest.masterNodeTimeout().millis() < 0
                ? null
                : rolloverRequest.masterNodeTimeout();

            notifyAllListeners(rolloverTaskContexts, context -> {
                // Now assuming we have a new state and the name of the rolled over index, we need to wait for the configured number of
                // active shards, as well as return the names of the indices that were rolled/created
                ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    new String[] { rolloverIndexName },
                    rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                    waitForActiveShardsTimeout,
                    allocationActionMultiListener.delay(context.getTask().listener())
                        .map(
                            isShardsAcknowledged -> new RolloverResponse(
                                // Note that we use the actual rollover result for these, because even though we're single threaded,
                                // it's possible for the rollover names generated before the actual rollover to be different due to
                                // things like date resolution
                                sourceIndexName,
                                rolloverIndexName,
                                Map.of(),
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
        }
    }

    /**
     * A lazy rollover is only needed if the data stream is marked to rollover on write or if it targets the failure store
     * and the failure store is empty.
     */
    private static boolean isLazyRolloverNeeded(DataStream dataStream, boolean failureStore) {
        DataStream.DataStreamIndices indices = dataStream.getDataStreamIndices(failureStore);
        return indices.isRolloverOnWrite() || (failureStore && indices.getIndices().isEmpty());
    }

    private static void notifyAllListeners(
        List<ClusterStateTaskExecutor.TaskContext<LazyRolloverTask>> taskContexts,
        Consumer<ClusterStateTaskExecutor.TaskContext<LazyRolloverTask>> onPublicationSuccess
    ) {
        taskContexts.forEach(context -> context.success(() -> onPublicationSuccess.accept(context)));
    }

    private static RolloverResponse noopLazyRolloverResponse(DataStream.DataStreamIndices indices) {
        String latestWriteIndex = indices.getWriteIndex().getName();
        return new RolloverResponse(latestWriteIndex, latestWriteIndex, Map.of(), false, false, true, true, false);
    }
}
