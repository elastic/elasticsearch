/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionMultiListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;

/**
 * API that lazily rolls over a data stream that has the flag {@link DataStream#rolloverOnWrite()} enabled. These requests always
 * originate from requests that write into the data stream.
 */
public final class LazyRolloverAction extends ActionType<RolloverResponse> {

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

        private final MasterServiceTaskQueue<RolloverTask> lazyRolloverTaskQueue;

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
            // We use high priority to not block writes for too long
            this.lazyRolloverTaskQueue = clusterService.createTaskQueue(
                "lazy-rollover",
                Priority.HIGH,
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
            DataStream.DataStreamIndices targetIndices = dataStream.getDataStreamIndices(rolloverRequest.targetsFailureStore());
            // Because of the high priority of the lazy rollover task we choose to skip even adding the task if we detect
            // that the lazy rollover has been already executed.
            if (targetIndices.isRolloverOnWrite() == false) {
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

            final RolloverResponse trialRolloverResponse = new RolloverResponse(
                trialSourceIndexName,
                trialRolloverIndexName,
                Map.of(),
                false,
                false,
                false,
                false,
                false
            );

            String source = "lazy_rollover source [" + trialSourceIndexName + "] to target [" + trialRolloverIndexName + "]";
            // We create a new rollover request to ensure that it doesn't contain any other parameters apart from the data stream name
            // This will provide a more resilient user experience
            var newRolloverRequest = new RolloverRequest(rolloverRequest.getRolloverTarget(), null);
            newRolloverRequest.setIndicesOptions(rolloverRequest.indicesOptions());
            RolloverTask rolloverTask = new RolloverTask(newRolloverRequest, null, trialRolloverResponse, null, listener);
            lazyRolloverTaskQueue.submitTask(source, rolloverTask, rolloverRequest.masterNodeTimeout());
        }
    }

    /**
     * Extends the {@link TransportRolloverAction.RolloverExecutor} and delegates all the execution to it
     * after it confirms that the rollover should be executed. A lazy rollover should be executed
     * iff the rolloverOnWrite flag is true on the requested target. Otherwise, we assume that a different
     * event has triggered the rollover and this one is not necessary anymore.
     */
    static class LazyRolloverExecutor extends TransportRolloverAction.RolloverExecutor {
        LazyRolloverExecutor(
            ClusterService clusterService,
            AllocationService allocationService,
            MetadataRolloverService rolloverService,
            ThreadPool threadPool
        ) {
            super(clusterService, allocationService, rolloverService, threadPool);
        }

        @Override
        public ClusterState executeTask(
            ClusterState currentState,
            List<MetadataRolloverService.RolloverResult> results,
            TaskContext<TransportRolloverAction.RolloverTask> rolloverTaskContext,
            AllocationActionMultiListener<RolloverResponse> allocationActionMultiListener
        ) throws Exception {
            final var rolloverTask = rolloverTaskContext.getTask();
            final var rolloverRequest = rolloverTask.rolloverRequest();

            final IndexAbstraction rolloverTargetAbstraction = currentState.metadata()
                .getIndicesLookup()
                .get(rolloverRequest.getRolloverTarget());
            assert rolloverTargetAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM : "Lazy rollover only applied to data streams";
            final DataStream dataStream = (DataStream) rolloverTargetAbstraction;
            final DataStream.DataStreamIndices targetIndices = dataStream.getDataStreamIndices(rolloverRequest.targetsFailureStore());
            if (targetIndices.isRolloverOnWrite() == false) {
                rolloverTaskContext.success(() -> rolloverTask.listener().onResponse(noopLazyRolloverResponse(targetIndices)));
                return currentState;
            }
            return super.executeTask(currentState, results, rolloverTaskContext, allocationActionMultiListener);
        }
    }

    private static RolloverResponse noopLazyRolloverResponse(DataStream.DataStreamIndices indices) {
        String latestWriteIndex = indices.getWriteIndex().getName();
        return new RolloverResponse(latestWriteIndex, latestWriteIndex, Map.of(), false, false, true, true, false);
    }
}
