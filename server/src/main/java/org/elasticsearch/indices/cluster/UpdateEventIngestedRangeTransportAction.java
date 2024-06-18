/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;

/**
 * Transport action to send info about updated min/max 'event.ingested' range info to master node.
 */
// TODO: impl AcknowledgedTransportMasterNodeAction instead?
public class UpdateEventIngestedRangeTransportAction extends TransportMasterNodeAction<
    UpdateEventIngestedRangeRequest,
    AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(UpdateEventIngestedRangeTransportAction.class);
    public static final String UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME = "internal:cluster/snapshot/update_event_ingested_range";
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>(UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME);
    private final MasterServiceTaskQueue<EventIngestedRangeTask> masterServiceTaskQueue;

    @Inject
    public UpdateEventIngestedRangeTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateEventIngestedRangeRequest::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.masterServiceTaskQueue = clusterService.createTaskQueue(
            "event-ingested-range-cluster-state-service",
            Priority.NORMAL,
            EVENT_INGESTED_UPDATE_TASK_EXECUTOR
        );

        logger.warn("XXX YYY: UpdateEventIngestedRangeAction ctor");
    }

    // MP TODO: why is this method passed ClusterState? what is it allowed to do? Can it update cluster state?
    @Override
    protected void masterOperation(
        Task task,
        UpdateEventIngestedRangeRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> responseListener
    ) {
        logger.warn("XXX YYY UpdateEventIngestedRangeAction.masterOperation NOW SUBMITTING TASK. Request: {}", request);

        // TODO: is this the better way to do it rather than how I had it below (now commented out)?
        ActionListener.run(
            responseListener,
            listener -> masterServiceTaskQueue.submitTask(
                "update-event-ingested-in-cluster-state",
                new EventIngestedRangeTask(request, listener),
                TimeValue.MAX_VALUE
            )
        );

        // masterServiceTaskQueue.submitTask(
        // "update-event-ingested-in-cluster-state",
        // new EventIngestedRangeTask(request, listener),
        // TimeValue.MAX_VALUE
        // );
        // // the ActionListener will get called by the task when it has completed or failed, so no need to call it here
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateEventIngestedRangeRequest request, ClusterState state) {
        return null;
    }

    record EventIngestedRangeTask(UpdateEventIngestedRangeRequest rangeUpdateRequest, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            if (e != null) {
                logger.info(
                    "Unable to update event.ingested range in cluster state from index/shard XXX due to error: {}: {}",
                    e.getMessage(),
                    e
                );
            }
            listener.onFailure(e);  // TODO: want this inside the if-null check?
        }
    }

    static final SimpleBatchedExecutor<EventIngestedRangeTask, Void> EVENT_INGESTED_UPDATE_TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
        @Override
        public Tuple<ClusterState, Void> executeTask(EventIngestedRangeTask rangeTask, ClusterState clusterState) throws Exception {

            ClusterState state = clusterState;
            var rangeMap = rangeTask.rangeUpdateRequest().getEventIngestedRangeMap();
            for (Map.Entry<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> entry : rangeMap.entrySet()) {
                Index index = entry.getKey();
                List<EventIngestedRangeClusterStateService.ShardRangeInfo> shardRangeList = entry.getValue();

                // TODO: is it realistic to assume that IndexMetadata would never be null here?
                IndexMetadata indexMetadata = state.getMetadata().index(index);

                IndexLongFieldRange currentEventIngestedRange = indexMetadata.getEventIngestedRange();
                if (currentEventIngestedRange == IndexLongFieldRange.UNKNOWN) {
                    // UNKNOWN.extendWithShardRange is a no-op, so switch it to NO_SHARDS
                    currentEventIngestedRange = IndexLongFieldRange.NO_SHARDS;
                }
                IndexLongFieldRange newRange = currentEventIngestedRange;
                for (EventIngestedRangeClusterStateService.ShardRangeInfo shardRange : shardRangeList) {
                    newRange = newRange.extendWithShardRange(
                        shardRange.shardId.id(),
                        indexMetadata.getNumberOfShards(),
                        shardRange.eventIngestedRange
                    );
                }
                if (newRange != currentEventIngestedRange) {
                    Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
                    metadataBuilder.put(IndexMetadata.builder(metadataBuilder.getSafe(index)).eventIngestedRange(newRange));
                    state = ClusterState.builder(state).metadata(metadataBuilder).build();
                }
            }
            return Tuple.tuple(state, null);
        }

        @Override
        public void taskSucceeded(EventIngestedRangeTask rangeTask, Void unused) {
            rangeTask.listener().onResponse(AcknowledgedResponse.TRUE);
        }
    };
}
