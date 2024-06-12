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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action to send info about updated min/max 'event.ingested' range info to master node.
 */
public class UpdateEventIngestedRangeTransportAction extends TransportMasterNodeAction<
    UpdateEventIngestedRangeRequest,
    ActionResponse.Empty> {
    private static final Logger logger = LogManager.getLogger(UpdateEventIngestedRangeTransportAction.class);
    public static final String UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME = "internal:cluster/snapshot/update_event_ingested_range";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(UPDATE_EVENT_INGESTED_RANGE_ACTION_NAME);
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
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.masterServiceTaskQueue = clusterService.createTaskQueue(
            "event-ingested-range-cluster-state-service",
            Priority.NORMAL,
            new TaskExecutor()
        );

        logger.warn("XXX YYY: UpdateEventIngestedRangeAction ctor");
    }

    // MP TODO: why is this method passed ClusterState? what is it allowed to do? Can it update cluster state?
    @Override
    protected void masterOperation(
        Task task,
        UpdateEventIngestedRangeRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) {
        logger.warn("XXX YYY UpdateEventIngestedRangeAction.masterOperation NOW SUBMITTING TASK. Request: {}", request);

        masterServiceTaskQueue.submitTask(
            "update-event-ingested-in-cluster-state",
            new EventIngestedRangeTask(request),
            TimeValue.MAX_VALUE
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateEventIngestedRangeRequest request, ClusterState state) {
        return null;
    }

    // runs on the master node only (called from masterOperation of UpdateEventIngestedRangeAction
    public static class TaskExecutor implements ClusterStateTaskExecutor<EventIngestedRangeTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<EventIngestedRangeTask> batchExecutionContext) throws Exception {
            ClusterState state = batchExecutionContext.initialState();
            final Map<Index, IndexLongFieldRange> updatedEventIngestedRangesMap = new HashMap<>();
            for (var taskContext : batchExecutionContext.taskContexts()) {
                EventIngestedRangeTask task = taskContext.getTask();
                Map<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> rangeMap = task.rangeUpdateRequest()
                    .getEventIngestedRangeMap();

                for (Map.Entry<Index, List<EventIngestedRangeClusterStateService.ShardRangeInfo>> entry : rangeMap.entrySet()) {
                    Index index = entry.getKey();
                    List<EventIngestedRangeClusterStateService.ShardRangeInfo> shardRangeList = entry.getValue();

                    // TODO: why does state.getMetadata().index(index) return null in my tests but
                    // state.getMetadata().index(index.getName()) does not?
                    // TODO: is it realistic to assume that IndexMetadata would never be null here?
                    IndexMetadata indexMetadata = state.getMetadata().index(index.getName());

                    // get the latest EventIngestedRange either from the map outside this loop (first choice) or from cluster state
                    IndexLongFieldRange currentEventIngestedRange = updatedEventIngestedRangesMap.get(index);
                    if (currentEventIngestedRange == null && indexMetadata != null) {
                        currentEventIngestedRange = indexMetadata.getEventIngestedRange();
                    }
                    if (currentEventIngestedRange == IndexLongFieldRange.UNKNOWN) {
                        // UNKNOWN.extendWithShardRange is a no-op, so switch it to NO_SHARDS
                        currentEventIngestedRange = IndexLongFieldRange.NO_SHARDS;
                    }
                    IndexLongFieldRange newEventIngestedRange = currentEventIngestedRange;
                    for (EventIngestedRangeClusterStateService.ShardRangeInfo shardRange : shardRangeList) {
                        newEventIngestedRange = newEventIngestedRange.extendWithShardRange(
                            shardRange.shardId.id(),
                            indexMetadata.getNumberOfShards(),
                            shardRange.eventIngestedRange
                        );
                    }

                    // TODO: or should we use .equals rather than '==' ?? (the .equals method on this class is very strange IMO)
                    if (newEventIngestedRange != currentEventIngestedRange) {
                        updatedEventIngestedRangesMap.put(index, newEventIngestedRange);
                    }
                }
            }
            if (updatedEventIngestedRangesMap.size() > 0) {
                Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
                for (Map.Entry<Index, IndexLongFieldRange> entry : updatedEventIngestedRangesMap.entrySet()) {
                    Index index = entry.getKey();
                    IndexLongFieldRange range = entry.getValue();

                    metadataBuilder.put(IndexMetadata.builder(metadataBuilder.get(index.getName())).eventIngestedRange(range));
                    // TODO: again, builder.getSafe(index)) returns null, but builder.get(index.getName())) does not - why?
                    // metadataBuilder.put(IndexMetadata.builder(metadataBuilder.getSafe(index)).eventIngestedRange(range));
                }

                // MP TODO: Hmm, not sure this should be inside the for loop - it is NOT in ShardStateAction.execute :-(
                // can you just (re)build state like this iteratively and have it work? // TODO: need to have a test for this
                state = ClusterState.builder(state).metadata(metadataBuilder).build();
            }

            for (var taskContext : batchExecutionContext.taskContexts()) {
                // TODO: am I supposed to do something in this success callback?
                taskContext.success(() -> {}); // TODO: need an error handler to call taskContext.onFailure() ??
            }
            return state;
        }

        @Override
        public boolean runOnlyOnMaster() {
            return true;
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            // TODO: need this?
        }

        @Override
        public String describeTasks(List<EventIngestedRangeTask> tasks) {
            // TODO: override this or just use the default?
            return ClusterStateTaskExecutor.super.describeTasks(tasks);
        }
    }

    // TODO: other things to override? ClusterStateAckListener?
    record EventIngestedRangeTask(UpdateEventIngestedRangeRequest rangeUpdateRequest)
        implements
            ClusterStateTaskListener,
            ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            if (e != null) {
                logger.info(
                    "Unable to update event.ingested range in cluster state from index/shard XXX due to error: {}: {}",
                    e.getMessage(),
                    e
                );
            }
        }

        // TODO: do I want to override ClusterStateAckListener? If yes, what do I do with this information?
        // TODO: I don't see that I can do anything useful here - the data nodes with shards are the ones that need to know
        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            // TODO: huh? What node is being passed here? All of them sequentially?
            return false;
        }

        @Override
        public void onAllNodesAcked() {

        }

        @Override
        public void onAckFailure(Exception e) {

        }

        @Override
        public void onAckTimeout() {

        }

        @Override
        public TimeValue ackTimeout() {
            return null;
        }
    }
}
