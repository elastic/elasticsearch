/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.RetryActionRequest;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate.getReindexRequiredPredicate;

public class ReindexDataStreamPersistentTaskExecutor extends PersistentTasksExecutor<ReindexDataStreamTaskParams> {
    /*
     * This setting controls how many indices we reindex concurrently for a single data stream. This is not an overall limit -- if five
     * data streams are being reindexed, then each of them can have this many indices being reindexed at once. This setting is dynamic,
     * but changing it does not have an impact if the task is already running (unless the task is restarted or moves to another node).
     */
    public static final Setting<Integer> MAX_CONCURRENT_INDICES_REINDEXED_PER_DATA_STREAM_SETTING = Setting.intSetting(
        "migrate.max_concurrent_indices_reindexed_per_data_stream",
        1,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(ReindexDataStreamPersistentTaskExecutor.class);
    private static final TimeValue TASK_KEEP_ALIVE_TIME = TimeValue.timeValueDays(1);
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public ReindexDataStreamPersistentTaskExecutor(Client client, ClusterService clusterService, String taskName, ThreadPool threadPool) {
        super(taskName, threadPool.generic());
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    @Override
    protected ReindexDataStreamTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<ReindexDataStreamTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        ReindexDataStreamTaskParams params = taskInProgress.getParams();
        return new ReindexDataStreamTask(
            clusterService,
            params.startTime(),
            params.totalIndices(),
            params.totalIndicesToBeUpgraded(),
            id,
            type,
            action,
            "Reindexing data stream " + taskInProgress.getParams().getSourceDataStream(),
            parentTaskId,
            headers
        );
    }

    @Override
    protected void nodeOperation(
        AllocatedPersistentTask task,
        ReindexDataStreamTaskParams params,
        PersistentTaskState persistentTaskState
    ) {
        Long completionTime = getCompletionTime(persistentTaskState);
        if (completionTime != null && task instanceof ReindexDataStreamTask reindexDataStreamTask) {
            reindexDataStreamTask.allReindexesCompleted(threadPool, getTimeToLive(completionTime));
            return;
        }
        ReindexDataStreamPersistentTaskState state = (ReindexDataStreamPersistentTaskState) persistentTaskState;
        String sourceDataStream = params.getSourceDataStream();
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        GetDataStreamAction.Request request = new GetDataStreamAction.Request(TimeValue.MAX_VALUE, new String[] { sourceDataStream });
        request.setParentTask(taskId);
        assert task instanceof ReindexDataStreamTask;
        final ReindexDataStreamTask reindexDataStreamTask = (ReindexDataStreamTask) task;
        client.execute(GetDataStreamAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = response.getDataStreams();
            if (dataStreamInfos.size() == 1) {
                DataStream dataStream = dataStreamInfos.getFirst().getDataStream();
                boolean includeSystem = dataStream.isSystem();
                if (getReindexRequiredPredicate(clusterService.state().metadata().getProject(), false, includeSystem).test(
                    dataStream.getWriteIndex()
                )) {
                    RolloverRequest rolloverRequest = new RolloverRequest(sourceDataStream, null);
                    rolloverRequest.setParentTask(taskId);
                    client.execute(
                        RolloverAction.INSTANCE,
                        rolloverRequest,
                        ActionListener.wrap(
                            rolloverResponse -> reindexIndices(
                                dataStream,
                                dataStream.getIndices().size() + 1,
                                reindexDataStreamTask,
                                params,
                                state,
                                sourceDataStream,
                                taskId
                            ),
                            e -> completeFailedPersistentTask(reindexDataStreamTask, state, e)
                        )
                    );
                } else {
                    reindexIndices(
                        dataStream,
                        dataStream.getIndices().size(),
                        reindexDataStreamTask,
                        params,
                        state,
                        sourceDataStream,
                        taskId
                    );
                }
            } else {
                completeFailedPersistentTask(reindexDataStreamTask, state, new ElasticsearchException("data stream does not exist"));
            }
        }, exception -> completeFailedPersistentTask(reindexDataStreamTask, state, exception)));
    }

    private void reindexIndices(
        DataStream dataStream,
        int totalIndicesInDataStream,
        ReindexDataStreamTask reindexDataStreamTask,
        ReindexDataStreamTaskParams params,
        ReindexDataStreamPersistentTaskState state,
        String sourceDataStream,
        TaskId parentTaskId
    ) {
        List<Index> indices = dataStream.getIndices();
        List<Index> indicesToBeReindexed = indices.stream()
            .filter(getReindexRequiredPredicate(clusterService.state().metadata().getProject(), false, dataStream.isSystem()))
            .toList();
        final ReindexDataStreamPersistentTaskState updatedState;
        if (params.totalIndices() != totalIndicesInDataStream
            || params.totalIndicesToBeUpgraded() != indicesToBeReindexed.size()
            || (state != null
                && (state.totalIndices() != null
                    && state.totalIndicesToBeUpgraded() != null
                    && (state.totalIndices() != totalIndicesInDataStream
                        || state.totalIndicesToBeUpgraded() != indicesToBeReindexed.size())))) {
            updatedState = new ReindexDataStreamPersistentTaskState(
                totalIndicesInDataStream,
                indicesToBeReindexed.size(),
                state == null ? null : state.completionTime()
            );
            reindexDataStreamTask.updatePersistentTaskState(updatedState, ActionListener.noop());
        } else {
            updatedState = state;
        }
        reindexDataStreamTask.setPendingIndicesCount(indicesToBeReindexed.size());
        // The CountDownActionListener is 1 more than the number of indices so that the count is not 0 if we have no indices
        CountDownActionListener listener = new CountDownActionListener(indicesToBeReindexed.size() + 1, ActionListener.wrap(response1 -> {
            completeSuccessfulPersistentTask(reindexDataStreamTask, updatedState);
        }, exception -> { completeFailedPersistentTask(reindexDataStreamTask, updatedState, exception); }));
        final int maxConcurrentIndices = clusterService.getClusterSettings().get(MAX_CONCURRENT_INDICES_REINDEXED_PER_DATA_STREAM_SETTING);
        List<Index> indicesRemaining = Collections.synchronizedList(new ArrayList<>(indicesToBeReindexed));
        logger.debug("Reindexing {} indices, with up to {} handled concurrently", indicesRemaining.size(), maxConcurrentIndices);
        for (int i = 0; i < maxConcurrentIndices; i++) {
            maybeProcessNextIndex(indicesRemaining, reindexDataStreamTask, sourceDataStream, listener, parentTaskId);
        }
        // This takes care of the additional latch count referenced above:
        listener.onResponse(null);
    }

    private void maybeProcessNextIndex(
        List<Index> indicesRemaining,
        ReindexDataStreamTask reindexDataStreamTask,
        String sourceDataStream,
        CountDownActionListener listener,
        TaskId parentTaskId
    ) {
        if (indicesRemaining.isEmpty()) {
            return;
        }
        Index index;
        try {
            index = indicesRemaining.removeFirst();
        } catch (NoSuchElementException e) {
            return;
        }
        reindexDataStreamTask.incrementInProgressIndicesCount(index.getName());
        ReindexDataStreamIndexAction.Request reindexDataStreamIndexRequest = new ReindexDataStreamIndexAction.Request(index.getName());
        reindexDataStreamIndexRequest.setParentTask(parentTaskId);

        SubscribableListener.<ReindexDataStreamIndexAction.Response>newForked(
            l -> client.execute(ReindexDataStreamIndexAction.INSTANCE, reindexDataStreamIndexRequest, l)
        )
            .<String>andThen((l, result) -> updateDataStream(sourceDataStream, index.getName(), result.getDestIndex(), l, parentTaskId))
            .<AcknowledgedResponse>andThen((l, newIndex) -> copySettings(index.getName(), newIndex, l, parentTaskId))
            .<AcknowledgedResponse>andThen(l -> deleteIndex(index.getName(), parentTaskId, l))
            .addListener(ActionListener.wrap(unused -> {
                reindexDataStreamTask.reindexSucceeded(index.getName());
                listener.onResponse(null);
                maybeProcessNextIndex(indicesRemaining, reindexDataStreamTask, sourceDataStream, listener, parentTaskId);
            }, e -> {
                reindexDataStreamTask.reindexFailed(index.getName(), e);
                listener.onResponse(null);
                maybeProcessNextIndex(indicesRemaining, reindexDataStreamTask, sourceDataStream, listener, parentTaskId);
            }));
    }

    private void updateDataStream(
        String dataStream,
        String oldIndex,
        String newIndex,
        ActionListener<String> listener,
        TaskId parentTaskId
    ) {
        ModifyDataStreamsAction.Request modifyDataStreamRequest = new ModifyDataStreamsAction.Request(
            TimeValue.MAX_VALUE,
            TimeValue.MAX_VALUE,
            List.of(DataStreamAction.removeBackingIndex(dataStream, oldIndex), DataStreamAction.addBackingIndex(dataStream, newIndex))
        );
        modifyDataStreamRequest.setParentTask(parentTaskId);
        client.execute(ModifyDataStreamsAction.INSTANCE, modifyDataStreamRequest, listener.map(ingored -> newIndex));
    }

    /**
     * Copy lifecycle name from the old index to the new index, so that ILM can now process the new index.
     * If the new index has a lifecycle name before it is swapped into the data stream, ILM will try, and fail, to process
     * the new index. For this reason, lifecycle is not set until after the new index has been added to the data stream.
     */
    private void copySettings(String oldIndex, String newIndex, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        var getSettingsRequest = new GetSettingsRequest(TimeValue.MAX_VALUE).indices(oldIndex);
        getSettingsRequest.setParentTask(parentTaskId);
        client.execute(GetSettingsAction.INSTANCE, getSettingsRequest, listener.delegateFailure((delegate, response) -> {
            String lifecycleName = response.getSetting(oldIndex, IndexMetadata.LIFECYCLE_NAME);
            if (lifecycleName != null) {
                var settings = Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, lifecycleName).build();
                var updateSettingsRequest = new UpdateSettingsRequest(settings, newIndex);
                updateSettingsRequest.setParentTask(parentTaskId);
                client.execute(
                    TransportUpdateSettingsAction.TYPE,
                    updateSettingsRequest,
                    delegate.delegateFailure((delegate2, response2) -> {
                        maybeRunILMAsyncAction(newIndex, delegate2, parentTaskId);
                    })
                );
            } else {
                delegate.onResponse(null);
            }
        }));
    }

    /**
     * If ILM runs an async action on the source index shortly before reindexing, the results of the async action
     * may not yet be in the source index. For example, if a force merge has just been started by ILM, the reindex
     * will see the un-force-merged index. But the ILM state will be copied to destination index saying that an
     * async action was started, and so ILM won't force merge the destination index. To be sure that the async
     * action is run on the destination index, we force a retry on async actions after adding the ILM policy
     * to the destination index.
     */
    private void maybeRunILMAsyncAction(String newIndex, ActionListener<AcknowledgedResponse> listener, TaskId parentTaskId) {
        var retryActionRequest = new RetryActionRequest(TimeValue.MAX_VALUE, TimeValue.MAX_VALUE, newIndex);
        retryActionRequest.setParentTask(parentTaskId);
        retryActionRequest.requireError(false);
        client.execute(ILMActions.RETRY, retryActionRequest, listener);
    }

    private void deleteIndex(String indexName, TaskId parentTaskId, ActionListener<AcknowledgedResponse> listener) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        deleteIndexRequest.setParentTask(parentTaskId);
        client.execute(TransportDeleteIndexAction.TYPE, deleteIndexRequest, listener);
    }

    private void completeSuccessfulPersistentTask(
        ReindexDataStreamTask persistentTask,
        @Nullable ReindexDataStreamPersistentTaskState state
    ) {
        persistentTask.allReindexesCompleted(threadPool, updateCompletionTimeAndGetTimeToLive(persistentTask, state));
    }

    private void completeFailedPersistentTask(
        ReindexDataStreamTask persistentTask,
        @Nullable ReindexDataStreamPersistentTaskState state,
        Exception e
    ) {
        persistentTask.taskFailed(threadPool, updateCompletionTimeAndGetTimeToLive(persistentTask, state), e);
    }

    private Long getCompletionTime(PersistentTaskState persistentTaskState) {
        if (persistentTaskState instanceof ReindexDataStreamPersistentTaskState state) {
            return state.completionTime();
        } else {
            return null;
        }
    }

    private TimeValue updateCompletionTimeAndGetTimeToLive(
        ReindexDataStreamTask reindexDataStreamTask,
        @Nullable ReindexDataStreamPersistentTaskState state
    ) {
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = PersistentTasksCustomMetadata.getTaskWithId(
            clusterService.state(),
            reindexDataStreamTask.getPersistentTaskId()
        );
        if (persistentTask == null) {
            return TimeValue.timeValueMillis(0);
        }
        final long completionTime;
        if (state == null) {
            completionTime = threadPool.absoluteTimeInMillis();
            reindexDataStreamTask.updatePersistentTaskState(
                new ReindexDataStreamPersistentTaskState(null, null, completionTime),
                ActionListener.noop()
            );
        } else {
            if (state.completionTime() == null) {
                completionTime = threadPool.absoluteTimeInMillis();
                reindexDataStreamTask.updatePersistentTaskState(
                    new ReindexDataStreamPersistentTaskState(state.totalIndices(), state.totalIndicesToBeUpgraded(), completionTime),
                    ActionListener.noop()
                );
            } else {
                completionTime = state.completionTime();
            }
        }
        return getTimeToLive(completionTime);
    }

    private TimeValue getTimeToLive(long completionTimeInMillis) {
        return TimeValue.timeValueMillis(
            TASK_KEEP_ALIVE_TIME.millis() - Math.min(
                TASK_KEEP_ALIVE_TIME.millis(),
                threadPool.absoluteTimeInMillis() - completionTimeInMillis
            )
        );
    }
}
