/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamIndexAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.getOldIndexVersionPredicate;

public class ReindexDataStreamPersistentTaskExecutor extends PersistentTasksExecutor<ReindexDataStreamTaskParams> {
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
            params.startTime(),
            params.totalIndices(),
            params.totalIndicesToBeUpgraded(),
            id,
            type,
            action,
            "id=" + taskInProgress.getId(),
            parentTaskId,
            headers
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ReindexDataStreamTaskParams params, PersistentTaskState state) {
        String sourceDataStream = params.getSourceDataStream();
        GetDataStreamAction.Request request = new GetDataStreamAction.Request(TimeValue.MAX_VALUE, new String[] { sourceDataStream });
        assert task instanceof ReindexDataStreamTask;
        final ReindexDataStreamTask reindexDataStreamTask = (ReindexDataStreamTask) task;
        ExecuteWithHeadersClient reindexClient = new ExecuteWithHeadersClient(client, params.headers());
        reindexClient.execute(GetDataStreamAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = response.getDataStreams();
            if (dataStreamInfos.size() == 1) {
                DataStream dataStream = dataStreamInfos.getFirst().getDataStream();
                if (getOldIndexVersionPredicate(clusterService.state().metadata()).test(dataStream.getWriteIndex())) {
                    reindexClient.execute(
                        RolloverAction.INSTANCE,
                        new RolloverRequest(sourceDataStream, null),
                        ActionListener.wrap(
                            rolloverResponse -> reindexIndices(dataStream, reindexDataStreamTask, reindexClient, sourceDataStream),
                            e -> completeFailedPersistentTask(reindexDataStreamTask, e)
                        )
                    );
                } else {
                    reindexIndices(dataStream, reindexDataStreamTask, reindexClient, sourceDataStream);
                }
            } else {
                completeFailedPersistentTask(reindexDataStreamTask, new ElasticsearchException("data stream does not exist"));
            }
        }, exception -> completeFailedPersistentTask(reindexDataStreamTask, exception)));
    }

    private void reindexIndices(
        DataStream dataStream,
        ReindexDataStreamTask reindexDataStreamTask,
        ExecuteWithHeadersClient reindexClient,
        String sourceDataStream
    ) {
        List<Index> indices = dataStream.getIndices();
        List<Index> indicesToBeReindexed = indices.stream().filter(getOldIndexVersionPredicate(clusterService.state().metadata())).toList();
        reindexDataStreamTask.setPendingIndicesCount(indicesToBeReindexed.size());
        // The CountDownActionListener is 1 more than the number of indices so that the count is not 0 if we have no indices
        CountDownActionListener listener = new CountDownActionListener(indicesToBeReindexed.size() + 1, ActionListener.wrap(response1 -> {
            completeSuccessfulPersistentTask(reindexDataStreamTask);
        }, exception -> { completeFailedPersistentTask(reindexDataStreamTask, exception); }));
        List<Index> indicesRemaining = Collections.synchronizedList(new ArrayList<>(indicesToBeReindexed));
        final int maxConcurrentIndices = 1;
        for (int i = 0; i < maxConcurrentIndices; i++) {
            maybeProcessNextIndex(indicesRemaining, reindexDataStreamTask, reindexClient, sourceDataStream, listener);
        }
        // This takes care of the additional latch count referenced above:
        listener.onResponse(null);
    }

    private void maybeProcessNextIndex(
        List<Index> indicesRemaining,
        ReindexDataStreamTask reindexDataStreamTask,
        ExecuteWithHeadersClient reindexClient,
        String sourceDataStream,
        CountDownActionListener listener
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
        reindexClient.execute(
            ReindexDataStreamIndexAction.INSTANCE,
            new ReindexDataStreamIndexAction.Request(index.getName()),
            ActionListener.wrap(response1 -> {
                updateDataStream(sourceDataStream, index.getName(), response1.getDestIndex(), ActionListener.wrap(unused -> {
                    reindexDataStreamTask.reindexSucceeded(index.getName());
                    listener.onResponse(null);
                    maybeProcessNextIndex(indicesRemaining, reindexDataStreamTask, reindexClient, sourceDataStream, listener);
                }, exception -> {
                    reindexDataStreamTask.reindexFailed(index.getName(), exception);
                    listener.onResponse(null);
                }), reindexClient);
            }, exception -> {
                reindexDataStreamTask.reindexFailed(index.getName(), exception);
                listener.onResponse(null);
            })
        );
    }

    private void updateDataStream(
        String dataStream,
        String oldIndex,
        String newIndex,
        ActionListener<Void> listener,
        ExecuteWithHeadersClient reindexClient
    ) {
        reindexClient.execute(
            ModifyDataStreamsAction.INSTANCE,
            new ModifyDataStreamsAction.Request(
                TimeValue.MAX_VALUE,
                TimeValue.MAX_VALUE,
                List.of(DataStreamAction.removeBackingIndex(dataStream, oldIndex), DataStreamAction.addBackingIndex(dataStream, newIndex))
            ),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    private void completeSuccessfulPersistentTask(ReindexDataStreamTask persistentTask) {
        persistentTask.allReindexesCompleted(threadPool, getTimeToLive(persistentTask));
    }

    private void completeFailedPersistentTask(ReindexDataStreamTask persistentTask, Exception e) {
        persistentTask.taskFailed(threadPool, getTimeToLive(persistentTask), e);
    }

    private TimeValue getTimeToLive(ReindexDataStreamTask reindexDataStreamTask) {
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
            .getMetadata()
            .custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = persistentTasksCustomMetadata.getTask(
            reindexDataStreamTask.getPersistentTaskId()
        );
        if (persistentTask == null) {
            return TimeValue.timeValueMillis(0);
        }
        PersistentTaskState state = persistentTask.getState();
        final long completionTime;
        if (state == null) {
            completionTime = threadPool.absoluteTimeInMillis();
            reindexDataStreamTask.updatePersistentTaskState(
                new ReindexDataStreamPersistentTaskState(completionTime),
                ActionListener.noop()
            );
        } else {
            completionTime = ((ReindexDataStreamPersistentTaskState) state).completionTime();
        }
        return TimeValue.timeValueMillis(TASK_KEEP_ALIVE_TIME.millis() - (threadPool.absoluteTimeInMillis() - completionTime));
    }
}
