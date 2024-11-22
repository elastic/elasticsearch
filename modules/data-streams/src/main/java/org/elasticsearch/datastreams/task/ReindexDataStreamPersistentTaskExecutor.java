/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamIndexAction;
import org.elasticsearch.action.datastreams.SwapDataStreamIndexAction;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

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
            threadPool,
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
        client.execute(GetDataStreamAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = response.getDataStreams();
            if (dataStreamInfos.size() == 1) {
                List<Index> indices = dataStreamInfos.getFirst().getDataStream().getIndices();
                List<Index> indicesToBeReindexed = indices.stream()
                    .filter(index -> clusterService.state().getMetadata().index(index).getCreationVersion().isLegacyIndexVersion())
                    .toList();
                reindexDataStreamTask.setPendingIndicesCount(indicesToBeReindexed.size());
                CountDownActionListener listener = new CountDownActionListener(
                    indicesToBeReindexed.size() + 1,
                    ActionListener.wrap(response1 -> {
                        completeSuccessfulPersistentTask(reindexDataStreamTask);
                    }, exception -> { completeFailedPersistentTask(reindexDataStreamTask, exception); })
                );
                // TODO: put all these on a queue, only process N from queue at a time
                for (Index index : indicesToBeReindexed) {
                    reindexDataStreamTask.incrementInProgressIndicesCount();
                    client.execute(
                        ReindexDataStreamIndexAction.INSTANCE,
                        new ReindexDataStreamIndexAction.Request(index.getName()),
                        ActionListener.wrap(response1 -> {
                            updateDataStream(sourceDataStream, index.getName(), response1.getDestIndex(), ActionListener.wrap(unused -> {
                                reindexDataStreamTask.reindexSucceeded();
                                listener.onResponse(null);
                            }, exception -> {
                                reindexDataStreamTask.reindexFailed(index.getName(), exception);
                                listener.onResponse(null);
                            }));

                        }, exception -> {
                            reindexDataStreamTask.reindexFailed(index.getName(), exception);
                            listener.onResponse(null);
                        })
                    );
                }
                listener.onResponse(null);
            } else {
                completeFailedPersistentTask(reindexDataStreamTask, new ElasticsearchException("data stream does not exist"));
            }
        }, exception -> completeFailedPersistentTask(reindexDataStreamTask, exception)));
    }

    private void updateDataStream(String dataStream, String oldIndex, String newIndex, ActionListener<Void> listener) {
        client.execute(
            SwapDataStreamIndexAction.INSTANCE,
            new SwapDataStreamIndexAction.Request(dataStream, oldIndex, newIndex),
            new ActionListener<>() {
                @Override
                public void onResponse(SwapDataStreamIndexAction.Response response) {
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
        persistentTask.allReindexesCompleted();
        threadPool.schedule(persistentTask::markAsCompleted, getTimeToLive(persistentTask), threadPool.generic());
    }

    private void completeFailedPersistentTask(ReindexDataStreamTask persistentTask, Exception e) {
        persistentTask.taskFailed(e);
        threadPool.schedule(() -> persistentTask.markAsFailed(e), getTimeToLive(persistentTask), threadPool.generic());
    }

    private TimeValue getTimeToLive(ReindexDataStreamTask reindexDataStreamTask) {
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
            .getMetadata()
            .custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = persistentTasksCustomMetadata.getTask(
            reindexDataStreamTask.getPersistentTaskId()
        );
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
