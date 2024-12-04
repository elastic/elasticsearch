/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
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
                for (Index index : indicesToBeReindexed) {
                    reindexDataStreamTask.incrementInProgressIndicesCount();
                    // TODO This is just a placeholder. This is where the real data stream reindex logic will go
                    reindexDataStreamTask.reindexSucceeded();
                }

                completeSuccessfulPersistentTask(reindexDataStreamTask);
            } else {
                completeFailedPersistentTask(reindexDataStreamTask, new ElasticsearchException("data stream does not exist"));
            }
        }, reindexDataStreamTask::markAsFailed));
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
