/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

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
        return new ReindexDataStreamTask(threadPool, id, type, action, "id=" + taskInProgress.getId(), parentTaskId, headers);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ReindexDataStreamTaskParams params, PersistentTaskState state) {
        String sourceDataStream = params.getSourceDataStream();
        GetDataStreamAction.Request request = new GetDataStreamAction.Request(TimeValue.MAX_VALUE, new String[] { sourceDataStream });
        client.execute(GetDataStreamAction.INSTANCE, request, ActionListener.wrap(response -> {
            List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = response.getDataStreams();
            if (dataStreamInfos.size() == 1) {
                List<Index> indices = dataStreamInfos.getFirst().getDataStream().getIndices();
                List<Index> indicesToBeReindexed = indices.stream()
                    .filter(index -> clusterService.state().getMetadata().index(index).getCreationVersion().isLegacyIndexVersion())
                    .toList();
                for (Index index : indicesToBeReindexed) {
                    // add to the queue to be reindexed
                }
                task.markAsCompleted();
            } else {
                task.markAsFailed(new ElasticsearchException("data stream does not exist"));
            }
        }, task::markAsFailed));
    }
}
