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
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

public class ReindexDataStreamTransportAction extends HandledTransportAction<ReindexDataStreamRequest, ReindexDataStreamResponse> {
    private final NodeClient client;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public ReindexDataStreamTransportAction(
        NodeClient client,
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService
    ) {
        super(
            ReindexDataStreamAction.NAME,
            true,
            transportService,
            actionFilters,
            ReindexDataStreamRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.client = client;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<ReindexDataStreamResponse> listener) {
        ReindexDataStreamTaskParams params = new ReindexDataStreamTaskParams(request.getSourceIndex());
        String persistentTaskId = "reindex-my-data-stream-" + request.getSourceIndex();
        final long startTime = System.currentTimeMillis();
        persistentTasksService.sendStartRequest(
            persistentTaskId,
            ReindexDataStreamTask.TASK_NAME,
            params,
            null,
            ActionListener.wrap(
                startedTask -> persistentTasksService.waitForPersistentTaskCondition(
                    startedTask.getId(),
                    persistentTask -> true,
                    null,
                    new PersistentTasksService.WaitForPersistentTaskListener<>() {
                        @Override
                        public void onResponse(
                            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTaskParamsPersistentTask
                        ) {
                            listener.onResponse(
                                new ReindexDataStreamResponse(
                                    client.getLocalNodeId() + ":" + getTaskIdForPersistentTaskId(startedTask.getId())
                                )
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(new ElasticsearchException("Task [" + persistentTaskId + "] failed starting", e));
                        }
                    }

                ),
                e -> listener.onFailure(new ElasticsearchException("Task [" + persistentTaskId + "] failed starting", e))
            )
        );

    }

    private String getTaskIdForPersistentTaskId(String persistentTaskId) {
        return taskManager.getCancellableTasks()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().getType().equals("persistent"))
            .filter(
                entry -> entry.getValue() instanceof AllocatedPersistentTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) entry.getValue()).getPersistentTaskId()))
            )
            .map(Map.Entry::getKey)
            .map(Object::toString)
            .findFirst()
            .orElse(null);
    }

    private void completeTask(String persistentTaskId) {
        taskManager.getCancellableTasks()
            .values()
            .stream()
            .filter(cancellableTask -> cancellableTask.getType().equals("persistent"))
            .filter(
                cancellableTask -> cancellableTask instanceof AllocatedPersistentTask
                    && persistentTaskId.equals((((AllocatedPersistentTask) cancellableTask).getPersistentTaskId()))
            )
            .forEach(task -> ((AllocatedPersistentTask) task).markAsCompleted());
    }
}
