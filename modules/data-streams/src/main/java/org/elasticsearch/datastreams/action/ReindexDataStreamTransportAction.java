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
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.datastreams.task.ReindexDataStreamTask;
import org.elasticsearch.datastreams.task.ReindexDataStreamTaskParams;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/*
 * This transport action creates a new persistent task for reindexing the source data stream given in the request. On successful creation
 *  of the persistent task, it responds with the persistent task id so that the user can monitor the persistent task.
 */
public class ReindexDataStreamTransportAction extends HandledTransportAction<ReindexDataStreamRequest, ReindexDataStreamResponse> {
    private final PersistentTasksService persistentTasksService;

    @Inject
    public ReindexDataStreamTransportAction(
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
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<ReindexDataStreamResponse> listener) {
        ReindexDataStreamTaskParams params = new ReindexDataStreamTaskParams(request.getSourceDataStream());
        try {
            String persistentTaskId = getPersistentTaskId(request.getSourceDataStream());
            persistentTasksService.sendStartRequest(
                persistentTaskId,
                ReindexDataStreamTask.TASK_NAME,
                params,
                null,
                ActionListener.wrap(
                    startedTask -> persistentTasksService.waitForPersistentTaskCondition(
                        startedTask.getId(),
                        PersistentTasksCustomMetadata.PersistentTask::isAssigned,
                        null,
                        new PersistentTasksService.WaitForPersistentTaskListener<>() {
                            @Override
                            public void onResponse(
                                PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> persistentTaskParamsPersistentTask
                            ) {
                                listener.onResponse(new ReindexDataStreamResponse(persistentTaskId));
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
        } catch (ResourceAlreadyExistsException e) {
            // There is already a persistent task running for this data stream
            listener.onFailure(e);
        }
    }

    private String getPersistentTaskId(String dataStreamName) throws ResourceAlreadyExistsException {
        return "reindex-data-stream-" + dataStreamName;
        // TODO: Do we want to make an attempt to make these unique, and allow multiple to be running at once as long as all but one are
        // complete?
    }
}
