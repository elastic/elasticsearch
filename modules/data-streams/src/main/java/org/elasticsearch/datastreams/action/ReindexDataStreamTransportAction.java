/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.action.datastreams.ReindexDataStreamAction.ReindexDataStreamResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.datastreams.task.ReindexDataStreamTask;
import org.elasticsearch.datastreams.task.ReindexDataStreamTaskParams;
import org.elasticsearch.injection.guice.Inject;
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
    private final TransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public ReindexDataStreamTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PersistentTasksService persistentTasksService,
        ClusterService clusterService
    ) {
        super(
            ReindexDataStreamAction.NAME,
            true,
            transportService,
            actionFilters,
            ReindexDataStreamRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.transportService = transportService;
        this.persistentTasksService = persistentTasksService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<ReindexDataStreamResponse> listener) {
        String sourceDataStreamName = request.getSourceDataStream();
        Metadata metadata = clusterService.state().metadata();
        DataStream dataStream = metadata.dataStreams().get(sourceDataStreamName);
        if (dataStream == null) {
            listener.onFailure(new ResourceNotFoundException("Data stream named [{}] does not exist", sourceDataStreamName));
            return;
        }
        int totalIndices = dataStream.getIndices().size();
        int totalIndicesToBeUpgraded = (int) dataStream.getIndices()
            .stream()
            .filter(index -> metadata.index(index).getCreationVersion().isLegacyIndexVersion())
            .count();
        ReindexDataStreamTaskParams params = new ReindexDataStreamTaskParams(
            sourceDataStreamName,
            transportService.getThreadPool().absoluteTimeInMillis(),
            totalIndices,
            totalIndicesToBeUpgraded
        );
        String persistentTaskId = getPersistentTaskId(sourceDataStreamName);
        persistentTasksService.sendStartRequest(
            persistentTaskId,
            ReindexDataStreamTask.TASK_NAME,
            params,
            null,
            ActionListener.wrap(startedTask -> listener.onResponse(new ReindexDataStreamResponse(persistentTaskId)), listener::onFailure)
        );
    }

    private String getPersistentTaskId(String dataStreamName) throws ResourceAlreadyExistsException {
        return "reindex-data-stream-" + dataStreamName;
    }
}
