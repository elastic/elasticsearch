/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.ReindexDataStreamRequest;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTask;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamTaskParams;

import static org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate.getReindexRequiredPredicate;
import static org.elasticsearch.xpack.migrate.action.ReindexDataStreamAction.TASK_ID_PREFIX;

/*
 * This transport action creates a new persistent task for reindexing the source data stream given in the request. On successful creation
 *  of the persistent task, it responds with the persistent task id so that the user can monitor the persistent task.
 */
public class ReindexDataStreamTransportAction extends HandledTransportAction<ReindexDataStreamRequest, AcknowledgedResponse> {
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
    protected void doExecute(Task task, ReindexDataStreamRequest request, ActionListener<AcknowledgedResponse> listener) {
        String sourceDataStreamName = request.getSourceDataStream();
        Metadata metadata = clusterService.state().metadata();
        DataStream dataStream = metadata.dataStreams().get(sourceDataStreamName);
        if (dataStream == null) {
            listener.onFailure(new ResourceNotFoundException("Data stream named [{}] does not exist", sourceDataStreamName));
            return;
        }
        int totalIndices = dataStream.getIndices().size();
        int totalIndicesToBeUpgraded = (int) dataStream.getIndices().stream().filter(getReindexRequiredPredicate(metadata, false)).count();
        ReindexDataStreamTaskParams params = new ReindexDataStreamTaskParams(
            sourceDataStreamName,
            transportService.getThreadPool().absoluteTimeInMillis(),
            totalIndices,
            totalIndicesToBeUpgraded,
            ClientHelper.getPersistableSafeSecurityHeaders(transportService.getThreadPool().getThreadContext(), clusterService.state())
        );
        String persistentTaskId = getPersistentTaskId(sourceDataStreamName);
        persistentTasksService.sendStartRequest(
            persistentTaskId,
            ReindexDataStreamTask.TASK_NAME,
            params,
            null,
            ActionListener.wrap(startedTask -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
        );
    }

    private String getPersistentTaskId(String dataStreamName) throws ResourceAlreadyExistsException {
        return TASK_ID_PREFIX + dataStreamName;
    }
}
