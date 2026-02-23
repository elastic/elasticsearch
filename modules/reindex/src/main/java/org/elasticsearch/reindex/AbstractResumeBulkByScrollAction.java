/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeBulkByScrollRequest;
import org.elasticsearch.index.reindex.ResumeBulkByScrollResponse;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/**
 * Abstract transport action for resuming BulkByScrollAction operations asynchronously. Delegates to the corresponding action on the local
 * node, then returns a {@link ResumeBulkByScrollResponse} containing the task id of the delegate action.
 */
public abstract class AbstractResumeBulkByScrollAction<Request extends AbstractBulkByScrollRequest<Request>> extends HandledTransportAction<
    ResumeBulkByScrollRequest,
    ResumeBulkByScrollResponse> {

    private final ClusterService clusterService;
    private final ActionType<BulkByScrollResponse> delegateAction;
    private final NodeClient nodeClient;

    protected AbstractResumeBulkByScrollAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ResumeBulkByScrollRequest> requestReader,
        Executor executor,
        ClusterService clusterService,
        ActionType<BulkByScrollResponse> delegateAction,
        NodeClient nodeClient
    ) {
        super(actionName, transportService, actionFilters, requestReader, executor);
        this.clusterService = clusterService;
        this.delegateAction = delegateAction;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, ResumeBulkByScrollRequest request, ActionListener<ResumeBulkByScrollResponse> listener) {
        Task delegateTask = nodeClient.executeLocally(delegateAction, request.getDelegate(), new LoggingTaskListener<>(task));
        TaskId taskId = new TaskId(clusterService.localNode().getId(), delegateTask.getId());
        listener.onResponse(new ResumeBulkByScrollResponse(taskId));
    }
}
