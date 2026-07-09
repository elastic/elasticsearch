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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ResumeBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.ResumeBulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

/// Abstract transport action for resuming BulkByPaginatedSearchAction operations asynchronously. Delegates to the corresponding action on
/// the local node, then returns a [ResumeBulkByPaginatedSearchResponse] containing the task id of the delegate action.
public abstract class AbstractResumeBulkByPaginatedSearchAction<Request extends AbstractBulkByPaginatedSearchRequest<Request>> extends
    HandledTransportAction<ResumeBulkByPaginatedSearchRequest, ResumeBulkByPaginatedSearchResponse> {

    private static final Logger logger = LogManager.getLogger(AbstractResumeBulkByPaginatedSearchAction.class);

    private final ClusterService clusterService;
    private final ActionType<BulkByPaginatedSearchResponse> delegateAction;
    private final NodeClient nodeClient;

    protected AbstractResumeBulkByPaginatedSearchAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ResumeBulkByPaginatedSearchRequest> requestReader,
        Executor executor,
        ClusterService clusterService,
        ActionType<BulkByPaginatedSearchResponse> delegateAction,
        NodeClient nodeClient
    ) {
        super(actionName, transportService, actionFilters, requestReader, executor);
        this.clusterService = clusterService;
        this.delegateAction = delegateAction;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(
        Task task,
        ResumeBulkByPaginatedSearchRequest request,
        ActionListener<ResumeBulkByPaginatedSearchResponse> listener
    ) {
        // ResumeBulkByPaginatedSearchRequest.validate() rejects requests with no ResumeInfo
        assert request.getDelegate().getResumeInfo().isPresent();
        final ResumeInfo resumeInfo = request.getDelegate().getResumeInfo().get();

        var responseListener = new SubscribableListener<BulkByPaginatedSearchResponse>();
        Task delegateTask = nodeClient.executeLocally(delegateAction, request.getDelegate(), responseListener);
        responseListener.addListener(new LoggingReindexTaskListener(delegateTask));
        TaskId taskId = new TaskId(clusterService.localNode().getId(), delegateTask.getId());
        logger.info(
            "resumed relocation for [{}]: original task [{}] continues as task [{}]",
            actionName,
            resumeInfo.relocationOrigin().originalTaskId(),
            taskId
        );

        listener.onResponse(new ResumeBulkByPaginatedSearchResponse(taskId));
    }
}
