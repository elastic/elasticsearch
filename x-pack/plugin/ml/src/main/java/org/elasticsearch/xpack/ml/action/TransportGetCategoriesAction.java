/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

public class TransportGetCategoriesAction extends HandledTransportAction<GetCategoriesAction.Request, GetCategoriesAction.Response> {

    private final JobResultsProvider jobResultsProvider;
    private final Client client;
    private final JobManager jobManager;
    private final ClusterService clusterService;

    @Inject
    public TransportGetCategoriesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        Client client,
        JobManager jobManager,
        ClusterService clusterService
    ) {
        super(GetCategoriesAction.NAME, transportService, actionFilters, GetCategoriesAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.client = client;
        this.jobManager = jobManager;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetCategoriesAction.Request request, ActionListener<GetCategoriesAction.Response> listener) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        jobManager.jobExists(request.getJobId(), parentTaskId, ActionListener.wrap(jobExists -> {
            Integer from = request.getPageParams() != null ? request.getPageParams().getFrom() : null;
            Integer size = request.getPageParams() != null ? request.getPageParams().getSize() : null;
            jobResultsProvider.categoryDefinitions(
                request.getJobId(),
                request.getCategoryId(),
                request.getPartitionFieldValue(),
                true,
                from,
                size,
                r -> listener.onResponse(new GetCategoriesAction.Response(r)),
                listener::onFailure,
                (CancellableTask) task,
                parentTaskId,
                new ParentTaskAssigningClient(client, parentTaskId)
            );
        }, listener::onFailure));
    }
}
