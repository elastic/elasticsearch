/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import static org.elasticsearch.core.Strings.format;

public class TransportGetModelSnapshotsAction extends HandledTransportAction<
    GetModelSnapshotsAction.Request,
    GetModelSnapshotsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetModelSnapshotsAction.class);

    private final JobResultsProvider jobResultsProvider;
    private final JobManager jobManager;
    private final ClusterService clusterService;

    @Inject
    public TransportGetModelSnapshotsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        JobResultsProvider jobResultsProvider,
        JobManager jobManager,
        ClusterService clusterService
    ) {
        super(GetModelSnapshotsAction.NAME, transportService, actionFilters, GetModelSnapshotsAction.Request::new);
        this.jobResultsProvider = jobResultsProvider;
        this.jobManager = jobManager;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(
        Task task,
        GetModelSnapshotsAction.Request request,
        ActionListener<GetModelSnapshotsAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        logger.debug(
            () -> format(
                "Get model snapshots for job %s snapshot ID %s. from = %s, size = %s start = '%s', end='%s', sort=%s descending=%s",
                request.getJobId(),
                request.getSnapshotId(),
                request.getPageParams().getFrom(),
                request.getPageParams().getSize(),
                request.getStart(),
                request.getEnd(),
                request.getSort(),
                request.getDescOrder()
            )
        );

        if (Strings.isAllOrWildcard(request.getJobId())) {
            getModelSnapshots(request, parentTaskId, listener);
            return;
        }
        jobManager.jobExists(
            request.getJobId(),
            parentTaskId,
            listener.delegateFailureAndWrap((l, ok) -> getModelSnapshots(request, parentTaskId, l))
        );
    }

    private void getModelSnapshots(
        GetModelSnapshotsAction.Request request,
        TaskId parentTaskId,
        ActionListener<GetModelSnapshotsAction.Response> listener
    ) {
        jobResultsProvider.modelSnapshots(
            request.getJobId(),
            request.getPageParams().getFrom(),
            request.getPageParams().getSize(),
            request.getStart(),
            request.getEnd(),
            request.getSort(),
            request.getDescOrder(),
            request.getSnapshotId(),
            parentTaskId,
            page -> listener.onResponse(new GetModelSnapshotsAction.Response(page)),
            listener::onFailure
        );
    }
}
