/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransportGetRollupJobAction extends TransportTasksAction<RollupJobTask, GetRollupJobsAction.Request,
        GetRollupJobsAction.Response, GetRollupJobsAction.Response> {

    @Inject
    public TransportGetRollupJobAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       ClusterService clusterService) {

        super(settings, GetRollupJobsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, GetRollupJobsAction.Request::new,
                GetRollupJobsAction.Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected void taskOperation(GetRollupJobsAction.Request request, RollupJobTask jobTask,
                                 ActionListener<GetRollupJobsAction.Response> listener) {
        List<GetRollupJobsAction.JobWrapper> jobs = Collections.emptyList();
        if (jobTask.getConfig().getId().equals(request.getId()) || request.getId().equals(MetaData.ALL)) {
            GetRollupJobsAction.JobWrapper wrapper = new GetRollupJobsAction.JobWrapper(jobTask.getConfig(), jobTask.getStats(),
                    (RollupJobStatus)jobTask.getStatus());
            jobs = Collections.singletonList(wrapper);
        }
        listener.onResponse(new GetRollupJobsAction.Response(jobs));
    }

    @Override
    protected GetRollupJobsAction.Response newResponse(GetRollupJobsAction.Request request, List<GetRollupJobsAction.Response> tasks,
                                                       List<TaskOperationFailure> taskOperationFailures,
                                                       List<FailedNodeException> failedNodeExceptions) {
        List<GetRollupJobsAction.JobWrapper> jobs = tasks.stream().map(GetRollupJobsAction.Response::getJobs)
                .flatMap(Collection::stream).collect(Collectors.toList());
        return new GetRollupJobsAction.Response(jobs, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected GetRollupJobsAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new GetRollupJobsAction.Response(in);
    }
}