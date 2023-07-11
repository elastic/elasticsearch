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
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.job.JobManager;

import java.util.Optional;
import java.util.stream.Collectors;

public class TransportGetJobsAction extends TransportMasterNodeReadAction<GetJobsAction.Request, GetJobsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetJobsAction.class);

    private final JobManager jobManager;
    private final DatafeedManager datafeedManager;

    @Inject
    public TransportGetJobsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        JobManager jobManager,
        DatafeedManager datafeedManager
    ) {
        super(
            GetJobsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetJobsAction.Request::new,
            indexNameExpressionResolver,
            GetJobsAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.jobManager = jobManager;
        this.datafeedManager = datafeedManager;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetJobsAction.Request request,
        ClusterState state,
        ActionListener<GetJobsAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        logger.debug("Get job '{}'", request.getJobId());
        jobManager.expandJobBuilders(
            request.getJobId(),
            request.allowNoMatch(),
            parentTaskId,
            listener.delegateFailureAndWrap(
                (delegate, jobs) -> datafeedManager.getDatafeedsByJobIds(
                    jobs.stream().map(Job.Builder::getId).collect(Collectors.toSet()),
                    parentTaskId,
                    delegate.delegateFailureAndWrap(
                        (l, dfsByJobId) -> l.onResponse(new GetJobsAction.Response(new QueryPage<>(jobs.stream().map(jb -> {
                            Optional.ofNullable(dfsByJobId.get(jb.getId())).ifPresent(jb::setDatafeed);
                            return jb.build();
                        }).collect(Collectors.toList()), jobs.size(), Job.RESULTS_FIELD)))
                    )
                )
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetJobsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
