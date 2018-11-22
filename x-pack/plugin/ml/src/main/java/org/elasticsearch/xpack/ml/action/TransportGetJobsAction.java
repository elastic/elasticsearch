/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.JobManager;

public class TransportGetJobsAction extends TransportMasterNodeReadAction<GetJobsAction.Request, GetJobsAction.Response> {

    private final JobManager jobManager;

    @Inject
    public TransportGetJobsAction(TransportService transportService, ClusterService clusterService,
                                  ThreadPool threadPool, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  JobManager jobManager) {
        super(GetJobsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetJobsAction.Request::new, indexNameExpressionResolver);
        this.jobManager = jobManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetJobsAction.Response newResponse() {
        return new GetJobsAction.Response();
    }

    @Override
    protected void masterOperation(GetJobsAction.Request request, ClusterState state, ActionListener<GetJobsAction.Response> listener)
            throws Exception {
        logger.debug("Get job '{}'", request.getJobId());
        QueryPage<Job> jobs = jobManager.expandJobs(request.getJobId(), request.allowNoJobs(), state);
        listener.onResponse(new GetJobsAction.Response(jobs));
    }

    @Override
    protected ClusterBlockException checkBlock(GetJobsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
