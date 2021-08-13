/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.job.JobManager;

public class TransportUpdateJobAction extends TransportMasterNodeAction<UpdateJobAction.Request, PutJobAction.Response> {

    private final JobManager jobManager;

    @Inject
    public TransportUpdateJobAction(TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver, JobManager jobManager) {
        super(UpdateJobAction.NAME, transportService, clusterService, threadPool, actionFilters, UpdateJobAction.Request::new,
                indexNameExpressionResolver, PutJobAction.Response::new, ThreadPool.Names.SAME);
        this.jobManager = jobManager;
    }

    @Override
    protected void masterOperation(Task task, UpdateJobAction.Request request, ClusterState state,
                                   ActionListener<PutJobAction.Response> listener) {
        jobManager.updateJob(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
