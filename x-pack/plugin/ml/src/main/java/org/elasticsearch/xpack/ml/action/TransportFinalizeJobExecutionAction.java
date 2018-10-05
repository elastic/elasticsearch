/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.utils.ChainTaskExecutor;

import java.util.Date;

public class TransportFinalizeJobExecutionAction extends TransportMasterNodeAction<FinalizeJobExecutionAction.Request,
    AcknowledgedResponse> {

    private final JobConfigProvider jobConfigProvider;

    @Inject
    public TransportFinalizeJobExecutionAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               JobConfigProvider jobConfigProvider) {
        super(settings, FinalizeJobExecutionAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, FinalizeJobExecutionAction.Request::new);
        this.jobConfigProvider = jobConfigProvider;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(FinalizeJobExecutionAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {

        String jobIdString = String.join(",", request.getJobIds());
        logger.debug("finalizing jobs [{}]", jobIdString);

        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.executor(
                MachineLearning.UTILITY_THREAD_POOL_NAME), false);

        Date now = new Date();
        for (String jobId : request.getJobIds()) {
            JobUpdate finishedTimeUpdate = new JobUpdate.Builder(jobId).setFinishedTime(now).build();
            chainTaskExecutor.add(updateListener -> {
                jobConfigProvider.updateJob(jobId, finishedTimeUpdate, null, ActionListener.wrap(
                        response -> updateListener.onResponse(null),
                        updateListener::onFailure
                ) );
            });
        }

        chainTaskExecutor.execute(ActionListener.wrap(
                response -> listener.onResponse(new AcknowledgedResponse(true)),
                listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(FinalizeJobExecutionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
