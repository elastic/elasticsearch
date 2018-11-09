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
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Date;

public class TransportFinalizeJobExecutionAction extends TransportMasterNodeAction<FinalizeJobExecutionAction.Request,
    AcknowledgedResponse> {

    @Inject
    public TransportFinalizeJobExecutionAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FinalizeJobExecutionAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, FinalizeJobExecutionAction.Request::new);
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
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        String jobIdString = String.join(",", request.getJobIds());
        String source = "finalize_job_execution [" + jobIdString + "]";
        logger.debug("finalizing jobs [{}]", jobIdString);
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
                MlMetadata mlMetadata = MlMetadata.getMlMetadata(currentState);
                MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder(mlMetadata);
                Date finishedTime = new Date();

                for (String jobId : request.getJobIds()) {
                    Job.Builder jobBuilder = new Job.Builder(mlMetadata.getJobs().get(jobId));
                    jobBuilder.setFinishedTime(finishedTime);
                    mlMetadataBuilder.putJob(jobBuilder.build(), true);
                }
                ClusterState.Builder builder = ClusterState.builder(currentState);
                return builder.metaData(new MetaData.Builder(currentState.metaData())
                        .putCustom(MlMetadata.TYPE, mlMetadataBuilder.build()))
                        .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState,
                                              ClusterState newState) {
                logger.debug("finalized job [{}]", jobIdString);
                listener.onResponse(new AcknowledgedResponse(true));
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(FinalizeJobExecutionAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
