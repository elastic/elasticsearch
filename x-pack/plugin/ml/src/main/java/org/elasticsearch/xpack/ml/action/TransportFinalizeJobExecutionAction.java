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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

// This action is only called from modes before version 6.6.0
public class TransportFinalizeJobExecutionAction extends TransportMasterNodeAction<FinalizeJobExecutionAction.Request,
    AcknowledgedResponse> {

    @Inject
    public TransportFinalizeJobExecutionAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, FinalizeJobExecutionAction.NAME, transportService, clusterService, threadPool, actionFilters,
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
                                   ActionListener<AcknowledgedResponse> listener) {

        MlMetadata mlMetadata = MlMetadata.getMlMetadata(state);
        List<String> jobsInClusterState = Arrays.stream(request.getJobIds())
                .filter(id -> mlMetadata.getJobs().containsKey(id))
                .collect(Collectors.toList());

        // This action should not be called for jobs that have
        // their configuration in index documents

        if (jobsInClusterState.isEmpty()) {
            // This action is a no-op for jobs not defined in the cluster state.
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }

        String jobIdString = String.join(",", jobsInClusterState);
        String source = "finalize_job_execution [" + jobIdString + "]";
        logger.debug("finalizing jobs [{}]", jobIdString);
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
                MlMetadata mlMetadata = MlMetadata.getMlMetadata(currentState);
                MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder(mlMetadata);
                Date finishedTime = new Date();

                for (String jobId : jobsInClusterState) {
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
