/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
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
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.utils.VoidChainTaskExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportFinalizeJobExecutionAction extends
        TransportMasterNodeAction<FinalizeJobExecutionAction.Request, AcknowledgedResponse> {

    private final Client client;
    @Inject
    public TransportFinalizeJobExecutionAction(Settings settings, TransportService transportService,
                                               ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Client client) {
        super(settings, FinalizeJobExecutionAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, FinalizeJobExecutionAction.Request::new);
        this.client = client;
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
        Set<String> jobsInClusterState = Arrays.stream(request.getJobIds())
                .filter(id -> mlMetadata.getJobs().containsKey(id))
                .collect(Collectors.toSet());

        if (jobsInClusterState.isEmpty()) {
            finalizeIndexJobs(Arrays.asList(request.getJobIds()), listener);
        } else {
            ActionListener<AcknowledgedResponse> finalizeClusterStateJobsListener = ActionListener.wrap(
                    ack -> {
                        Set<String> jobsInIndex = new HashSet<>(Arrays.asList(request.getJobIds()));
                        jobsInIndex.removeAll(jobsInClusterState);
                        if (jobsInIndex.isEmpty()) {
                            listener.onResponse(ack);
                        } else {
                            finalizeIndexJobs(jobsInIndex, listener);
                        }
                    },
                    listener::onFailure
            );

            finalizeClusterStateJobs(jobsInClusterState, finalizeClusterStateJobsListener);
        }
    }

    private void finalizeIndexJobs(Collection<String> jobIds, ActionListener<AcknowledgedResponse> listener) {
        String jobIdString = String.join(",", jobIds);
        logger.debug("finalizing jobs [{}]", jobIdString);

        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.executor(
                MachineLearning.UTILITY_THREAD_POOL_NAME), true);

        Map<Object, Object> update = Collections.singletonMap(Job.FINISHED_TIME.getPreferredName(), new Date());

        for (String jobId: jobIds) {
            UpdateRequest updateRequest = new UpdateRequest(AnomalyDetectorsIndex.configIndexName(),
                    ElasticsearchMappings.DOC_TYPE, Job.documentId(jobId));
            updateRequest.retryOnConflict(3);
            updateRequest.doc(update);
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            voidChainTaskExecutor.add(chainedListener -> {
                executeAsyncWithOrigin(client, ML_ORIGIN, UpdateAction.INSTANCE, updateRequest, ActionListener.wrap(
                        updateResponse -> chainedListener.onResponse(null),
                        chainedListener::onFailure
                ));
            });
        }

        voidChainTaskExecutor.execute(ActionListener.wrap(
                aVoids ->  {
                    logger.debug("finalized job [{}]", jobIdString);
                    listener.onResponse(new AcknowledgedResponse(true));
                },
                listener::onFailure
        ));
    }

    private void finalizeClusterStateJobs(Collection<String> jobIds, ActionListener<AcknowledgedResponse> listener) {
        String jobIdString = String.join(",", jobIds);
        String source = "finalize_job_execution [" + jobIdString + "]";
        logger.debug("finalizing jobs [{}]", jobIdString);
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
                MlMetadata mlMetadata = MlMetadata.getMlMetadata(currentState);
                MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder(mlMetadata);
                Date finishedTime = new Date();

                for (String jobId : jobIds) {
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
