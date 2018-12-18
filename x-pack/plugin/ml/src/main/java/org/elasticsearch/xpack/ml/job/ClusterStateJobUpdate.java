/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;

/**
 * Helper functions for managing cluster state job configurations
 */
public final class ClusterStateJobUpdate {

    private ClusterStateJobUpdate() {
    }

    public static boolean jobIsInClusterState(ClusterState clusterState, String jobId) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        return mlMetadata.getJobs().containsKey(jobId);
    }

    public static boolean jobIsInMlMetadata(MlMetadata mlMetadata, String jobId) {
        return mlMetadata.getJobs().containsKey(jobId);
    }

    public static ClusterState putJobInClusterState(Job job, boolean overwrite, ClusterState currentState) {
        MlMetadata.Builder builder = createMlMetadataBuilder(currentState);
        builder.putJob(job, overwrite);
        return buildNewClusterState(currentState, builder);
    }

    private static MlMetadata.Builder createMlMetadataBuilder(ClusterState currentState) {
        return new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
    }

    private static ClusterState buildNewClusterState(ClusterState currentState, MlMetadata.Builder builder) {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
        return newState.build();
    }

    public static void markJobAsDeleting(String jobId, boolean force, ClusterService clusterService, ActionListener<Boolean> listener) {
        clusterService.submitStateUpdateTask("mark-job-as-deleted", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData tasks = currentState.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                MlMetadata.Builder builder = new MlMetadata.Builder(MlMetadata.getMlMetadata(currentState));
                builder.markJobAsDeleting(jobId, tasks, force);
                return buildNewClusterState(currentState, builder);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(true);
            }
        });
    }

    public static void deleteJob(DeleteJobAction.Request request, ClusterService clusterService, ActionListener<Boolean> listener) {
        String jobId = request.getJobId();

        clusterService.submitStateUpdateTask(
                "delete-job-" + jobId,
                new AckedClusterStateUpdateTask<Boolean>(request, listener) {

                    @Override
                    protected Boolean newResponse(boolean acknowledged) {
                        return acknowledged;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        MlMetadata currentMlMetadata = MlMetadata.getMlMetadata(currentState);
                        if (currentMlMetadata.getJobs().containsKey(jobId) == false) {
                            // We wouldn't have got here if the job never existed so
                            // the Job must have been deleted by another action.
                            // Don't error in this case
                            return currentState;
                        }

                        MlMetadata.Builder builder = new MlMetadata.Builder(currentMlMetadata);
                        builder.deleteJob(jobId, currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
                        return buildNewClusterState(currentState, builder);
                    }
                });
    }
}
