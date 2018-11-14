/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlMetadata;
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
}
