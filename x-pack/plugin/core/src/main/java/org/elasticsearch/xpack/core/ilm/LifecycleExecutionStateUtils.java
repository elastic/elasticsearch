/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * A utility class used for index lifecycle execution states
 */
public class LifecycleExecutionStateUtils {

    private LifecycleExecutionStateUtils() {}

    /**
     * Given a cluster state, index metadata, and lifecycle state, return a new cluster state where
     * the lifecycle state will be associated with the given index metadata.
     */
    public static ClusterState newClusterStateWithLifecycleState(
        ClusterState clusterState,
        IndexMetadata indexMetadata,
        LifecycleExecutionState lifecycleState
    ) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        builder.metadata(
            Metadata.builder(clusterState.getMetadata())
                .put(IndexMetadata.builder(indexMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap()))
        );
        return builder.build();
    }

}
