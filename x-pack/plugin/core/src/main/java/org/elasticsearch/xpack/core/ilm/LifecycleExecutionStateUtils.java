/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import java.util.Objects;

/**
 * A utility class used for index lifecycle execution states
 */
public class LifecycleExecutionStateUtils {

    private LifecycleExecutionStateUtils() {}

    /**
     * Given a cluster state, index, and lifecycle state, return a new cluster state where
     * the lifecycle state will be associated with the given index.
     *
     * The index associated with the passed-in index metadata must already exist in the cluster state,
     * this method cannot be used to add an index.
     *
     *  See also {@link Metadata#withLifecycleState}.
     */
    public static ClusterState newClusterStateWithLifecycleState(
        final ClusterState clusterState,
        final Index index,
        final LifecycleExecutionState lifecycleState
    ) {
        Objects.requireNonNull(clusterState, "clusterState must not be null");
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(lifecycleState, "lifecycleState must not be null");

        final Metadata metadata = clusterState.metadata().withLifecycleState(index, lifecycleState);
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

}
