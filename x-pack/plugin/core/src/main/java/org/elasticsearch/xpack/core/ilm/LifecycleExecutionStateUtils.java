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
import org.elasticsearch.index.Index;

import java.util.Objects;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * A utility class used for index lifecycle execution states
 */
public class LifecycleExecutionStateUtils {

    private LifecycleExecutionStateUtils() {}

    /**
     * Given a cluster state, index metadata, and lifecycle state, return a new cluster state where
     * the lifecycle state will be associated with the given index metadata.
     *
     * The index associated with the passed-in index metadata must already exist in the cluster state,
     * this method cannot be used to add an index.
     *
     * The passed-in index metadata does not have to be the same object as the one that is already a
     * part of this cluster state, however, if it is the same object then additional optimization is
     * possible (see {@link Metadata#withLifecycleState}).
     */
    public static ClusterState newClusterStateWithLifecycleState(
        final ClusterState clusterState,
        final IndexMetadata indexMetadata,
        final LifecycleExecutionState lifecycleState
    ) {
        Objects.requireNonNull(clusterState, "clusterState must not be null");
        Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
        Objects.requireNonNull(lifecycleState, "lifecycleState must not be null");

        // the index associated with this indexMetadata must already exist
        final Index index = indexMetadata.getIndex();
        final IndexMetadata existingIndexMetadata = clusterState.metadata().index(index);
        if (existingIndexMetadata == null) {
            throw new IllegalArgumentException("index " + index.getName() + " must exist in the cluster state");
        }

        final Metadata metadata;
        if (existingIndexMetadata == indexMetadata) {
            // optimization, the only thing that has changed is that we're swapping one lifecycle state for another
            metadata = clusterState.metadata().withLifecycleState(indexMetadata.getIndex(), lifecycleState);
        } else {
            // a change has been made to the indexMetadata also, we must run through the whole metadata builder cycle
            metadata = Metadata.builder(clusterState.metadata())
                .put(IndexMetadata.builder(indexMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap()))
                .build();
        }
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

}
