/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.reservedstate.PostTransformResult;

import java.util.Collection;

/**
 * Generic task to update the reserved keys from any {@link PostTransformResult}
 *
 * <p>
 * Any post transformation result can supply an updated set of reserved keys that need to be
 * saved into the reserved state handler section of the cluster state. This task does the update.
 */
public class ReservedStateUpdateKeysTask implements ClusterStateTaskListener {
    private final ActionListener<ActionResponse.Empty> listener;
    private final String namespace;
    private final Collection<PostTransformResult> updatedKeysCollection;

    public ReservedStateUpdateKeysTask(
        String namespace,
        Collection<PostTransformResult> updatedKeysCollection,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.listener = listener;
        this.namespace = namespace;
        this.updatedKeysCollection = updatedKeysCollection;
    }

    ActionListener<ActionResponse.Empty> listener() {
        return listener;
    }

    protected ClusterState execute(final ClusterState currentState) {
        var reservedMetadataBuilder = new ReservedStateMetadata.Builder(currentState.metadata().reservedStateMetadata().get(namespace));

        // iterate over all post transform results, update the specific handler keys
        for (var result : updatedKeysCollection) {
            reservedMetadataBuilder.putHandler(new ReservedStateHandlerMetadata(result.handlerName(), result.updatedKeys()));
        }

        // return the updated cluster state
        ClusterState.Builder stateBuilder = new ClusterState.Builder(currentState);
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(reservedMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }
}
