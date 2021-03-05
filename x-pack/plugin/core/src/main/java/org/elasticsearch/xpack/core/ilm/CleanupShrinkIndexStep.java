/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Deletes the index identified by the shrink index name stored in the lifecycle state of the managed index (if any was generated)
 */
public class CleanupShrinkIndexStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "cleanup-shrink-index";

    public CleanupShrinkIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, Listener listener) {
        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);
        final String shrinkIndexName = lifecycleState.getShrinkIndexName();
        // if the shrink index was not generated there is nothing to delete so we move on
        if (Strings.hasText(shrinkIndexName) == false) {
            listener.onResponse(true);
            return;
        }
        getClient().admin().indices()
            .delete(new DeleteIndexRequest(shrinkIndexName).masterNodeTimeout(getMasterTimeout(currentClusterState)),
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

}
