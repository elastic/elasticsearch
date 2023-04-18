/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotMissingException;

/**
 * Deletes the snapshot designated by the repository and snapshot name present in the lifecycle execution state.
 */
public class CleanupSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "cleanup-snapshot";

    public CleanupSnapshotStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener) {
        final String indexName = indexMetadata.getIndex().getName();

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        final String repositoryName = lifecycleState.snapshotRepository();
        // if the snapshot information is missing from the ILM execution state there is nothing to delete so we move on
        if (Strings.hasText(repositoryName) == false) {
            listener.onResponse(null);
            return;
        }
        final String snapshotName = lifecycleState.snapshotName();
        if (Strings.hasText(snapshotName) == false) {
            listener.onResponse(null);
            return;
        }
        getClient().admin()
            .cluster()
            .prepareDeleteSnapshot(repositoryName, snapshotName)
            .setMasterNodeTimeout(TimeValue.MAX_VALUE)
            .execute(new ActionListener<>() {

                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    assert acknowledgedResponse.isAcknowledged();
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof SnapshotMissingException) {
                        // during the happy flow we generate a snapshot name and that snapshot doesn't exist in the repository
                        listener.onResponse(null);
                    } else {
                        if (e instanceof RepositoryMissingException) {
                            String policyName = indexMetadata.getLifecyclePolicyName();
                            listener.onFailure(
                                new IllegalStateException(
                                    "repository ["
                                        + repositoryName
                                        + "] is missing. ["
                                        + policyName
                                        + "] policy for index ["
                                        + indexName
                                        + "] cannot continue until the repository is created",
                                    e
                                )
                            );
                        } else {
                            listener.onFailure(e);
                        }
                    }
                }
            });
    }
}
