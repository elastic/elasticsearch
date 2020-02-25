/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotMissingException;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Deletes the snapshot designated by the snapshot name present in the lifecycle execution state, hosted in the configured repository.
 */
public class CleanupSnapshotStep extends AsyncActionStep {
    public static final String NAME = "cleanup-snapshot";

    private final String snapshotRepository;

    public CleanupSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String snapshotRepository) {
        super(key, nextStepKey, client);
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState, ClusterStateObserver observer,
                              Listener listener) {
        final String indexName = indexMetaData.getIndex().getName();

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetaData);
        final String snapshotName = lifecycleState.getSnapshotName();
        if (Strings.hasText(snapshotName) == false) {
            String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            listener.onFailure(
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]")
            );
            return;
        }
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest(snapshotRepository, snapshotName);
        getClient().admin().cluster().deleteSnapshot(deleteSnapshotRequest, new ActionListener<>() {

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                listener.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof SnapshotMissingException) {
                    // during the happy flow we generate a snapshot name and that snapshot doesn't exist in the repository
                    listener.onResponse(true);
                } else {
                    if (e instanceof RepositoryMissingException) {
                        String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
                        listener.onFailure(new IllegalStateException("repository [" + snapshotRepository + "] is missing. [" + policyName +
                            "] policy for index [" + indexName + "] cannot continue until the repository is created", e));
                    } else {
                        listener.onFailure(e);
                    }
                }
            }
        });
    }
}
