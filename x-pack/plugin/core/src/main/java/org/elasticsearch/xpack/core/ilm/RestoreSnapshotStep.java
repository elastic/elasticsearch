/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Restores the snapshot created for the designated index via the ILM policy to an index named using the provided prefix appended to the
 * designated index name.
 */
public class RestoreSnapshotStep extends AsyncActionStep {
    public static final String NAME = "restore-snapshot";

    private final String snapshotRepository;
    private final String restoredIndexPrefix;

    public RestoreSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String snapshotRepository, String restoredIndexPrefix) {
        super(key, nextStepKey, client);
        this.snapshotRepository = snapshotRepository;
        this.restoredIndexPrefix = restoredIndexPrefix;
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
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            return;
        }
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(snapshotRepository, snapshotName);
        // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't perform
        // expensive operations (ie. clusterStateProcessed)
        restoreSnapshotRequest.waitForCompletion(false);
        restoreSnapshotRequest.indices(indexName);
        restoreSnapshotRequest.renamePattern(indexName);
        restoreSnapshotRequest.renameReplacement(restoredIndexPrefix + indexName);
        // we captured the index metadata when we took the snapshot. the index likely had the ILM execution state in the metadata.
        // if we were to restore the lifecycle.name setting, the restored index would be captured by the ILM runner and, depending on what
        // ILM execution state was captured at snapshot time, make it's way forward from _that_ step forward in the ILM policy.
        // we'll re-set this setting on the restored index at a later step once we restored a deterministic execution state
        restoreSnapshotRequest.ignoreIndexSettings(LifecycleSettings.LIFECYCLE_NAME);
        restoreSnapshotRequest.includeAliases(false);

        getClient().admin().cluster().restoreSnapshot(restoreSnapshotRequest, new ActionListener<>() {
            @Override
            public void onResponse(RestoreSnapshotResponse response) {
                listener.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
