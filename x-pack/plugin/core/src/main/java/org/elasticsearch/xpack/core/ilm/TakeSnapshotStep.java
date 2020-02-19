/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

public class TakeSnapshotStep extends AsyncActionStep {
    public static final String NAME = "take-snapshot";

    private final String snapshotRepository;

    public TakeSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String snapshotRepository) {
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
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            return;
        }
        CreateSnapshotRequest request = new CreateSnapshotRequest(snapshotRepository, snapshotName);
        // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't perform
        // expensive operations (ie. clusterStateProcessed)
        request.waitForCompletion(false);
        // TODO should we expose this?
        request.includeGlobalState(false);
        getClient().admin().cluster().createSnapshot(request, new ActionListener<>() {
            @Override
            public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                listener.onResponse(true);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
