/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotException;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

public class CreateSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "create-snapshot";

    private final String snapshotRepository;

    public CreateSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String snapshotRepository) {
        super(key, nextStepKey, client);
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetaData indexMetaData, ClusterState currentClusterState, Listener listener) {
        final String indexName = indexMetaData.getIndex().getName();

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetaData);

        final String snapshotName = lifecycleState.getSnapshotName();
        String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        if (Strings.hasText(snapshotName) == false) {
            listener.onFailure(
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            return;
        }
        CreateSnapshotRequest request = new CreateSnapshotRequest(snapshotRepository, snapshotName);
        // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't perform
        // expensive operations (ie. clusterStateProcessed)
        request.waitForCompletion(false);
        request.includeGlobalState(false);
        request.masterNodeTimeout(getMasterTimeout(currentClusterState));
        getClient().admin().cluster().createSnapshot(request,
            ActionListener.wrap(response -> {
                if (response.status().equals(RestStatus.INTERNAL_SERVER_ERROR)) {
                    listener.onFailure(new SnapshotException(snapshotRepository, snapshotName,
                        "unable to request snapshot creation [" + snapshotName + "] for index [ " + indexName + "] as part of policy [" +
                            policyName + "] execution due to an internal server error"));
                } else {
                    listener.onResponse(true);
                }
            }, listener::onFailure));
    }
}
