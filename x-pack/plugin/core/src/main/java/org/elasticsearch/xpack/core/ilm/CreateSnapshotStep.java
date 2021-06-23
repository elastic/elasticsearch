/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.util.Locale;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Creates a snapshot of the managed index into the configured repository and snapshot name. The repository and snapshot names are expected
 * to be present in the lifecycle execution state (usually generated and stored by a different ILM step)
 */
public class CreateSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "create-snapshot";

    private static final Logger logger = LogManager.getLogger(CreateSnapshotStep.class);

    public CreateSnapshotStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Boolean> listener) {
        final String indexName = indexMetadata.getIndex().getName();

        final LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);

        final String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        final String snapshotRepository = lifecycleState.getSnapshotRepository();
        if (Strings.hasText(snapshotRepository) == false) {
            listener.onFailure(new IllegalStateException("snapshot repository is not present for policy [" + policyName + "] and index [" +
                indexName + "]"));
            return;
        }

        final String snapshotName = lifecycleState.getSnapshotName();
        if (Strings.hasText(snapshotName) == false) {
            listener.onFailure(
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            return;
        }
        CreateSnapshotRequest request = new CreateSnapshotRequest(snapshotRepository, snapshotName);
        request.indices(indexName);
        // this is safe as the snapshot creation will still be async, it's just that the listener will be notified when the snapshot is
        // complete
        request.waitForCompletion(true);
        request.includeGlobalState(false);
        request.masterNodeTimeout(TimeValue.MAX_VALUE);
        getClient().admin().cluster().createSnapshot(request,
            ActionListener.wrap(response -> {
                logger.debug("create snapshot response for policy [{}] and index [{}] is: {}", policyName, indexName,
                    Strings.toString(response));
                final SnapshotInfo snapInfo = response.getSnapshotInfo();

                // Check that there are no failed shards, since the request may not entirely
                // fail, but may still have failures (such as in the case of an aborted snapshot)
                if (snapInfo.failedShards() == 0) {
                    listener.onResponse(true);
                } else {
                    int failures = snapInfo.failedShards();
                    int total = snapInfo.totalShards();
                    String message = String.format(Locale.ROOT,
                        "failed to create snapshot successfully, %s failures out of %s total shards failed", failures, total);
                    logger.warn(message);
                    ElasticsearchException failure = new ElasticsearchException(message,
                        snapInfo.shardFailures().stream().findFirst().orElse(null));
                    listener.onFailure(failure);
                }
            }, listener::onFailure));
    }
}
