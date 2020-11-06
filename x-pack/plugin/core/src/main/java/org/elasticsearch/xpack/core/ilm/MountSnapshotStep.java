/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Restores the snapshot created for the designated index via the ILM policy to an index named using the provided prefix appended to the
 * designated index name.
 */
public class MountSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "mount-snapshot";

    private static final Logger logger = LogManager.getLogger(MountSnapshotStep.class);

    private final String restoredIndexPrefix;

    public MountSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String restoredIndexPrefix) {
        super(key, nextStepKey, client);
        this.restoredIndexPrefix = restoredIndexPrefix;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public String getRestoredIndexPrefix() {
        return restoredIndexPrefix;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, Listener listener) {
        final String indexName = indexMetadata.getIndex().getName();

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);

        String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
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

        String mountedIndexName = restoredIndexPrefix + indexName;
        if(currentClusterState.metadata().index(mountedIndexName) != null) {
            logger.debug("mounted index [{}] for policy [{}] and index [{}] already exists. will not attempt to mount the index again",
                mountedIndexName, policyName, indexName);
            listener.onResponse(true);
            return;
        }

        final MountSearchableSnapshotRequest mountSearchableSnapshotRequest = new MountSearchableSnapshotRequest(mountedIndexName,
            snapshotRepository, snapshotName, indexName, Settings.builder()
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString())
            .build(),
            // we captured the index metadata when we took the snapshot. the index likely had the ILM execution state in the metadata.
            // if we were to restore the lifecycle.name setting, the restored index would be captured by the ILM runner and,
            // depending on what ILM execution state was captured at snapshot time, make it's way forward from _that_ step forward in
            // the ILM policy.
            // we'll re-set this setting on the restored index at a later step once we restored a deterministic execution state
            new String[]{LifecycleSettings.LIFECYCLE_NAME},
            // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't
            // perform expensive operations (ie. clusterStateProcessed)
            false);
        getClient().execute(MountSearchableSnapshotAction.INSTANCE, mountSearchableSnapshotRequest,
            ActionListener.wrap(response -> {
                if (response.status() != RestStatus.OK && response.status() != RestStatus.ACCEPTED) {
                    logger.debug("mount snapshot response failed to complete");
                    throw new ElasticsearchException("mount snapshot response failed to complete, got response " + response.status());
                }
                listener.onResponse(true);
            }, listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), restoredIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MountSnapshotStep other = (MountSnapshotStep) obj;
        return super.equals(obj) && Objects.equals(restoredIndexPrefix, other.restoredIndexPrefix);
    }
}
