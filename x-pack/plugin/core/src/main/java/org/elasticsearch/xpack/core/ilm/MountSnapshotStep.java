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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Restores the snapshot created for the designated index via the ILM policy to an index named using the provided prefix appended to the
 * designated index name.
 */
public class MountSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "mount-snapshot";

    private static final Logger logger = LogManager.getLogger(MountSnapshotStep.class);

    private final String restoredIndexPrefix;
    private final MountSearchableSnapshotRequest.Storage storageType;

    public MountSnapshotStep(StepKey key, StepKey nextStepKey, Client client, String restoredIndexPrefix,
                             MountSearchableSnapshotRequest.Storage storageType) {
        super(key, nextStepKey, client);
        this.restoredIndexPrefix = restoredIndexPrefix;
        this.storageType = Objects.requireNonNull(storageType, "a storage type must be specified");
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public String getRestoredIndexPrefix() {
        return restoredIndexPrefix;
    }

    public MountSearchableSnapshotRequest.Storage getStorage() {
        return storageType;
    }

    @Override
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Boolean> listener) {
        String indexName = indexMetadata.getIndex().getName();

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
        if (currentClusterState.metadata().index(mountedIndexName) != null) {
            logger.debug("mounted index [{}] for policy [{}] and index [{}] already exists. will not attempt to mount the index again",
                mountedIndexName, policyName, indexName);
            listener.onResponse(true);
            return;
        }

        final String snapshotIndexName = lifecycleState.getSnapshotIndexName();
        if (snapshotIndexName == null) {
            // This index had its searchable snapshot created prior to a version where we captured
            // the original index name, so make our best guess at the name
            indexName = bestEffortIndexNameResolution(indexName);
            logger.debug("index [{}] using policy [{}] does not have a stored snapshot index name, " +
                "using our best effort guess of [{}] for the original snapshotted index name",
                indexMetadata.getIndex().getName(), policyName, indexName);
        } else {
            // Use the name of the snapshot as specified in the metadata, because the current index
            // name not might not reflect the name of the index actually in the snapshot
            logger.debug("index [{}] using policy [{}] has a different name [{}] within the snapshot to be restored, " +
                "using the snapshot index name from generated metadata for mounting", indexName, policyName, snapshotIndexName);
            indexName = snapshotIndexName;
        }

        final Settings.Builder settingsBuilder = Settings.builder();

        overrideTierPreference(this.getKey().getPhase())
            .ifPresent(override -> settingsBuilder.put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, override));

        final MountSearchableSnapshotRequest mountSearchableSnapshotRequest = new MountSearchableSnapshotRequest(mountedIndexName,
            snapshotRepository, snapshotName, indexName, settingsBuilder.build(),
            // we captured the index metadata when we took the snapshot. the index likely had the ILM execution state in the metadata.
            // if we were to restore the lifecycle.name setting, the restored index would be captured by the ILM runner and,
            // depending on what ILM execution state was captured at snapshot time, make it's way forward from _that_ step forward in
            // the ILM policy.
            // we'll re-set this setting on the restored index at a later step once we restored a deterministic execution state
            new String[]{LifecycleSettings.LIFECYCLE_NAME},
            // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't
            // perform expensive operations (ie. clusterStateProcessed)
            false,
            storageType);
        mountSearchableSnapshotRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
        getClient().execute(MountSearchableSnapshotAction.INSTANCE, mountSearchableSnapshotRequest,
            ActionListener.wrap(response -> {
                if (response.status() != RestStatus.OK && response.status() != RestStatus.ACCEPTED) {
                    logger.debug("mount snapshot response failed to complete");
                    throw new ElasticsearchException("mount snapshot response failed to complete, got response " + response.status());
                }
                listener.onResponse(true);
            }, listener::onFailure));
    }

    /**
     * Tries to guess the original index name given the current index name, tries to drop the
     * "partial-" and "restored-" prefixes, since those are what ILM uses. Does not handle
     * unorthodox cases like "restored-partial-[indexname]" since this is not intended to be
     * exhaustive.
     */
    static String bestEffortIndexNameResolution(String indexName) {
        String originalName = indexName.replaceFirst("^" + SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX, "");
        originalName = originalName.replaceFirst("^" + SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX, "");
        return originalName;
    }

    /**
     * return the tier preference to use or empty to use default.
     * @param phase the phase the step will run in.
     * @return tier preference override or empty.
     */
    static Optional<String> overrideTierPreference(String phase) {
        // if we are mounting a searchable snapshot in the hot phase, then the index should be pinned to the hot nodes
        if (TimeseriesLifecycleType.HOT_PHASE.equals(phase)) {
            return Optional.of(DataTier.DATA_HOT);
        }
        return Optional.empty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), restoredIndexPrefix, storageType);
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
        return super.equals(obj) &&
            Objects.equals(restoredIndexPrefix, other.restoredIndexPrefix) &&
            Objects.equals(storageType, other.storageType);
    }
}
