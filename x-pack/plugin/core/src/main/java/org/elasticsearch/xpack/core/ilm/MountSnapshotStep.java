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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.Objects;
import java.util.Optional;

/**
 * Restores the snapshot created for the designated index via the ILM policy to an index named using the provided prefix appended to the
 * designated index name.
 */
public class MountSnapshotStep extends AsyncRetryDuringSnapshotActionStep {
    public static final String NAME = "mount-snapshot";

    private static final Logger logger = LogManager.getLogger(MountSnapshotStep.class);

    private final String restoredIndexPrefix;
    private final MountSearchableSnapshotRequest.Storage storageType;

    public MountSnapshotStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        String restoredIndexPrefix,
        MountSearchableSnapshotRequest.Storage storageType
    ) {
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
    void performDuringNoSnapshot(IndexMetadata indexMetadata, ClusterState currentClusterState, ActionListener<Void> listener) {
        String indexName = indexMetadata.getIndex().getName();

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        SearchableSnapshotAction.SearchableSnapshotMetadata searchableSnapshotMetadata = SearchableSnapshotAction
            .extractSearchableSnapshotFromSettings(indexMetadata);

        String policyName = indexMetadata.getLifecyclePolicyName();
        String snapshotRepository = lifecycleState.snapshotRepository();
        if (Strings.hasText(snapshotRepository) == false) {
            if (searchableSnapshotMetadata == null) {
                listener.onFailure(
                    new IllegalStateException(
                        "snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]"
                    )
                );
                return;
            } else {
                snapshotRepository = searchableSnapshotMetadata.repositoryName();
            }
        }

        String snapshotName = lifecycleState.snapshotName();
        if (Strings.hasText(snapshotName) == false && searchableSnapshotMetadata == null) {
            listener.onFailure(
                new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]")
            );
            return;
        } else if (searchableSnapshotMetadata != null) {
            snapshotName = searchableSnapshotMetadata.snapshotName();
        }

        String mountedIndexName = restoredIndexPrefix + indexName;
        if (currentClusterState.metadata().index(mountedIndexName) != null) {
            logger.debug(
                "mounted index [{}] for policy [{}] and index [{}] already exists. will not attempt to mount the index again",
                mountedIndexName,
                policyName,
                indexName
            );
            listener.onResponse(null);
            return;
        }

        final String snapshotIndexName = lifecycleState.snapshotIndexName();
        if (snapshotIndexName == null) {
            if (searchableSnapshotMetadata == null) {
                // This index had its searchable snapshot created prior to a version where we captured
                // the original index name, so make our best guess at the name
                indexName = bestEffortIndexNameResolution(indexName);
                logger.debug(
                    "index [{}] using policy [{}] does not have a stored snapshot index name, "
                        + "using our best effort guess of [{}] for the original snapshotted index name",
                    indexMetadata.getIndex().getName(),
                    policyName,
                    indexName
                );
            } else {
                indexName = searchableSnapshotMetadata.sourceIndex();
            }
        } else {
            // Use the name of the snapshot as specified in the metadata, because the current index
            // name not might not reflect the name of the index actually in the snapshot
            logger.debug(
                "index [{}] using policy [{}] has a different name [{}] within the snapshot to be restored, "
                    + "using the snapshot index name from generated metadata for mounting",
                indexName,
                policyName,
                snapshotIndexName
            );
            indexName = snapshotIndexName;
        }

        final Settings.Builder settingsBuilder = Settings.builder();

        overrideTierPreference(this.getKey().phase()).ifPresent(override -> settingsBuilder.put(DataTier.TIER_PREFERENCE, override));

        final MountSearchableSnapshotRequest mountSearchableSnapshotRequest = new MountSearchableSnapshotRequest(
            mountedIndexName,
            snapshotRepository,
            snapshotName,
            indexName,
            settingsBuilder.build(),
            ignoredIndexSettings(this.getKey().phase()),
            // we'll not wait for the snapshot to complete in this step as the async steps are executed from threads that shouldn't
            // perform expensive operations (ie. clusterStateProcessed)
            false,
            storageType
        );
        mountSearchableSnapshotRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
        getClient().execute(
            MountSearchableSnapshotAction.INSTANCE,
            mountSearchableSnapshotRequest,
            listener.delegateFailureAndWrap((l, response) -> {
                if (response.status() != RestStatus.OK && response.status() != RestStatus.ACCEPTED) {
                    logger.debug("mount snapshot response failed to complete");
                    throw new ElasticsearchException("mount snapshot response failed to complete, got response " + response.status());
                }
                l.onResponse(null);
            })
        );
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

    /**
     * This method returns the settings that need to be ignored when we mount the searchable snapshot. Currently, it returns:
     * - index.lifecycle.name: The index likely had the ILM execution state in the metadata. If we were to restore the lifecycle.name
     * setting, the restored index would be captured by the ILM runner and, depending on what ILM execution state was captured at snapshot
     * time, make it's way forward from _that_ step forward in the ILM policy. We'll re-set this setting on the restored index at a later
     * step once we restored a deterministic execution state
     * - index.routing.allocation.total_shards_per_node: It is  likely that frozen tier has fewer nodes than the hot tier.
     * Keeping this setting runs the risk that we will not have enough nodes to allocate all the shards in the
     * frozen tier and the user does not have any way of fixing this. For this reason, we ignore this setting when moving to frozen.
     */
    static String[] ignoredIndexSettings(String phase) {
        // if we are mounting a searchable snapshot in the hot phase, then we should not change the total_shards_per_node setting
        if (TimeseriesLifecycleType.FROZEN_PHASE.equals(phase)) {
            return new String[] {
                LifecycleSettings.LIFECYCLE_NAME,
                ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey() };
        }
        return new String[] { LifecycleSettings.LIFECYCLE_NAME };
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
        return super.equals(obj)
            && Objects.equals(restoredIndexPrefix, other.restoredIndexPrefix)
            && Objects.equals(storageType, other.storageType);
    }
}
