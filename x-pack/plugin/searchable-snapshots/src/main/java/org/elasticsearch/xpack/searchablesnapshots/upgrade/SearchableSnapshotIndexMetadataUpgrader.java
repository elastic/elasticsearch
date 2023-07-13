/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.upgrade;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class upgrades frozen indices to apply the index.shard_limit.group=frozen setting after all nodes have been upgraded to 7.13+
 */
public class SearchableSnapshotIndexMetadataUpgrader {
    private static final Logger logger = LogManager.getLogger(SearchableSnapshotIndexMetadataUpgrader.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final AtomicBoolean upgraded = new AtomicBoolean();
    private final ClusterStateListener listener = this::clusterChanged;

    public SearchableSnapshotIndexMetadataUpgrader(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void initialize() {
        clusterService.addListener(listener);
    }

    private void clusterChanged(ClusterChangedEvent event) {
        if (upgraded.get()) {
            return;
        }

        if (event.localNodeMaster() && event.state().nodes().getMinNodeVersion().onOrAfter(Version.V_7_13_0)) {
            // only want one doing this at a time, assume it succeeds and reset if not.
            if (upgraded.compareAndSet(false, true)) {
                final Executor executor = threadPool.generic();
                executor.execute(() -> maybeUpgradeIndices(event.state()));
            }
        }
    }

    private void maybeUpgradeIndices(ClusterState state) {
        // 99% of the time, this will be a noop, so precheck that before adding a cluster state update.
        if (needsUpgrade(state)) {
            logger.info("Upgrading partial searchable snapshots to use frozen shard limit group");
            submitUnbatchedTask("searchable-snapshot-index-upgrader", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return upgradeIndices(currentState);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    clusterService.removeListener(listener);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(
                        "upgrading frozen indices to have frozen shard limit group failed, will retry on the next cluster state update",
                        e
                    );
                    // let us try again later.
                    upgraded.set(false);
                }
            });
        } else {
            clusterService.removeListener(listener);
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static boolean needsUpgrade(ClusterState state) {
        return state.metadata()
            .stream()
            .filter(
                imd -> imd.getCompatibilityVersion().onOrAfter(IndexVersion.V_7_12_0)
                    && imd.getCompatibilityVersion().before(IndexVersion.V_8_0_0)
            )
            .filter(IndexMetadata::isPartialSearchableSnapshot)
            .map(IndexMetadata::getSettings)
            .anyMatch(SearchableSnapshotIndexMetadataUpgrader::notFrozenShardLimitGroup);
    }

    static ClusterState upgradeIndices(ClusterState currentState) {
        if (needsUpgrade(currentState) == false) {
            return currentState;
        }
        Metadata.Builder builder = Metadata.builder(currentState.metadata());
        currentState.metadata()
            .stream()
            .filter(
                imd -> imd.getCompatibilityVersion().onOrAfter(IndexVersion.V_7_12_0)
                    && imd.getCompatibilityVersion().before(IndexVersion.V_8_0_0)
            )
            .filter(imd -> imd.isPartialSearchableSnapshot() && notFrozenShardLimitGroup(imd.getSettings()))
            .map(SearchableSnapshotIndexMetadataUpgrader::setShardLimitGroupFrozen)
            .forEach(imd -> builder.put(imd, true));
        return ClusterState.builder(currentState).metadata(builder).build();
    }

    private static boolean notFrozenShardLimitGroup(org.elasticsearch.common.settings.Settings settings) {
        return ShardLimitValidator.FROZEN_GROUP.equals(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.get(settings)) == false;
    }

    private static IndexMetadata setShardLimitGroupFrozen(IndexMetadata indexMetadata) {
        return IndexMetadata.builder(indexMetadata)
            .settings(
                Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP.getKey(), ShardLimitValidator.FROZEN_GROUP)
            )
            .settingsVersion(indexMetadata.getSettingsVersion() + 1)
            .build();
    }
}
