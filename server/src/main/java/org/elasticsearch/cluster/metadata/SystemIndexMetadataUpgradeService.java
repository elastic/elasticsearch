/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A service responsible for updating the metadata used by system indices.
 *
 * Mapping updates are handled by {@link org.elasticsearch.indices.SystemIndexManager}.
 */
public class SystemIndexMetadataUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexMetadataUpgradeService.class);

    private final SystemIndices systemIndices;
    private final ClusterService clusterService;

    private boolean master = false;

    private volatile Map<String, IndexMetadata> lastIndexMetadataMap = ImmutableOpenMap.of();
    private volatile boolean updateTaskPending = false;

    public SystemIndexMetadataUpgradeService(SystemIndices systemIndices, ClusterService clusterService) {
        this.systemIndices = systemIndices;
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() != master) {
            this.master = event.localNodeMaster();
        }

        if (master && updateTaskPending == false) {
            final Map<String, IndexMetadata> indexMetadataMap = event.state().metadata().indices();

            if (lastIndexMetadataMap != indexMetadataMap) {
                for (Map.Entry<String, IndexMetadata> cursor : indexMetadataMap.entrySet()) {
                    if (cursor.getValue() != lastIndexMetadataMap.get(cursor.getKey())) {
                        IndexMetadata indexMetadata = cursor.getValue();
                        boolean requiresUpdate = requiresUpdate(indexMetadata);
                        if (requiresUpdate) {
                            updateTaskPending = true;
                            submitUnbatchedTask(
                                "system_index_metadata_upgrade_service {system metadata change}",
                                new SystemIndexMetadataUpdateTask()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    // package-private for testing
    boolean requiresUpdate(IndexMetadata indexMetadata) {
        final boolean shouldBeSystem = shouldBeSystem(indexMetadata);

        // should toggle system index status
        if (shouldBeSystem != indexMetadata.isSystem()) {
            return true;
        }

        if (shouldBeSystem) {
            return isVisible(indexMetadata) || hasVisibleAlias(indexMetadata);
        }

        return false;
    }

    // package-private for testing
    boolean isVisible(IndexMetadata indexMetadata) {
        return indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false) == false;
    }

    // package-private for testing
    boolean shouldBeSystem(IndexMetadata indexMetadata) {
        return systemIndices.isSystemIndex(indexMetadata.getIndex())
            || systemIndices.isSystemIndexBackingDataStream(indexMetadata.getIndex().getName());
    }

    // package-private for testing
    boolean hasVisibleAlias(IndexMetadata indexMetadata) {
        return indexMetadata.getAliases().values().stream().anyMatch(a -> Boolean.FALSE.equals(a.isHidden()));
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    // visible for testing
    SystemIndexMetadataUpdateTask getTask() {
        return new SystemIndexMetadataUpdateTask();
    }

    public class SystemIndexMetadataUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            final Map<String, IndexMetadata> indexMetadataMap = currentState.metadata().indices();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();
            for (Map.Entry<String, IndexMetadata> entry : indexMetadataMap.entrySet()) {
                final IndexMetadata indexMetadata = entry.getValue();
                if (indexMetadata != lastIndexMetadataMap.get(entry.getKey())) {
                    final boolean shouldBeSystem = shouldBeSystem(indexMetadata);
                    IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                    boolean updated = false;
                    if (shouldBeSystem != indexMetadata.isSystem()) {
                        builder.system(indexMetadata.isSystem() == false);
                        updated = true;
                    }
                    if (shouldBeSystem && isVisible(indexMetadata)) {
                        builder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true));
                        builder.settingsVersion(builder.settingsVersion() + 1);
                        updated = true;
                    }
                    if (shouldBeSystem && hasVisibleAlias(indexMetadata)) {
                        for (AliasMetadata aliasMetadata : indexMetadata.getAliases().values()) {
                            if (Boolean.FALSE.equals(aliasMetadata.isHidden())) {
                                builder.removeAlias(aliasMetadata.alias());
                                builder.putAlias(
                                    AliasMetadata.builder(aliasMetadata.alias())
                                        .filter(aliasMetadata.filter())
                                        .indexRouting(aliasMetadata.indexRouting())
                                        .isHidden(true)
                                        .searchRouting(aliasMetadata.searchRouting())
                                        .writeIndex(aliasMetadata.writeIndex())
                                );
                            }
                        }
                    }
                    if (updated) {
                        updatedMetadata.add(builder.build());
                    }
                }
            }

            if (updatedMetadata.isEmpty() == false) {
                final Metadata.Builder builder = Metadata.builder(currentState.metadata());
                updatedMetadata.forEach(idxMeta -> builder.put(idxMeta, true));
                return ClusterState.builder(currentState).metadata(builder).build();
            }
            return currentState;
        }

        @Override
        public void onFailure(Exception e) {
            updateTaskPending = false;
            logger.error("failed to update system index metadata", e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            lastIndexMetadataMap = newState.metadata().indices();
            updateTaskPending = false;
        }
    }
}
