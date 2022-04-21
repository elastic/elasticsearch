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
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
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
 */
public class SystemIndexMetadataUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexMetadataUpgradeService.class);

    private final SystemIndices systemIndices;
    private final ClusterService clusterService;

    private boolean master = false;

    private volatile ImmutableOpenMap<String, IndexMetadata> lastIndexMetadataMap = ImmutableOpenMap.of();
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
            final ImmutableOpenMap<String, IndexMetadata> indexMetadataMap = event.state().metadata().indices();

            if (lastIndexMetadataMap != indexMetadataMap) {
                for (Map.Entry<String, IndexMetadata> cursor : indexMetadataMap.entrySet()) {
                    if (cursor.getValue() != lastIndexMetadataMap.get(cursor.getKey())) {
                        final boolean isSystem = systemIndices.isSystemIndex(cursor.getValue().getIndex())
                            || systemIndices.isSystemIndexBackingDataStream(cursor.getValue().getIndex().getName());
                        if (isSystem != cursor.getValue().isSystem()) {
                            updateTaskPending = true;
                            clusterService.submitStateUpdateTask(
                                "system_index_metadata_upgrade_service {system metadata change}",
                                new SystemIndexMetadataUpdateTask(),
                                newExecutor()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> newExecutor() {
        return ClusterStateTaskExecutor.unbatched();
    }

    // visible for testing
    SystemIndexMetadataUpdateTask getTask() {
        return new SystemIndexMetadataUpdateTask();
    }

    public class SystemIndexMetadataUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            final ImmutableOpenMap<String, IndexMetadata> indexMetadataMap = currentState.metadata().indices();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();
            for (Map.Entry<String, IndexMetadata> cursor : indexMetadataMap.entrySet()) {
                final IndexMetadata indexMetadata = cursor.getValue();
                if (indexMetadata != lastIndexMetadataMap.get(cursor.getKey())) {
                    final boolean isSystem = systemIndices.isSystemIndex(indexMetadata.getIndex())
                        || systemIndices.isSystemIndexBackingDataStream(indexMetadata.getIndex().getName());
                    IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                    boolean updated = false;
                    if (isSystem != indexMetadata.isSystem()) {
                        builder.system(indexMetadata.isSystem() == false);
                        updated = true;
                    }
                    boolean isHidden = indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false);
                    if (isSystem && isHidden == false) {
                        builder.settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true));
                        builder.settingsVersion(builder.settingsVersion() + 1);
                        updated = true;
                    }
                    if (isSystem && indexMetadata.getAliases().values().stream().anyMatch(a -> Boolean.FALSE.equals(a.isHidden()))) {
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
