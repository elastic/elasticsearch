/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;

import java.util.ArrayList;
import java.util.List;

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
                for (ObjectObjectCursor<String, IndexMetadata> cursor : indexMetadataMap) {
                    if (cursor.value != lastIndexMetadataMap.get(cursor.key)) {
                        final boolean isSystem = systemIndices.isSystemIndex(cursor.value.getIndex()) ||
                            systemIndices.isSystemIndexBackingDataStream(cursor.value.getIndex().getName());
                        if (isSystem != cursor.value.isSystem()) {
                            updateTaskPending = true;
                            clusterService.submitStateUpdateTask("system_index_metadata_upgrade_service {system metadata change}",
                                new SystemIndexMetadataUpdateTask());
                            break;
                        }
                    }
                }
            }
        }
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
            for (ObjectObjectCursor<String, IndexMetadata> cursor : indexMetadataMap) {
                if (cursor.value != lastIndexMetadataMap.get(cursor.key)) {
                    final boolean isSystem = systemIndices.isSystemIndex(cursor.value.getIndex()) ||
                        systemIndices.isSystemIndexBackingDataStream(cursor.value.getIndex().getName());
                    IndexMetadata.Builder builder = IndexMetadata.builder(cursor.value);
                    boolean updated = false;
                    if (isSystem != cursor.value.isSystem()) {
                        builder.system(cursor.value.isSystem() == false);
                        updated = true;
                    }
                    if (isSystem && cursor.value.getSettings().getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false)) {
                        builder.settings(Settings.builder()
                            .put(cursor.value.getSettings())
                            .put(IndexMetadata.SETTING_INDEX_HIDDEN, false));
                        updated = true;
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
        public void onFailure(String source, Exception e) {
            updateTaskPending = false;
            logger.error("failed to update system index metadata", e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            lastIndexMetadataMap = newState.metadata().indices();
            updateTaskPending = false;
        }
    }
}
