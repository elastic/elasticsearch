/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A service responsible for updating the metadata used by system indices.
 *
 * Mapping updates are handled by {@link SystemIndexMappingUpdateService}.
 */
public class SystemIndexMetadataUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SystemIndexMetadataUpgradeService.class);

    private final SystemIndices systemIndices;
    private final ClusterService clusterService;

    private volatile boolean updateTaskPending = false;

    public SystemIndexMetadataUpgradeService(SystemIndices systemIndices, ClusterService clusterService) {
        this.systemIndices = systemIndices;
        this.clusterService = clusterService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Metadata currentMetadata = event.state().metadata();
        Metadata previousMetadata = event.previousState().metadata();
        if (updateTaskPending == false
            && event.localNodeMaster()
            && (event.previousState().nodes().isLocalNodeElectedMaster() == false
                || currentMetadata.indices() != previousMetadata.indices()
                || currentMetadata.dataStreams() != previousMetadata.dataStreams())) {
            final Map<String, IndexMetadata> indexMetadataMap = currentMetadata.indices();
            final var previousIndices = previousMetadata.indices();
            Map<String, DataStream> dataStreams = currentMetadata.dataStreams();
            Map<String, DataStream> previousDataStreams = previousMetadata.dataStreams();
            // Fork to the management pool to avoid blocking the cluster applier thread unnecessarily for very large index counts
            // TODO: we should have a more efficient way of getting just the changed indices so that we don't have to fork here
            clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT).execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    for (Map.Entry<String, IndexMetadata> cursor : indexMetadataMap.entrySet()) {
                        if (cursor.getValue() != previousIndices.get(cursor.getKey())) {
                            IndexMetadata indexMetadata = cursor.getValue();
                            if (requiresUpdate(indexMetadata)) {
                                submitUpdateTask();
                                break;
                            }
                        }
                    }
                    for (Map.Entry<String, DataStream> cursor : dataStreams.entrySet()) {
                        if (cursor.getValue() != previousDataStreams.get(cursor.getKey())) {
                            DataStream dataStream = cursor.getValue();
                            if (requiresUpdate(dataStream)) {
                                submitUpdateTask();
                                break;
                            }
                        }
                    }
                }

                private void submitUpdateTask() {
                    updateTaskPending = true;
                    submitUnbatchedTask(
                        "system_index_metadata_upgrade_service {system metadata change}",
                        new SystemIndexMetadataUpdateTask()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected exception on checking for metadata upgrades", e);
                    assert false : e;
                }
            });
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
    boolean requiresUpdate(DataStream dataStream) {
        final boolean shouldBeSystem = shouldBeSystem(dataStream);

        // should toggle system index status
        if (shouldBeSystem != dataStream.isSystem()) {
            return true;
        }

        if (shouldBeSystem) {
            return dataStream.isHidden() == false;
        }

        return false;
    }

    private boolean shouldBeSystem(DataStream dataStream) {
        return systemIndices.isSystemDataStream(dataStream.getName());
    }

    // package-private for testing
    static boolean isVisible(IndexMetadata indexMetadata) {
        return indexMetadata.getSettings().getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, false) == false;
    }

    // package-private for testing
    boolean shouldBeSystem(IndexMetadata indexMetadata) {
        return systemIndices.isSystemIndex(indexMetadata.getIndex());
    }

    // package-private for testing
    static boolean hasVisibleAlias(IndexMetadata indexMetadata) {
        return indexMetadata.getAliases().values().stream().anyMatch(a -> Boolean.FALSE.equals(a.isHidden()));
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    // visible for testing
    ClusterState executeMetadataUpdateTask(ClusterState clusterState) {
        return new SystemIndexMetadataUpdateTask().execute(clusterState);
    }

    private class SystemIndexMetadataUpdateTask extends ClusterStateUpdateTask {

        @Override
        public ClusterState execute(ClusterState currentState) {
            List<IndexMetadata> updatedMetadata = updateIndices(currentState);
            List<DataStream> updatedDataStreams = updateDataStreams(currentState);
            List<IndexMetadata> updatedBackingIndices = updateIndicesBackingDataStreams(currentState, updatedDataStreams);

            if (updatedMetadata.isEmpty() == false || updatedDataStreams.isEmpty() == false) {
                Metadata.Builder builder = Metadata.builder(currentState.metadata());
                updatedMetadata.forEach(idxMeta -> builder.put(idxMeta, true));
                updatedDataStreams.forEach(builder::put);
                updatedBackingIndices.forEach(idxMeta -> builder.put(idxMeta, true));

                return ClusterState.builder(currentState).metadata(builder).build();
            }
            return currentState;
        }

        private List<IndexMetadata> updateIndices(ClusterState currentState) {
            final Map<String, IndexMetadata> indexMetadataMap = currentState.metadata().indices();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();
            for (Map.Entry<String, IndexMetadata> entry : indexMetadataMap.entrySet()) {
                final IndexMetadata indexMetadata = entry.getValue();
                final boolean shouldBeSystem = shouldBeSystem(indexMetadata);
                IndexMetadata updatedIndexMetadata = updateIndexIfNecessary(indexMetadata, shouldBeSystem);
                if (updatedIndexMetadata != null) {
                    updatedMetadata.add(updatedIndexMetadata);
                }
            }
            return updatedMetadata;
        }

        private IndexMetadata updateIndexIfNecessary(IndexMetadata indexMetadata, boolean shouldBeSystem) {
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
                        updated = true;
                    }
                }
            }
            return updated ? builder.build() : null;
        }

        private List<DataStream> updateDataStreams(ClusterState currentState) {
            List<DataStream> updatedDataStreams = new ArrayList<>();
            for (DataStream dataStream : currentState.getMetadata().dataStreams().values()) {
                boolean shouldBeSystem = shouldBeSystem(dataStream);
                if (dataStream.isSystem() != shouldBeSystem) {
                    DataStream.Builder dataStreamBuilder = dataStream.copy().setSystem(shouldBeSystem);
                    if (shouldBeSystem) {
                        dataStreamBuilder.setHidden(true);
                    }

                    updatedDataStreams.add(dataStreamBuilder.build());
                }
            }
            return updatedDataStreams;
        }

        private List<IndexMetadata> updateIndicesBackingDataStreams(ClusterState currentState, List<DataStream> updatedDataStreams) {
            if (updatedDataStreams.isEmpty()) {
                return Collections.emptyList();
            }
            Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();

            for (DataStream updatedDataStream : updatedDataStreams) {
                boolean shouldBeSystem = updatedDataStream.isSystem();
                List<IndexMetadata> updatedIndicesMetadata = getIndicesBackingDataStreamMetadata(metadata, updatedDataStream).map(
                    idx -> updateIndexIfNecessary(idx, shouldBeSystem)
                ).filter(Objects::nonNull).toList();

                updatedMetadata.addAll(updatedIndicesMetadata);
            }
            return updatedMetadata;
        }

        private Stream<IndexMetadata> getIndicesBackingDataStreamMetadata(Metadata metadata, DataStream dataStream) {
            return Stream.concat(dataStream.getIndices().stream(), dataStream.getFailureIndices().stream()).map(metadata::index);
        }

        @Override
        public void onFailure(Exception e) {
            updateTaskPending = false;
            logger.error("failed to update system index metadata", e);
        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            updateTaskPending = false;
        }
    }
}
