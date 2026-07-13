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
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndexMappingUpdateService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
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
    private final MasterServiceTaskQueue<SystemIndexMetadataUpgradeTask> taskQueue;

    public SystemIndexMetadataUpgradeService(SystemIndices systemIndices, ClusterService clusterService) {
        this.systemIndices = systemIndices;
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue(
            "system-indices-metadata-upgrade",
            Priority.NORMAL,
            new SystemIndexMetadataUpgradeExecutor()
        );
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        @FixForMultiProject
        ProjectMetadata currentMetadata = event.state().metadata().getProject();
        ProjectMetadata previousMetadata = event.previousState().metadata().getProject();
        if (event.localNodeMaster()
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
                    Collection<DataStream> changedDataStreams = new ArrayList<>();
                    Set<Index> dataStreamIndices = new HashSet<>();
                    for (Map.Entry<String, DataStream> cursor : dataStreams.entrySet()) {
                        DataStream dataStream = cursor.getValue();
                        if (dataStream != previousDataStreams.get(cursor.getKey())) {
                            if (requiresUpdate(dataStream)) {
                                changedDataStreams.add(dataStream);
                            }
                        }

                        getIndicesBackingDataStream(dataStream).forEach(dataStreamIndices::add);
                    }

                    Collection<Index> changedIndices = new ArrayList<>();
                    for (Map.Entry<String, IndexMetadata> cursor : indexMetadataMap.entrySet()) {
                        IndexMetadata indexMetadata = cursor.getValue();
                        Index index = indexMetadata.getIndex();
                        if (cursor.getValue() != previousIndices.get(cursor.getKey()) && dataStreamIndices.contains(index) == false) {
                            if (requiresUpdate(indexMetadata)) {
                                changedIndices.add(index);
                            }
                        }
                    }

                    if (changedIndices.isEmpty() == false || changedDataStreams.isEmpty() == false) {
                        submitUpdateTask(changedIndices, changedDataStreams);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected exception on checking for metadata upgrades", e);
                    assert false : e;
                }
            });
        }
    }

    // visible for testing
    void submitUpdateTask(Collection<Index> changedIndices, Collection<DataStream> changedDataStreams) {
        SystemIndexMetadataUpgradeTask task = new SystemIndexMetadataUpgradeTask(changedIndices, changedDataStreams);
        taskQueue.submitTask("system-index-metadata-upgrade-service", task, null);
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

    private static Stream<Index> getIndicesBackingDataStream(DataStream dataStream) {
        return Stream.concat(dataStream.getIndices().stream(), dataStream.getFailureIndices().stream());
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

    private record SystemIndexMetadataUpgradeTask(Collection<Index> changedIndices, Collection<DataStream> changedDataStreams)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            logger.error("System index metadata upgrade failed", e);
        }

        @Override
        public String toString() {
            return "SystemIndexMetadataUpgradeTask[changedIndices="
                + changedIndices.stream().map(Index::getName).collect(Collectors.joining(","))
                + ";changedDataStreams="
                + changedDataStreams.stream().map(DataStream::getName).collect(Collectors.joining(","))
                + "]";
        }
    }

    private class SystemIndexMetadataUpgradeExecutor implements ClusterStateTaskExecutor<SystemIndexMetadataUpgradeTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<SystemIndexMetadataUpgradeTask> batchExecutionContext) {
            ClusterState initialState = batchExecutionContext.initialState();

            List<? extends TaskContext<SystemIndexMetadataUpgradeTask>> taskContexts = batchExecutionContext.taskContexts();
            List<Index> indices = taskContexts.stream()
                .map(TaskContext::getTask)
                .map(SystemIndexMetadataUpgradeTask::changedIndices)
                .flatMap(Collection::stream)
                .toList();
            List<IndexMetadata> updatedMetadata = updateIndices(initialState, indices);

            List<DataStream> dataStreams = taskContexts.stream()
                .map(TaskContext::getTask)
                .map(SystemIndexMetadataUpgradeTask::changedDataStreams)
                .flatMap(Collection::stream)
                .toList();
            List<DataStream> updatedDataStreams = updateDataStreams(dataStreams);
            List<IndexMetadata> updatedBackingIndices = updateIndicesBackingDataStreams(initialState, updatedDataStreams);

            for (TaskContext<SystemIndexMetadataUpgradeTask> taskContext : taskContexts) {
                taskContext.success(() -> {});
            }

            if (updatedMetadata.isEmpty() == false || updatedDataStreams.isEmpty() == false) {
                ProjectMetadata.Builder builder = ProjectMetadata.builder(initialState.metadata().getProject());
                updatedMetadata.forEach(idxMeta -> builder.put(idxMeta, true));
                updatedDataStreams.forEach(builder::put);
                updatedBackingIndices.forEach(idxMeta -> builder.put(idxMeta, true));

                return ClusterState.builder(initialState).putProjectMetadata(builder).build();
            }
            return initialState;
        }

        private List<IndexMetadata> updateIndices(ClusterState currentState, List<Index> indices) {
            if (indices.isEmpty()) {
                return Collections.emptyList();
            }
            Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updatedMetadata = new ArrayList<>();
            for (Index index : indices) {
                IndexMetadata indexMetadata = metadata.indexMetadata(index);
                // this might happen because update is async and the index might have been deleted between task creation and execution
                if (indexMetadata == null) {
                    continue;
                }
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

        private List<DataStream> updateDataStreams(List<DataStream> dataStreams) {
            if (dataStreams.isEmpty()) {
                return Collections.emptyList();
            }
            List<DataStream> updatedDataStreams = new ArrayList<>();
            for (DataStream dataStream : dataStreams) {
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
                List<IndexMetadata> updatedIndicesMetadata = getIndicesBackingDataStreamMetadata(metadata.getProject(), updatedDataStream)
                    .map(idx -> updateIndexIfNecessary(idx, shouldBeSystem))
                    .filter(Objects::nonNull)
                    .toList();

                updatedMetadata.addAll(updatedIndicesMetadata);
            }
            return updatedMetadata;
        }

        private Stream<IndexMetadata> getIndicesBackingDataStreamMetadata(ProjectMetadata projectMetadata, DataStream dataStream) {
            return getIndicesBackingDataStream(dataStream).map(projectMetadata::index);
        }
    }
}
