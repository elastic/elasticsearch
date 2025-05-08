/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.lookupTemplateForDataStream;

/**
 * Handles data stream modification requests.
 */
public class MetadataDataStreamsService {
    private static final Logger LOGGER = LogManager.getLogger(MetadataDataStreamsService.class);
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;
    private final MasterServiceTaskQueue<UpdateLifecycleTask> updateLifecycleTaskQueue;
    private final MasterServiceTaskQueue<SetRolloverOnWriteTask> setRolloverOnWriteTaskQueue;
    private final MasterServiceTaskQueue<UpdateOptionsTask> updateOptionsTaskQueue;
    private final MasterServiceTaskQueue<UpdateSettingsTask> updateSettingsTaskQueue;

    public MetadataDataStreamsService(
        ClusterService clusterService,
        IndicesService indicesService,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.globalRetentionSettings = globalRetentionSettings;
        ClusterStateTaskExecutor<UpdateLifecycleTask> updateLifecycleExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateLifecycleTask modifyLifecycleTask,
                ClusterState clusterState
            ) {
                return new Tuple<>(
                    ClusterState.builder(clusterState)
                        .putProjectMetadata(
                            updateDataLifecycle(
                                clusterState.metadata().getProject(modifyLifecycleTask.getProjectId()),
                                modifyLifecycleTask.getDataStreamNames(),
                                modifyLifecycleTask.getDataLifecycle()
                            )
                        )
                        .build(),
                    modifyLifecycleTask
                );
            }
        };
        // We chose priority high because changing the lifecycle is changing the retention of a backing index, so processing it quickly
        // can either free space when the retention is shortened, or prevent an index to be deleted when the retention is extended.
        this.updateLifecycleTaskQueue = clusterService.createTaskQueue("modify-lifecycle", Priority.HIGH, updateLifecycleExecutor);
        ClusterStateTaskExecutor<SetRolloverOnWriteTask> rolloverOnWriteExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                SetRolloverOnWriteTask setRolloverOnWriteTask,
                ClusterState clusterState
            ) {
                return new Tuple<>(
                    setRolloverOnWrite(
                        clusterState.projectState(setRolloverOnWriteTask.projectId()),
                        setRolloverOnWriteTask.getDataStreamName(),
                        setRolloverOnWriteTask.rolloverOnWrite(),
                        setRolloverOnWriteTask.targetFailureStore()
                    ),
                    setRolloverOnWriteTask
                );
            }
        };
        this.setRolloverOnWriteTaskQueue = clusterService.createTaskQueue(
            "data-stream-rollover-on-write",
            Priority.NORMAL,
            rolloverOnWriteExecutor
        );
        ClusterStateTaskExecutor<UpdateOptionsTask> updateOptionsExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateOptionsTask modifyOptionsTask,
                ClusterState clusterState
            ) {
                return new Tuple<>(
                    ClusterState.builder(clusterState)
                        .putProjectMetadata(
                            updateDataStreamOptions(
                                clusterState.projectState(modifyOptionsTask.projectId).metadata(),
                                modifyOptionsTask.getDataStreamNames(),
                                modifyOptionsTask.getOptions()
                            )
                        )
                        .build(),
                    modifyOptionsTask
                );
            }
        };
        this.updateOptionsTaskQueue = clusterService.createTaskQueue("modify-data-stream-options", Priority.NORMAL, updateOptionsExecutor);
        ClusterStateTaskExecutor<UpdateSettingsTask> updateSettingsExecutor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateSettingsTask updateSettingsTask,
                ClusterState clusterState
            ) throws Exception {

                ProjectMetadata projectMetadata = clusterState.metadata().getProject(updateSettingsTask.projectId);
                ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
                Map<String, DataStream> dataStreamMap = projectMetadata.dataStreams();
                DataStream dataStream = dataStreamMap.get(updateSettingsTask.dataStreamName);
                Settings existingSettings = dataStream.getSettings();

                Template.Builder templateBuilder = Template.builder();
                Settings.Builder mergedSettingsBuilder = Settings.builder().put(existingSettings).put(updateSettingsTask.settingsOverrides);
                Settings mergedSettings = mergedSettingsBuilder.build();

                final ComposableIndexTemplate template = lookupTemplateForDataStream(updateSettingsTask.dataStreamName, projectMetadata);
                ComposableIndexTemplate mergedTemplate = template.mergeSettings(mergedSettings);
                MetadataIndexTemplateService.validateTemplate(
                    mergedTemplate.template().settings(),
                    mergedTemplate.template().mappings(),
                    indicesService
                );

                templateBuilder.settings(mergedSettingsBuilder);
                DataStream.Builder dataStreamBuilder = dataStream.copy().setSettings(mergedSettings);
                projectMetadataBuilder.removeDataStream(updateSettingsTask.dataStreamName);
                projectMetadataBuilder.put(dataStreamBuilder.build());
                ClusterState updatedClusterState = ClusterState.builder(clusterState).putProjectMetadata(projectMetadataBuilder).build();

                return new Tuple<>(updatedClusterState, updateSettingsTask);
            }
        };
        this.updateSettingsTaskQueue = clusterService.createTaskQueue(
            "update-data-stream-settings",
            Priority.NORMAL,
            updateSettingsExecutor
        );
    }

    public void modifyDataStream(
        final ProjectId projectId,
        final ModifyDataStreamsAction.Request request,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        if (request.getActions().size() == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            submitUnbatchedTask("update-backing-indices", new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final var project = modifyDataStream(
                        currentState.metadata().getProject(projectId),
                        request.getActions(),
                        indexMetadata -> {
                            try {
                                return indicesService.createIndexMapperServiceForValidation(indexMetadata);
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        },
                        clusterService.getSettings()
                    );
                    return ClusterState.builder(currentState).putProjectMetadata(project).build();
                }
            });
        }
    }

    /**
     * Submits the task to set the lifecycle to the requested data streams.
     */
    public void setLifecycle(
        final ProjectId projectId,
        final List<String> dataStreamNames,
        DataStreamLifecycle lifecycle,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        updateLifecycleTaskQueue.submitTask(
            "set-lifecycle",
            new UpdateLifecycleTask(projectId, dataStreamNames, lifecycle, ackTimeout, listener),
            masterTimeout
        );
    }

    /**
     * Submits the task to remove the lifecycle from the requested data streams.
     */
    public void removeLifecycle(
        ProjectId projectId,
        List<String> dataStreamNames,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        updateLifecycleTaskQueue.submitTask(
            "delete-lifecycle",
            new UpdateLifecycleTask(projectId, dataStreamNames, null, ackTimeout, listener),
            masterTimeout
        );
    }

    /**
     * Submits the task to set the provided data stream options to the requested data streams.
     */
    public void setDataStreamOptions(
        final ProjectId projectId,
        final List<String> dataStreamNames,
        DataStreamOptions options,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        updateOptionsTaskQueue.submitTask(
            "set-data-stream-options",
            new UpdateOptionsTask(projectId, dataStreamNames, options, ackTimeout, listener),
            masterTimeout
        );
    }

    /**
     * Submits the task to remove the data stream options from the requested data streams.
     */
    public void removeDataStreamOptions(
        ProjectId projectId,
        List<String> dataStreamNames,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        updateOptionsTaskQueue.submitTask(
            "delete-data-stream-options",
            new UpdateOptionsTask(projectId, dataStreamNames, null, ackTimeout, listener),
            masterTimeout
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Submits the task to signal that the next time this data stream receives a document, it will be rolled over.
     */
    public void setRolloverOnWrite(
        ProjectId projectId,
        String dataStreamName,
        boolean rolloverOnWrite,
        boolean targetFailureStore,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        setRolloverOnWriteTaskQueue.submitTask(
            "set-rollover-on-write",
            new SetRolloverOnWriteTask(projectId, dataStreamName, rolloverOnWrite, targetFailureStore, ackTimeout, listener),
            masterTimeout
        );
    }

    /**
     * Computes the resulting cluster state after applying all requested data stream modifications in order.
     *
     * @param currentProject current project metadata
     * @param actions ordered list of modifications to perform
     * @return resulting cluster state after all modifications have been performed
     */
    static ProjectMetadata modifyDataStream(
        ProjectMetadata currentProject,
        Iterable<DataStreamAction> actions,
        Function<IndexMetadata, MapperService> mapperSupplier,
        Settings nodeSettings
    ) {
        var updatedProject = currentProject;
        for (var action : actions) {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(updatedProject);
            if (action.getType() == DataStreamAction.Type.ADD_BACKING_INDEX) {
                addBackingIndex(
                    updatedProject,
                    builder,
                    mapperSupplier,
                    action.getDataStream(),
                    action.getIndex(),
                    action.isFailureStore(),
                    nodeSettings
                );
            } else if (action.getType() == DataStreamAction.Type.REMOVE_BACKING_INDEX) {
                removeBackingIndex(updatedProject, builder, action.getDataStream(), action.getIndex(), action.isFailureStore());
            } else {
                throw new IllegalStateException("unsupported data stream action type [" + action.getClass().getName() + "]");
            }
            updatedProject = builder.build();
        }

        return updatedProject;
    }

    /**
     * Creates an updated cluster state in which the requested data streams have the data stream lifecycle provided.
     * Visible for testing.
     */
    ProjectMetadata updateDataLifecycle(ProjectMetadata project, List<String> dataStreamNames, @Nullable DataStreamLifecycle lifecycle) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        boolean onlyInternalDataStreams = true;
        for (var dataStreamName : dataStreamNames) {
            var dataStream = validateDataStream(project, dataStreamName);
            builder.put(dataStream.copy().setLifecycle(lifecycle).build());
            onlyInternalDataStreams = onlyInternalDataStreams && dataStream.isInternal();
        }
        if (lifecycle != null) {
            // We don't issue any warnings if all data streams are internal data streams
            lifecycle.addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(false), onlyInternalDataStreams);
        }
        return builder.build();
    }

    /**
     * Creates an updated cluster state in which the requested data streams have the data stream options provided.
     * Visible for testing.
     */
    ProjectMetadata updateDataStreamOptions(
        ProjectMetadata project,
        List<String> dataStreamNames,
        @Nullable DataStreamOptions dataStreamOptions
    ) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
        boolean onlyInternalDataStreams = true;
        for (var dataStreamName : dataStreamNames) {
            var dataStream = validateDataStream(project, dataStreamName);
            builder.put(dataStream.copy().setDataStreamOptions(dataStreamOptions).build());
            onlyInternalDataStreams = onlyInternalDataStreams && dataStream.isInternal();
        }
        if (dataStreamOptions != null && dataStreamOptions.failureStore() != null && dataStreamOptions.failureStore().lifecycle() != null) {
            // We don't issue any warnings if all data streams are internal data streams
            dataStreamOptions.failureStore()
                .lifecycle()
                .addWarningHeaderIfDataRetentionNotEffective(globalRetentionSettings.get(true), onlyInternalDataStreams);
        }
        return builder.build();
    }

    /**
     * Creates an updated cluster state in which the requested data stream has the flag {@link DataStream#rolloverOnWrite()}
     * set to the value of the parameter rolloverOnWrite
     *
     * @param currentState the initial project state
     * @param dataStreamName the name of the data stream to be updated
     * @param rolloverOnWrite the value of the flag
     * @param targetFailureStore whether this rollover targets the failure store or the backing indices
     * @return the updated cluster state
     */
    public static ClusterState setRolloverOnWrite(
        ProjectState currentState,
        String dataStreamName,
        boolean rolloverOnWrite,
        boolean targetFailureStore
    ) {
        var metadata = currentState.metadata();
        var dataStream = validateDataStream(metadata, dataStreamName);
        var indices = dataStream.getDataStreamIndices(targetFailureStore);
        if (indices.isRolloverOnWrite() == rolloverOnWrite) {
            return currentState.cluster();
        }
        return currentState.updatedState(
            builder -> builder.put(
                dataStream.copy()
                    .setDataStreamIndices(targetFailureStore, indices.copy().setRolloverOnWrite(rolloverOnWrite).build())
                    .build()
            )
        );
    }

    public void updateSettings(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        String dataStreamName,
        Settings settingsOverrides,
        ActionListener<AcknowledgedResponse> listener
    ) {
        updateSettingsTaskQueue.submitTask(
            "updating settings on data stream",
            new UpdateSettingsTask(projectId, dataStreamName, settingsOverrides, ackTimeout, listener),
            masterNodeTimeout
        );
    }

    private static void addBackingIndex(
        ProjectMetadata project,
        ProjectMetadata.Builder builder,
        Function<IndexMetadata, MapperService> mapperSupplier,
        String dataStreamName,
        String indexName,
        boolean failureStore,
        Settings nodeSettings
    ) {
        var dataStream = validateDataStream(project, dataStreamName);
        var index = validateIndex(project, indexName);

        try {
            MetadataMigrateToDataStreamService.prepareBackingIndex(
                builder,
                project.index(index.getWriteIndex()),
                dataStreamName,
                mapperSupplier,
                false,
                failureStore,
                dataStream.isSystem(),
                nodeSettings
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to prepare backing index", e);
        }

        // add index to data stream
        if (failureStore) {
            builder.put(dataStream.addFailureStoreIndex(project, index.getWriteIndex()));
        } else {
            builder.put(dataStream.addBackingIndex(project, index.getWriteIndex()));
        }
    }

    private static void removeBackingIndex(
        ProjectMetadata project,
        ProjectMetadata.Builder builder,
        String dataStreamName,
        String indexName,
        boolean failureStore
    ) {
        boolean indexNotRemoved = true;
        DataStream dataStream = validateDataStream(project, dataStreamName);
        List<Index> targetIndices = failureStore ? dataStream.getFailureIndices() : dataStream.getIndices();
        for (Index backingIndex : targetIndices) {
            if (backingIndex.getName().equals(indexName)) {
                if (failureStore) {
                    builder.put(dataStream.removeFailureStoreIndex(backingIndex));
                } else {
                    builder.put(dataStream.removeBackingIndex(backingIndex));
                }
                indexNotRemoved = false;
                break;
            }
        }

        if (indexNotRemoved) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }

        // un-hide index
        var indexMetadata = builder.get(indexName);
        if (indexMetadata != null) {
            builder.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(Settings.builder().put(indexMetadata.getSettings()).put("index.hidden", "false").build())
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );
        }
    }

    private static DataStream validateDataStream(ProjectMetadata project, String dataStreamName) {
        IndexAbstraction dataStream = project.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (DataStream) dataStream;
    }

    private static IndexAbstraction validateIndex(ProjectMetadata project, String indexName) {
        IndexAbstraction index = project.getIndicesLookup().get(indexName);
        if (index == null || index.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return index;
    }

    /**
     * Removes the given data stream and their backing indices from the Project State.
     *
     * @param projectState The project state
     * @param dataStreams  The data streams to remove
     * @param settings     The settings
     * @return The updated Project State
     */
    public static ClusterState deleteDataStreams(ProjectState projectState, Set<DataStream> dataStreams, Settings settings) {
        if (dataStreams.isEmpty()) {
            return projectState.cluster();
        }

        Set<String> dataStreamNames = dataStreams.stream().map(DataStream::getName).collect(Collectors.toSet());
        Set<String> snapshottingDataStreams = SnapshotsService.snapshottingDataStreams(projectState, dataStreamNames);
        if (snapshottingDataStreams.isEmpty() == false) {
            throw new SnapshotInProgressException(
                "Cannot delete data streams that are being snapshotted: ["
                    + String.join(", ", snapshottingDataStreams)
                    + "]. Try again after snapshot finishes or cancel the currently running snapshot."
            );
        }

        Set<Index> backingIndicesToRemove = new HashSet<>();
        for (DataStream dataStream : dataStreams) {
            assert dataStream != null;
            if (projectState.metadata().dataStreams().get(dataStream.getName()) == null) {
                throw new ResourceNotFoundException("data stream [" + dataStream.getName() + "] not found");
            }
            backingIndicesToRemove.addAll(dataStream.getIndices());
            backingIndicesToRemove.addAll(dataStream.getFailureIndices());
        }

        // first delete the data streams and then the indices:
        // (this to avoid data stream validation from failing when deleting an index that is part of a data stream
        // without updating the data stream)
        // TODO: change order when "delete index api" also updates the data stream the "index to be removed" is a member of
        ClusterState newState = projectState.updatedState(builder -> {
            for (DataStream ds : dataStreams) {
                LOGGER.info("removing data stream [{}]", ds.getName());
                builder.removeDataStream(ds.getName());
            }
        });
        return MetadataDeleteIndexService.deleteIndices(newState.projectState(projectState.projectId()), backingIndicesToRemove, settings);
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    static class UpdateLifecycleTask extends AckedBatchedClusterStateUpdateTask {

        private final ProjectId projectId;
        private final List<String> dataStreamNames;
        private final DataStreamLifecycle lifecycle;

        UpdateLifecycleTask(
            ProjectId projectId,
            List<String> dataStreamNames,
            @Nullable DataStreamLifecycle lifecycle,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.dataStreamNames = dataStreamNames;
            this.lifecycle = lifecycle;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public List<String> getDataStreamNames() {
            return dataStreamNames;
        }

        public DataStreamLifecycle getDataLifecycle() {
            return lifecycle;
        }
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    static class UpdateOptionsTask extends AckedBatchedClusterStateUpdateTask {
        ProjectId projectId;
        private final List<String> dataStreamNames;
        private final DataStreamOptions options;

        UpdateOptionsTask(
            ProjectId projectId,
            List<String> dataStreamNames,
            @Nullable DataStreamOptions options,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.dataStreamNames = dataStreamNames;
            this.options = options;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public List<String> getDataStreamNames() {
            return dataStreamNames;
        }

        public DataStreamOptions getOptions() {
            return options;
        }
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    static class SetRolloverOnWriteTask extends AckedBatchedClusterStateUpdateTask {

        private final ProjectId projectId;
        private final String dataStreamName;
        private final boolean rolloverOnWrite;
        private final boolean targetFailureStore;

        SetRolloverOnWriteTask(
            ProjectId projectId,
            String dataStreamName,
            boolean rolloverOnWrite,
            boolean targetFailureStore,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.dataStreamName = dataStreamName;
            this.rolloverOnWrite = rolloverOnWrite;
            this.targetFailureStore = targetFailureStore;
        }

        public ProjectId projectId() {
            return projectId;
        }

        public String getDataStreamName() {
            return dataStreamName;
        }

        public boolean rolloverOnWrite() {
            return rolloverOnWrite;
        }

        public boolean targetFailureStore() {
            return targetFailureStore;
        }
    }

    static class UpdateSettingsTask extends AckedBatchedClusterStateUpdateTask {
        final ProjectId projectId;
        private final String dataStreamName;
        private final Settings settingsOverrides;

        UpdateSettingsTask(
            ProjectId projectId,
            String dataStreamName,
            Settings settingsOverrides,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.dataStreamName = dataStreamName;
            this.settingsOverrides = settingsOverrides;
        }
    }
}
