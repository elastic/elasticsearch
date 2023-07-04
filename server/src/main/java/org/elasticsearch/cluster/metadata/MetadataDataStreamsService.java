/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Handles data stream modification requests.
 */
public class MetadataDataStreamsService {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final MasterServiceTaskQueue<UpdateLifecycleTask> taskQueue;

    public MetadataDataStreamsService(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        ClusterStateTaskExecutor<UpdateLifecycleTask> executor = new SimpleBatchedAckListenerTaskExecutor<>() {

            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(
                UpdateLifecycleTask modifyLifecycleTask,
                ClusterState clusterState
            ) {
                return new Tuple<>(
                    updateDataLifecycle(clusterState, modifyLifecycleTask.getDataStreamNames(), modifyLifecycleTask.getDataLifecycle()),
                    modifyLifecycleTask
                );
            }
        };
        // We chose priority high because changing the lifecycle is changing the retention of a backing index, so processing it quickly
        // can either free space when the retention is shortened, or prevent an index to be deleted when the retention is extended.
        this.taskQueue = clusterService.createTaskQueue("modify-lifecycle", Priority.HIGH, executor);
    }

    public void modifyDataStream(final ModifyDataStreamsAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        if (request.getActions().size() == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            submitUnbatchedTask("update-backing-indices", new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return modifyDataStream(currentState, request.getActions(), indexMetadata -> {
                        try {
                            return indicesService.createIndexMapperServiceForValidation(indexMetadata);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                }
            });
        }
    }

    /**
     * Submits the task to set the lifecycle to the requested data streams.
     */
    public void setLifecycle(
        final List<String> dataStreamNames,
        DataStreamLifecycle lifecycle,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask("set-lifecycle", new UpdateLifecycleTask(dataStreamNames, lifecycle, ackTimeout, listener), masterTimeout);
    }

    /**
     * Submits the task to remove the lifecycle from the requested data streams.
     */
    public void removeLifecycle(
        List<String> dataStreamNames,
        TimeValue ackTimeout,
        TimeValue masterTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask("delete-lifecycle", new UpdateLifecycleTask(dataStreamNames, null, ackTimeout, listener), masterTimeout);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Computes the resulting cluster state after applying all requested data stream modifications in order.
     *
     * @param currentState current cluster state
     * @param actions      ordered list of modifications to perform
     * @return resulting cluster state after all modifications have been performed
     */
    static ClusterState modifyDataStream(
        ClusterState currentState,
        Iterable<DataStreamAction> actions,
        Function<IndexMetadata, MapperService> mapperSupplier
    ) {
        Metadata updatedMetadata = currentState.metadata();

        for (var action : actions) {
            Metadata.Builder builder = Metadata.builder(updatedMetadata);
            if (action.getType() == DataStreamAction.Type.ADD_BACKING_INDEX) {
                addBackingIndex(updatedMetadata, builder, mapperSupplier, action.getDataStream(), action.getIndex());
            } else if (action.getType() == DataStreamAction.Type.REMOVE_BACKING_INDEX) {
                removeBackingIndex(updatedMetadata, builder, action.getDataStream(), action.getIndex());
            } else {
                throw new IllegalStateException("unsupported data stream action type [" + action.getClass().getName() + "]");
            }
            updatedMetadata = builder.build();
        }

        return ClusterState.builder(currentState).metadata(updatedMetadata).build();
    }

    /**
     * Creates an updated cluster state in which the requested data streams have the data lifecycle provided.
     * Visible for testing.
     */
    static ClusterState updateDataLifecycle(
        ClusterState currentState,
        List<String> dataStreamNames,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        Metadata metadata = currentState.metadata();
        Metadata.Builder builder = Metadata.builder(metadata);
        for (var dataStreamName : dataStreamNames) {
            var dataStream = validateDataStream(metadata, dataStreamName);
            builder.put(
                new DataStream(
                    dataStream.getName(),
                    dataStream.getIndices(),
                    dataStream.getGeneration(),
                    dataStream.getMetadata(),
                    dataStream.isHidden(),
                    dataStream.isReplicated(),
                    dataStream.isSystem(),
                    dataStream.isAllowCustomRouting(),
                    dataStream.getIndexMode(),
                    lifecycle
                )
            );
        }
        return ClusterState.builder(currentState).metadata(builder.build()).build();
    }

    private static void addBackingIndex(
        Metadata metadata,
        Metadata.Builder builder,
        Function<IndexMetadata, MapperService> mapperSupplier,
        String dataStreamName,
        String indexName
    ) {
        var dataStream = validateDataStream(metadata, dataStreamName);
        var index = validateIndex(metadata, indexName);

        try {
            MetadataMigrateToDataStreamService.prepareBackingIndex(
                builder,
                metadata.index(index.getWriteIndex()),
                dataStreamName,
                mapperSupplier,
                false
            );
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to prepare backing index", e);
        }

        // add index to data stream
        builder.put(dataStream.addBackingIndex(metadata, index.getWriteIndex()));
    }

    private static void removeBackingIndex(Metadata metadata, Metadata.Builder builder, String dataStreamName, String indexName) {
        boolean indexNotRemoved = true;
        DataStream dataStream = validateDataStream(metadata, dataStreamName);
        for (Index backingIndex : dataStream.getIndices()) {
            if (backingIndex.getName().equals(indexName)) {
                builder.put(dataStream.removeBackingIndex(backingIndex));
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

    private static DataStream validateDataStream(Metadata metadata, String dataStreamName) {
        IndexAbstraction dataStream = metadata.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (DataStream) dataStream;
    }

    private static IndexAbstraction validateIndex(Metadata metadata, String indexName) {
        IndexAbstraction index = metadata.getIndicesLookup().get(indexName);
        if (index == null || index.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return index;
    }

    /**
     * A cluster state update task that consists of the cluster state request and the listeners that need to be notified upon completion.
     */
    static class UpdateLifecycleTask extends AckedBatchedClusterStateUpdateTask {

        private final List<String> dataStreamNames;
        private final DataStreamLifecycle lifecycle;

        UpdateLifecycleTask(
            List<String> dataStreamNames,
            @Nullable DataStreamLifecycle lifecycle,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.dataStreamNames = dataStreamNames;
            this.lifecycle = lifecycle;
        }

        public List<String> getDataStreamNames() {
            return dataStreamNames;
        }

        public DataStreamLifecycle getDataLifecycle() {
            return lifecycle;
        }
    }
}
