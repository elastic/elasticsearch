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
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.function.Function;

/**
 * Handles data stream modification requests.
 */
public class MetadataDataStreamsService {

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    public MetadataDataStreamsService(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    public void modifyDataStream(final ModifyDataStreamsAction.Request request, final ActionListener<AcknowledgedResponse> listener) {
        if (request.getActions().size() == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            clusterService.submitStateUpdateTask(
                "update-backing-indices",
                new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return modifyDataStream(currentState, request.getActions(), indexMetadata -> {
                            try {
                                return indicesService.createIndexMapperService(indexMetadata);
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        });
                    }
                }
            );
        }
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

        for (DataStreamAction action : actions) {
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

    private static void addBackingIndex(
        Metadata metadata,
        Metadata.Builder builder,
        Function<IndexMetadata, MapperService> mapperSupplier,
        String dataStreamName,
        String indexName
    ) {
        IndexAbstraction.DataStream dataStream = validateDataStream(metadata, dataStreamName);
        IndexAbstraction.ConcreteIndex index = validateIndex(metadata, indexName);

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
        builder.put(dataStream.getDataStream().addBackingIndex(metadata, index.getWriteIndex()));
    }

    private static void removeBackingIndex(Metadata metadata, Metadata.Builder builder, String dataStreamName, String indexName) {
        boolean indexNotRemoved = true;
        DataStream dataStream = validateDataStream(metadata, dataStreamName).getDataStream();
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
        IndexMetadata indexMetadata = builder.get(indexName);
        if (indexMetadata != null) {
            builder.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(Settings.builder().put(indexMetadata.getSettings()).put("index.hidden", "false").build())
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1)
            );
        }
    }

    private static IndexAbstraction.DataStream validateDataStream(Metadata metadata, String dataStreamName) {
        IndexAbstraction dataStream = metadata.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (IndexAbstraction.DataStream) dataStream;
    }

    private static IndexAbstraction.ConcreteIndex validateIndex(Metadata metadata, String indexName) {
        IndexAbstraction index = metadata.getIndicesLookup().get(indexName);
        if (index == null || index.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return (IndexAbstraction.ConcreteIndex) index;
    }

}
