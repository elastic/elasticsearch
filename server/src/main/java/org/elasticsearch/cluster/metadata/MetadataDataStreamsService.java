/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.action.ValidateActions.addValidationError;

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

    public void modifyDataStream(final ModifyDataStreamRequest request,
                                 final ActionListener<AcknowledgedResponse> listener) {
        if (request.getActions().size() == 0) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            clusterService.submitStateUpdateTask("update-backing-indices",
                new AckedClusterStateUpdateTask(Priority.URGENT, request, listener) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return modifyDataStream(
                            currentState,
                            request.getActions(),
                            indexMetadata -> {
                                try {
                                    return indicesService.createIndexMapperService(indexMetadata);
                                } catch (IOException e) {
                                    throw new IllegalStateException(e);
                                }
                            }
                        );
                    }
                });
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

        for (var action : actions) {
            Metadata.Builder builder = Metadata.builder(updatedMetadata);
            if (action.getType() == DataStreamAction.Type.ADD_BACKING_INDEX) {
                addBackingIndex(
                    updatedMetadata,
                    builder,
                    mapperSupplier,
                    action.getDataStream(),
                    action.getIndex()
                );
            } else if (action.getType() == DataStreamAction.Type.REMOVE_BACKING_INDEX) {
                removeBackingIndex(
                    updatedMetadata,
                    builder,
                    action.getDataStream(),
                    action.getIndex()
                );
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
        var dataStream = validateDataStream(metadata, dataStreamName);
        var index = validateIndex(metadata, indexName);

        try {
            MetadataMigrateToDataStreamService.prepareBackingIndex(
                builder,
                metadata.index(index.getWriteIndex()),
                dataStreamName,
                mapperSupplier,
                false);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to prepare backing index", e);
        }

        // add index to data stream
        builder.put(dataStream.getDataStream().addBackingIndex(metadata, index.getWriteIndex()));
    }

    private static void removeBackingIndex(Metadata metadata, Metadata.Builder builder, String dataStreamName, String indexName) {
        var dataStream = validateDataStream(metadata, dataStreamName);
        var index = validateIndex(metadata, indexName);
        var writeIndex = metadata.index(index.getWriteIndex());
        builder.put(dataStream.getDataStream().removeBackingIndex(writeIndex.getIndex()));

        // un-hide index
        builder.put(IndexMetadata.builder(writeIndex)
            .settings(Settings.builder().put(writeIndex.getSettings()).put("index.hidden", "false").build())
            .settingsVersion(writeIndex.getSettingsVersion() + 1));
    }

    private static IndexAbstraction.DataStream validateDataStream(Metadata metadata, String dataStreamName) {
        IndexAbstraction dataStream = metadata.getIndicesLookup().get(dataStreamName);
        if (dataStream == null || dataStream.getType() != IndexAbstraction.Type.DATA_STREAM) {
            throw new IllegalArgumentException("data stream [" + dataStreamName + "] not found");
        }
        return (IndexAbstraction.DataStream) dataStream;
    }

    private static IndexAbstraction validateIndex(Metadata metadata, String indexName) {
        IndexAbstraction index = metadata.getIndicesLookup().get(indexName);
        if (index == null || index.getType() != IndexAbstraction.Type.CONCRETE_INDEX) {
            throw new IllegalArgumentException("index [" + indexName + "] not found");
        }
        return index;
    }

    public static final class ModifyDataStreamRequest
        extends AcknowledgedRequest<ModifyDataStreamRequest>
        implements IndicesRequest, ToXContentObject {

        // relevant only for authorizing the request, so require every specified
        // index to exist, expand wildcards only to open indices, prohibit
        // wildcard expressions that resolve to zero indices, and do not attempt
        // to resolve expressions as aliases
        private static final IndicesOptions INDICES_OPTIONS =
            IndicesOptions.fromOptions(false, false, true, false, true, false, true, false);

        private List<DataStreamAction> actions;

        public ModifyDataStreamRequest() {}

        public ModifyDataStreamRequest(StreamInput in) throws IOException {
            super(in);
            actions = in.readList(DataStreamAction::new);
        }

        public ModifyDataStreamRequest(List<DataStreamAction> actions) {
            this.actions = Collections.unmodifiableList(actions);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }

        public void setActions(List<DataStreamAction> actions) {
            this.actions = Collections.unmodifiableList(actions);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("actions");
            for (DataStreamAction action : actions) {
                action.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (actions.isEmpty()) {
                return addValidationError("must specify at least one data stream modification action", null);
            }
            return null;
        }

        public static final ObjectParser<ModifyDataStreamRequest, Void> PARSER =
            new ObjectParser<>("data_stream_actions", ModifyDataStreamRequest::new);
        static {
            PARSER.declareObjectArray(ModifyDataStreamRequest::setActions, DataStreamAction.PARSER, new ParseField("actions"));
        }

        @Override
        public String[] indices() {
            return actions.stream().map(DataStreamAction::getDataStream).toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }
    }
}
