/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class TransportFreezeIndexAction extends
    TransportMasterNodeAction<TransportFreezeIndexAction.FreezeRequest, AcknowledgedResponse> {

    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportFreezeIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      DestructiveOperations destructiveOperations) {
        super(settings, FreezeIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            FreezeRequest::new);
        this.destructiveOperations = destructiveOperations;
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected void doExecute(Task task, FreezeRequest request, ActionListener<AcknowledgedResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(FreezeRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            throw new ResourceNotFoundException("index not found");
        }

        clusterService.submitStateUpdateTask("toggle-frozen-settings",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.URGENT, request, listener) {
            @Override
            public ClusterState execute(final ClusterState currentState) {
                final MetaData.Builder builder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                for (Index index : concreteIndices) {
                    IndexMetaData meta = currentState.metaData().getIndexSafe(index);
                    if (meta.getState() != IndexMetaData.State.CLOSE) {
                        throw new IllegalStateException("index [" + index.getName() + "] is not closed");
                    }
                    final IndexMetaData.Builder imdBuilder = IndexMetaData.builder(meta);
                    final Settings.Builder settingsBuilder =
                        Settings.builder()
                            .put(currentState.metaData().index(index).getSettings())
                            .put("index.blocks.write", request.freeze())
                            .put(FrozenEngine.INDEX_FROZEN.getKey(), request.freeze())
                            .put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), request.freeze());
                    if (request.freeze()) {
                        blocks.addIndexBlock(index.getName(), IndexMetaData.INDEX_WRITE_BLOCK);
                    } else {
                        blocks.removeIndexBlock(index.getName(), IndexMetaData.INDEX_WRITE_BLOCK);
                    }
                    imdBuilder.settings(settingsBuilder);
                    builder.put(imdBuilder.build(), true);
                }
                return ClusterState.builder(currentState).blocks(blocks).metaData(builder).build();
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(FreezeRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    public static class FreezeIndexAction extends Action<FreezeRequest, AcknowledgedResponse, FreezeRequestBuilder> {

        public static final FreezeIndexAction INSTANCE = new FreezeIndexAction();
        public static final String NAME = "indices:admin/freeze";

        private FreezeIndexAction() {
            super(NAME);
        }

        @Override
        public AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }

        @Override
        public FreezeRequestBuilder newRequestBuilder(ElasticsearchClient client) {
            return new FreezeRequestBuilder(client, this, new FreezeRequest());
        }
    }

    public static final class FreezeRequestBuilder extends ActionRequestBuilder<FreezeRequest, AcknowledgedResponse, FreezeRequestBuilder> {

        protected FreezeRequestBuilder(ElasticsearchClient client, Action<FreezeRequest, AcknowledgedResponse, FreezeRequestBuilder> action,
                                       FreezeRequest request) {
            super(client, action, request);
        }

        public FreezeRequestBuilder setFreeze(boolean freeze) {
            request.setFreeze(freeze);
            return this;
        }

        /**
         * Sets the indices to be frozen
         *
         * @param indices the indices to be frozen
         * @return the request itself
         */
        public FreezeRequestBuilder setIndices(String... indices) {
            request.indices(indices);
            return this;
        }

        /**
         * Specifies what type of requested indices to ignore and wildcard indices expressions
         * For example indices that don't exist.
         *
         * @param indicesOptions the desired behaviour regarding indices to ignore and indices wildcard expressions
         * @return the request itself
         */
        public FreezeRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
            request.indicesOptions(indicesOptions);
            return this;
        }
    }

    public static class FreezeRequest extends AcknowledgedRequest<FreezeRequest>
        implements IndicesRequest.Replaceable {
        private String[] indices;
        private boolean freeze = true;
        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, false, true);

        public FreezeRequest(String... indices) {
            this.indices = indices;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (CollectionUtils.isEmpty(indices)) {
                validationException = addValidationError("index is missing", validationException);
            }
            return validationException;
        }

        public void setFreeze(boolean freeze) {
            this.freeze = freeze;
        }

        public boolean freeze() {
            return freeze;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            indicesOptions = IndicesOptions.readIndicesOptions(in);
            indices = in.readStringArray();
            freeze = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indicesOptions.writeIndicesOptions(out);
            out.writeStringArray(indices);
            out.writeBoolean(freeze);
        }

        /**
         * @return the indices to be frozen or unfrozen
         */
        @Override
        public String[] indices() {
            return indices;
        }

        /**
         * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
         * For example indices that don't exist.
         *
         * @return the current behaviour when it comes to index names and wildcard indices expressions
         */
        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
         * For example indices that don't exist.
         *
         * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
         * @return the request itself
         */
        public FreezeRequest indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }
    }
}
