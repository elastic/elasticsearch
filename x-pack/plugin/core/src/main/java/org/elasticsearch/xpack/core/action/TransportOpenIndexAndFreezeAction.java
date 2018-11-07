/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public final class TransportOpenIndexAndFreezeAction extends
    TransportMasterNodeAction<TransportOpenIndexAndFreezeAction.OpenIndexAndFreezeRequest, OpenIndexResponse> {

    private final MetaDataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportOpenIndexAndFreezeAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, MetaDataIndexStateService indexStateService,
                                             ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                             DestructiveOperations destructiveOperations) {
        super(OpenIndexAndFreezeAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            OpenIndexAndFreezeRequest::new);
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected void doExecute(Task task, OpenIndexAndFreezeRequest request, ActionListener<OpenIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected OpenIndexResponse newResponse() {
        return new OpenIndexResponse();
    }

    @Override
    protected void masterOperation(OpenIndexAndFreezeRequest request, ClusterState state, ActionListener<OpenIndexResponse> listener) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new OpenIndexResponse(true, true));
            return;
        }

        clusterService.submitStateUpdateTask("toggle-frozen-settings", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                final MetaData.Builder builder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                for (Index index : concreteIndices) {
                    IndexMetaData meta = currentState.metaData().index(index);
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
                        // we never remove this block when unfreeze for now. we don't know if it was read-only
                    }
                    imdBuilder.settings(settingsBuilder);
                    builder.put(imdBuilder.build(), true);
                }
                return ClusterState.builder(currentState).blocks(blocks).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                OpenIndexClusterStateUpdateRequest updateRequest = new OpenIndexClusterStateUpdateRequest()
                    .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                    .indices(concreteIndices).waitForActiveShards(request.waitForActiveShards());
                indexStateService.openIndex(updateRequest, new ActionListener<OpenIndexClusterStateUpdateResponse>() {

                    @Override
                    public void onResponse(OpenIndexClusterStateUpdateResponse response) {
                        listener.onResponse(new OpenIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged()));
                    }

                    @Override
                    public void onFailure(Exception t) {
                        logger.debug(() -> new ParameterizedMessage("failed to open indices [{}]", (Object) concreteIndices), t);
                        listener.onFailure(t);
                    }
                });
            }

            @Override
            public void onFailure(final String source, final Exception e) {
                listener.onFailure(e);
            }

        });
    }

    @Override
    protected ClusterBlockException checkBlock(OpenIndexAndFreezeRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    public static class OpenIndexAndFreezeAction extends Action<OpenIndexResponse> {

        public static final OpenIndexAndFreezeAction INSTANCE = new OpenIndexAndFreezeAction();
        public static final String NAME = "indices:admin/open_and_freeze";

        private OpenIndexAndFreezeAction() {
            super(NAME);
        }

        @Override
        public OpenIndexResponse newResponse() {
            return new OpenIndexResponse();
        }
    }

    public static class OpenIndexAndFreezeRequest extends AcknowledgedRequest<OpenIndexAndFreezeRequest>
        implements IndicesRequest.Replaceable {
        private OpenIndexRequest openIndexRequest;
        private boolean freeze = true;

        public OpenIndexAndFreezeRequest() {
            openIndexRequest = new OpenIndexRequest();
        }

        public OpenIndexAndFreezeRequest(String... indices) {
            openIndexRequest = new OpenIndexRequest(indices);
        }

        @Override
        public ActionRequestValidationException validate() {
            return openIndexRequest.validate();
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
            openIndexRequest = new OpenIndexRequest();
            openIndexRequest.readFrom(in);
            freeze = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            openIndexRequest.writeTo(out);
            out.writeBoolean(freeze);
        }

        @Override
        public String[] indices() {
            return openIndexRequest.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return openIndexRequest.indicesOptions();
        }

        @Override
        public IndicesRequest indices(String... indices) {
            openIndexRequest.indices(indices);
            return this;
        }

        public OpenIndexAndFreezeRequest indicesOptions(IndicesOptions indicesOptions) {
            openIndexRequest.indicesOptions(indicesOptions);
            return this;
        }

        public ActiveShardCount waitForActiveShards() {
            return openIndexRequest.waitForActiveShards();
        }

        public OpenIndexAndFreezeRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
            openIndexRequest.waitForActiveShards(waitForActiveShards);
            return this;
        }

        public OpenIndexAndFreezeRequest waitForActiveShards(int waitForActiveShards) {
            openIndexRequest.waitForActiveShards(waitForActiveShards);
            return this;
        }
    }

}
