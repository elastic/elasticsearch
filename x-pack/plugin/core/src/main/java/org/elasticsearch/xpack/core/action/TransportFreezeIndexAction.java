/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.OpenIndexClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
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
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class TransportFreezeIndexAction extends
    TransportMasterNodeAction<TransportFreezeIndexAction.FreezeRequest, TransportFreezeIndexAction.FreezeResponse> {

    private final DestructiveOperations destructiveOperations;
    private final MetaDataIndexStateService indexStateService;
    private final TransportCloseIndexAction transportCloseIndexAction;

    @Inject
    public TransportFreezeIndexAction(MetaDataIndexStateService indexStateService, TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      DestructiveOperations destructiveOperations,
                                      TransportCloseIndexAction transportCloseIndexAction) {
        super(FreezeIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            FreezeRequest::new);
        this.destructiveOperations = destructiveOperations;
        this.indexStateService = indexStateService;
        this.transportCloseIndexAction = transportCloseIndexAction;
    }
    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected void doExecute(Task task, FreezeRequest request, ActionListener<FreezeResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected FreezeResponse newResponse() {
        return new FreezeResponse();
    }

    private Index[] resolveIndices(FreezeRequest request, ClusterState state) {
        List<Index> indices = new ArrayList<>();
        for (Index index : indexNameExpressionResolver.concreteIndices(state, request)) {
            IndexMetaData metaData = state.metaData().index(index);
            Settings settings = metaData.getSettings();
            // only unfreeze if we are frozen and only freeze if we are not frozen already.
            // this prevents all indices that are already frozen that match a pattern to
            // go through the cycles again.
            if ((request.freeze() && FrozenEngine.INDEX_FROZEN.get(settings) == false) ||
                (request.freeze() == false && FrozenEngine.INDEX_FROZEN.get(settings))) {
                indices.add(index);
            }
        }
        if (indices.isEmpty() && request.indicesOptions().allowNoIndices() == false) {
            throw new ResourceNotFoundException("no index found to " + (request.freeze() ? "freeze" : "unfreeze"));
        }
        return indices.toArray(Index.EMPTY_ARRAY);
    }

    @Override
    protected void masterOperation(FreezeRequest request, ClusterState state, ActionListener<FreezeResponse> listener) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void masterOperation(Task task, TransportFreezeIndexAction.FreezeRequest request, ClusterState state,
                                   ActionListener<TransportFreezeIndexAction.FreezeResponse> listener) throws Exception {
        final Index[] concreteIndices = resolveIndices(request, state);
        if (concreteIndices.length == 0) {
            listener.onResponse(new FreezeResponse(true, true));
            return;
        }

        final CloseIndexClusterStateUpdateRequest closeRequest = new CloseIndexClusterStateUpdateRequest(task.getId())
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .indices(concreteIndices);

        indexStateService.closeIndices(closeRequest, new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(final AcknowledgedResponse response) {
                if (response.isAcknowledged()) {
                    toggleFrozenSettings(concreteIndices, request, listener);
                } else {
                    // TODO improve FreezeResponse so that it also reports failures from the close index API
                    listener.onResponse(new FreezeResponse(false, false));
                }
            }

            @Override
            public void onFailure(final Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to close indices [{}]", (Object) concreteIndices), t);
                listener.onFailure(t);
            }
        });
    }

    private void toggleFrozenSettings(final Index[] concreteIndices, final FreezeRequest request,
                                      final ActionListener<FreezeResponse> listener) {
        clusterService.submitStateUpdateTask("toggle-frozen-settings",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.URGENT, request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    OpenIndexClusterStateUpdateRequest updateRequest = new OpenIndexClusterStateUpdateRequest()
                        .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                        .indices(concreteIndices).waitForActiveShards(request.waitForActiveShards());
                    indexStateService.openIndex(updateRequest, new ActionListener<OpenIndexClusterStateUpdateResponse>() {
                        @Override
                        public void onResponse(OpenIndexClusterStateUpdateResponse openIndexClusterStateUpdateResponse) {
                            listener.onResponse(new FreezeResponse(openIndexClusterStateUpdateResponse.isAcknowledged(),
                                openIndexClusterStateUpdateResponse.isShardsAcknowledged()));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }) {
            @Override
            public ClusterState execute(ClusterState currentState) {
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
                            .put(FrozenEngine.INDEX_FROZEN.getKey(), request.freeze())
                            .put(IndexSettings.INDEX_SEARCH_THROTTLED.getKey(), request.freeze());
                    if (request.freeze()) {
                        settingsBuilder.put("index.blocks.write", true);
                        blocks.addIndexBlock(index.getName(), IndexMetaData.INDEX_WRITE_BLOCK);
                    } else {
                        settingsBuilder.remove("index.blocks.write");
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

    public static class FreezeResponse extends OpenIndexResponse {
        public FreezeResponse() {
            super();
        }

        public FreezeResponse(boolean acknowledged, boolean shardsAcknowledged) {
            super(acknowledged, shardsAcknowledged);
        }
    }

    public static class FreezeIndexAction extends Action<FreezeResponse> {

        public static final FreezeIndexAction INSTANCE = new FreezeIndexAction();
        public static final String NAME = "indices:admin/freeze";

        private FreezeIndexAction() {
            super(NAME);
        }

        @Override
        public FreezeResponse newResponse() {
            return new FreezeResponse();
        }
    }

    public static class FreezeRequest extends AcknowledgedRequest<FreezeRequest>
        implements IndicesRequest.Replaceable {
        private String[] indices;
        private boolean freeze = true;
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
        private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

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

        public FreezeRequest setFreeze(boolean freeze) {
            this.freeze = freeze;
            return this;
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
            waitForActiveShards = ActiveShardCount.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            indicesOptions.writeIndicesOptions(out);
            out.writeStringArray(indices);
            out.writeBoolean(freeze);
            waitForActiveShards.writeTo(out);
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

        public ActiveShardCount waitForActiveShards() {
            return waitForActiveShards;
        }

        /**
         * Sets the number of shard copies that should be active for indices opening to return.
         * Defaults to {@link ActiveShardCount#DEFAULT}, which will wait for one shard copy
         * (the primary) to become active. Set this value to {@link ActiveShardCount#ALL} to
         * wait for all shards (primary and all replicas) to be active before returning.
         * Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
         * non-negative integer, up to the number of copies per shard (number of replicas + 1),
         * to wait for the desired amount of shard copies to become active before returning.
         * Indices opening will only wait up until the timeout value for the number of shard copies
         * to be active before returning.  Check {@link OpenIndexResponse#isShardsAcknowledged()} to
         * determine if the requisite shard copies were all started before returning or timing out.
         *
         * @param waitForActiveShards number of active shard copies to wait on
         */
        public FreezeRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
            this.waitForActiveShards = waitForActiveShards;
            return this;
        }
    }
}
