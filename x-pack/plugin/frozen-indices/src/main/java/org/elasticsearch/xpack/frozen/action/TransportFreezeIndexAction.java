/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.frozen.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.protocol.xpack.frozen.FreezeResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TransportFreezeIndexAction extends
    TransportMasterNodeAction<FreezeRequest, FreezeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportFreezeIndexAction.class);

    private final DestructiveOperations destructiveOperations;
    private final MetaDataIndexStateService indexStateService;

    @Inject
    public TransportFreezeIndexAction(MetaDataIndexStateService indexStateService, TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      DestructiveOperations destructiveOperations) {
        super(FreezeIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, FreezeRequest::new,
            indexNameExpressionResolver);
        this.destructiveOperations = destructiveOperations;
        this.indexStateService = indexStateService;
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
    protected FreezeResponse read(StreamInput in) throws IOException {
        return new FreezeResponse(in);
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
    protected void masterOperation(Task task, FreezeRequest request, ClusterState state,
                                   ActionListener<FreezeResponse> listener) throws Exception {
        final Index[] concreteIndices = resolveIndices(request, state);
        if (concreteIndices.length == 0) {
            listener.onResponse(new FreezeResponse(true, true));
            return;
        }

        final CloseIndexClusterStateUpdateRequest closeRequest = new CloseIndexClusterStateUpdateRequest(task.getId())
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .indices(concreteIndices);

        indexStateService.closeIndices(closeRequest, new ActionListener<>() {
            @Override
            public void onResponse(final CloseIndexResponse response) {
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
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    OpenIndexClusterStateUpdateRequest updateRequest = new OpenIndexClusterStateUpdateRequest()
                        .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                        .indices(concreteIndices).waitForActiveShards(request.waitForActiveShards());
                    indexStateService.openIndex(updateRequest, new ActionListener<>() {
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
                    imdBuilder.settingsVersion(meta.getSettingsVersion() + 1);
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
}
