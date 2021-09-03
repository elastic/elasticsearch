/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.protocol.xpack.frozen.FreezeResponse;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;

import java.util.ArrayList;
import java.util.List;

public final class TransportFreezeIndexAction extends TransportMasterNodeAction<FreezeRequest, FreezeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportFreezeIndexAction.class);

    private final MetadataIndexStateService indexStateService;

    @Inject
    public TransportFreezeIndexAction(
        MetadataIndexStateService indexStateService,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            FreezeIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            FreezeRequest::new,
            indexNameExpressionResolver,
            FreezeResponse::new,
            ThreadPool.Names.SAME
        );
        this.indexStateService = indexStateService;
    }

    @Override
    protected void doExecute(Task task, FreezeRequest request, ActionListener<FreezeResponse> listener) {
        super.doExecute(task, request, listener);
    }

    private Index[] resolveIndices(FreezeRequest request, ClusterState state) {
        List<Index> indices = new ArrayList<>();
        for (Index index : indexNameExpressionResolver.concreteIndices(state, request)) {
            IndexMetadata metadata = state.metadata().index(index);
            Settings settings = metadata.getSettings();
            // only unfreeze if we are frozen so that all indices that are already unfrozen that match a pattern
            // do not go through the cycles again
            if (FrozenEngine.INDEX_FROZEN.get(settings)) {
                indices.add(index);
            }
        }
        if (indices.isEmpty() && request.indicesOptions().allowNoIndices() == false) {
            throw new ResourceNotFoundException("no index found to unfreeze");
        }
        return indices.toArray(Index.EMPTY_ARRAY);
    }

    @Override
    protected void masterOperation(Task task, FreezeRequest request, ClusterState state, ActionListener<FreezeResponse> listener) {
        final Index[] concreteIndices = resolveIndices(request, state);
        if (concreteIndices.length == 0) {
            listener.onResponse(new FreezeResponse(true, true));
            return;
        }

        final CloseIndexClusterStateUpdateRequest closeRequest = new CloseIndexClusterStateUpdateRequest(task.getId()).ackTimeout(
            request.timeout()
        ).masterNodeTimeout(request.masterNodeTimeout()).indices(concreteIndices);

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

    private void toggleFrozenSettings(
        final Index[] concreteIndices,
        final FreezeRequest request,
        final ActionListener<FreezeResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "toggle-frozen-settings",
            new AckedClusterStateUpdateTask(Priority.URGENT, request, listener.delegateFailure((delegate, acknowledgedResponse) -> {
                OpenIndexClusterStateUpdateRequest updateRequest = new OpenIndexClusterStateUpdateRequest().ackTimeout(request.timeout())
                    .masterNodeTimeout(request.masterNodeTimeout())
                    .indices(concreteIndices)
                    .waitForActiveShards(request.waitForActiveShards());
                indexStateService.openIndex(
                    updateRequest,
                    delegate.delegateFailure(
                        (l, openIndexClusterStateUpdateResponse) -> l.onResponse(
                            new FreezeResponse(
                                openIndexClusterStateUpdateResponse.isAcknowledged(),
                                openIndexClusterStateUpdateResponse.isShardsAcknowledged()
                            )
                        )
                    )
                );
            })) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final Metadata.Builder builder = Metadata.builder(currentState.metadata());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    for (Index index : concreteIndices) {
                        final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
                        if (indexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            throw new IllegalStateException("index [" + index.getName() + "] is not closed");
                        }
                        final Settings.Builder settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                        settingsBuilder.remove(FrozenEngine.INDEX_FROZEN.getKey());
                        settingsBuilder.remove(IndexSettings.INDEX_SEARCH_THROTTLED.getKey());
                        if (SearchableSnapshotsSettings.isSearchableSnapshotStore(indexMetadata.getSettings()) == false) {
                            settingsBuilder.remove("index.blocks.write");
                            blocks.removeIndexBlock(index.getName(), IndexMetadata.INDEX_WRITE_BLOCK);
                        }
                        builder.put(
                            IndexMetadata.builder(indexMetadata)
                                .settingsVersion(indexMetadata.getSettingsVersion() + 1)
                                .settings(settingsBuilder)
                                .build(),
                            true
                        );
                    }
                    return ClusterState.builder(currentState).blocks(blocks).metadata(builder).build();
                }
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(FreezeRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }
}
