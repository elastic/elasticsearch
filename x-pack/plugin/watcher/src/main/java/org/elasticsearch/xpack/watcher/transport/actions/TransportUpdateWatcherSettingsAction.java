/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.UpdateWatcherSettingsAction;

public class TransportUpdateWatcherSettingsAction extends TransportMasterNodeAction<
    UpdateWatcherSettingsAction.Request,
    AcknowledgedResponse> {

    public static final String WATCHER_INDEX_NAME = ".watches";

    private static final Logger logger = LogManager.getLogger(TransportUpdateWatcherSettingsAction.class);
    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpdateWatcherSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        MetadataUpdateSettingsService updateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpdateWatcherSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateWatcherSettingsAction.Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            ThreadPool.Names.SAME
        );
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateWatcherSettingsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final IndexMetadata watcherIndexMd = state.metadata().index(WATCHER_INDEX_NAME);
        if (watcherIndexMd == null) {
            // Index does not exist, so fail fast
            listener.onFailure(new ResourceNotFoundException("no Watches found on which to modify settings"));
            return;
        }
        final Settings newSettings = Settings.builder().loadFromMap(request.settings()).build();
        final UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest().indices(
            new Index[] { watcherIndexMd.getIndex() }
        ).settings(newSettings).ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout());

        final ThreadContext threadContext = threadPool.getThreadContext();
        // Stashing and un-stashing the context allows warning headers about accessing a system index to be ignored.
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            updateSettingsService.updateSettings(clusterStateUpdateRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        logger.info("successfully updated Watcher service settings to {}", request.settings());
                    } else {
                        logger.warn("updating Watcher service settings to {} was not acknowledged", request.settings());
                    }
                    listener.onResponse(acknowledgedResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> "failed to update settings for Watcher service", e);
                    listener.onFailure(e);
                }
            });
        }
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateWatcherSettingsAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.LENIENT_EXPAND_OPEN, WATCHER_INDEX_NAME)
            );
    }
}
