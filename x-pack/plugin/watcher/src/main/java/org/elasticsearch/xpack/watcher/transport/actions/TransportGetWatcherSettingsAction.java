/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.GetWatcherSettingsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.UpdateWatcherSettingsAction;

import static org.elasticsearch.xpack.watcher.transport.actions.TransportUpdateWatcherSettingsAction.WATCHER_INDEX_NAME;

public class TransportGetWatcherSettingsAction extends TransportMasterNodeAction<
    GetWatcherSettingsAction.Request,
    GetWatcherSettingsAction.Response> {

    @Inject
    public TransportGetWatcherSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetWatcherSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetWatcherSettingsAction.Request::new,
            indexNameExpressionResolver,
            GetWatcherSettingsAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetWatcherSettingsAction.Request request,
        ClusterState state,
        ActionListener<GetWatcherSettingsAction.Response> listener
    ) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        // Stashing and un-stashing the context allows warning headers about accessing a system index to be ignored.
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            IndexMetadata metadata = state.metadata().index(WATCHER_INDEX_NAME);
            if (metadata == null) {
                listener.onResponse(new GetWatcherSettingsAction.Response(Settings.EMPTY));
            } else {
                listener.onResponse(new GetWatcherSettingsAction.Response(filterSettableSettings(metadata.getSettings())));
            }
        }
    }

    /**
     * Filters the settings to only those settable by the user (using the update watcher settings API).
     */
    private static Settings filterSettableSettings(Settings settings) {
        Settings.Builder builder = Settings.builder();
        for (String settingName : UpdateWatcherSettingsAction.ALLOWED_SETTING_KEYS) {
            if (settings.hasValue(settingName)) {
                builder.put(settingName, settings.get(settingName));
            }
        }
        return builder.build();
    }

    @Override
    protected ClusterBlockException checkBlock(GetWatcherSettingsAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (globalBlock != null) {
            return globalBlock;
        }
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.LENIENT_EXPAND_OPEN, WATCHER_INDEX_NAME)
            );
    }
}
