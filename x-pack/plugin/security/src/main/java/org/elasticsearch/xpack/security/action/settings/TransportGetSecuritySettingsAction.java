/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.settings.GetSecuritySettingsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.UpdateWatcherSettingsAction;

import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_TOKENS_ALIAS;

public class TransportGetSecuritySettingsAction extends TransportMasterNodeAction<
    GetSecuritySettingsAction.Request,
    GetSecuritySettingsAction.Response> {

    @Inject
    public TransportGetSecuritySettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSecuritySettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSecuritySettingsAction.Request::new,
            indexNameExpressionResolver,
            GetSecuritySettingsAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetSecuritySettingsAction.Request request,
        ClusterState state,
        ActionListener<GetSecuritySettingsAction.Response> listener
    ) {
        listener.onResponse(
            new GetSecuritySettingsAction.Response(
                getFilteredSettingsForIndex(SECURITY_MAIN_ALIAS, state),
                getFilteredSettingsForIndex(SECURITY_TOKENS_ALIAS, state),
                getFilteredSettingsForIndex(SECURITY_PROFILE_ALIAS, state)
            )
        );
    }

    /**
     * Filters the settings to only those settable by the user (using the update watcher settings API).
     */
    private static Settings getFilteredSettingsForIndex(String indexName, ClusterState state) {
        // Check the indices lookup to resolve the alias

        return resolveConcreteIndex(indexName, state).map(idx -> state.metadata().index(idx))
            .map(IndexMetadata::getSettings)
            .map(settings -> {
                Settings.Builder builder = Settings.builder();
                for (String settingName : UpdateWatcherSettingsAction.ALLOWED_SETTING_KEYS) {
                    if (settings.hasValue(settingName)) {
                        builder.put(settingName, settings.get(settingName));
                    }
                }
                return builder.build();
            })
            .orElse(Settings.EMPTY);
    }

    static Optional<Index> resolveConcreteIndex(String indexAbstractionName, ClusterState state) {
        IndexAbstraction abstraction = state.metadata().getIndicesLookup().get(indexAbstractionName);
        if (abstraction == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(abstraction.getWriteIndex());
    }

    @Override
    protected ClusterBlockException checkBlock(GetSecuritySettingsAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (globalBlock != null) {
            return globalBlock;
        }

        // Don't use the indexNameExpressionResolver here so we don't trigger a system index deprecation warning
        String[] indices = Stream.of(SECURITY_MAIN_ALIAS, SECURITY_TOKENS_ALIAS, SECURITY_PROFILE_ALIAS)
            .map(alias -> resolveConcreteIndex(alias, state).map(Index::getName))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indices);
    }

}
