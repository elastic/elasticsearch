/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Get index action.
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings
    ) {
        super(
            GetIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexRequest::new,
            indexNameExpressionResolver,
            GetIndexResponse::new
        );
        this.indicesService = indicesService;
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected void doMasterOperation(
        Task task,
        final GetIndexRequest request,
        String[] concreteIndices,
        final ClusterState state,
        final ActionListener<GetIndexResponse> listener
    ) {
        ImmutableOpenMap<String, MappingMetadata> mappingsResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, List<AliasMetadata>> aliasesResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> defaultSettings = ImmutableOpenMap.of();
        ImmutableOpenMap<String, String> dataStreams = ImmutableOpenMap.<String, String>builder()
            .putAllFromMap(
                state.metadata()
                    .findDataStreams(concreteIndices)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getName()))
            )
            .build();
        Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        for (Feature feature : features) {
            switch (feature) {
                case MAPPINGS:
                    if (doneMappings == false) {
                        mappingsResult = state.metadata()
                            .findMappings(concreteIndices, indicesService.getFieldFilter(), Metadata.ON_NEXT_INDEX_FIND_MAPPINGS_NOOP);
                        doneMappings = true;
                    }
                    break;
                case ALIASES:
                    if (doneAliases == false) {
                        aliasesResult = state.metadata().findAllAliases(concreteIndices);
                        doneAliases = true;
                    }
                    break;
                case SETTINGS:
                    if (doneSettings == false) {
                        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();
                        ImmutableOpenMap.Builder<String, Settings> defaultSettingsMapBuilder = ImmutableOpenMap.builder();
                        for (String index : concreteIndices) {
                            Settings indexSettings = state.metadata().index(index).getSettings();
                            if (request.humanReadable()) {
                                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
                            }
                            settingsMapBuilder.put(index, indexSettings);
                            if (request.includeDefaults()) {
                                Settings defaultIndexSettings = settingsFilter.filter(
                                    indexScopedSettings.diff(indexSettings, Settings.EMPTY)
                                );
                                defaultSettingsMapBuilder.put(index, defaultIndexSettings);
                            }
                        }
                        settings = settingsMapBuilder.build();
                        defaultSettings = defaultSettingsMapBuilder.build();
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings, defaultSettings, dataStreams));
    }
}
