/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Get index action.
 */
public class TransportGetIndexAction extends TransportLocalProjectMetadataAction<GetIndexRequest, GetIndexResponse> {

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * NB prior to 9.1 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings,
        ProjectResolver projectResolver
    ) {
        super(
            GetIndexAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indicesService = indicesService;
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexScopedSettings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetIndexRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexRequest request, ProjectState state) {
        return state.blocks()
            .indicesBlockedException(
                state.projectId(),
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state.metadata(), request)
            );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        final GetIndexRequest request,
        final ProjectState state,
        final ActionListener<GetIndexResponse> listener
    ) throws Exception {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state.metadata(), request);
        Map<String, MappingMetadata> mappingsResult = ImmutableOpenMap.of();
        Map<String, List<AliasMetadata>> aliasesResult = Map.of();
        Map<String, Settings> settings = Map.of();
        Map<String, Settings> defaultSettings = Map.of();
        ProjectMetadata project = state.metadata();
        Map<String, String> dataStreams = project.findDataStreams(concreteIndices)
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, v -> v.getValue().getName()));
        Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        for (Feature feature : features) {
            checkCancellation(task);
            switch (feature) {
                case MAPPINGS:
                    if (doneMappings == false) {
                        mappingsResult = project.findMappings(
                            concreteIndices,
                            indicesService.getFieldFilter(),
                            () -> checkCancellation(task)
                        );
                        doneMappings = true;
                    }
                    break;
                case ALIASES:
                    if (doneAliases == false) {
                        aliasesResult = project.findAllAliases(concreteIndices);
                        doneAliases = true;
                    }
                    break;
                case SETTINGS:
                    if (doneSettings == false) {
                        Map<String, Settings> settingsMapBuilder = new HashMap<>();
                        Map<String, Settings> defaultSettingsMapBuilder = new HashMap<>();
                        for (String index : concreteIndices) {
                            checkCancellation(task);
                            Settings indexSettings = project.index(index).getSettings();
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
                        settings = Collections.unmodifiableMap(settingsMapBuilder);
                        defaultSettings = Collections.unmodifiableMap(defaultSettingsMapBuilder);
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings, defaultSettings, dataStreams));
    }

    private static void checkCancellation(Task task) {
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.ensureNotCancelled();
        }
    }
}
