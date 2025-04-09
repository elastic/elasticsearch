/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.datastreams.PostDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PostDataStreamTransportAction extends TransportMasterNodeAction<PostDataStreamAction.Request, PostDataStreamAction.Response> {
    private static final Logger logger = LogManager.getLogger(PostDataStreamTransportAction.class);
    private static final Set<String> APPLY_TO_BACKING_INDICES = Set.of("index.number_of_replicas", "index.lifecycle.name");
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final MetadataUpdateSettingsService updateSettingsService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;

    @Inject
    public PostDataStreamTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        MetadataDataStreamsService metadataDataStreamsService,
        MetadataUpdateSettingsService updateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            PostDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PostDataStreamAction.Request::new,
            PostDataStreamAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.updateSettingsService = updateSettingsService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(
        Task task,
        PostDataStreamAction.Request request,
        ClusterState state,
        ActionListener<PostDataStreamAction.Response> listener
    ) throws Exception {
        String dataStreamName = request.getName();
        if (systemIndices.isSystemDataStream(dataStreamName)) {
            listener.onFailure(new ElasticsearchException("Cannot update a system data stream"));
            return;
        }

        metadataDataStreamsService.updateTemplateOverrides(
            projectResolver,
            request,
            dataStreamName,
            request.getTemplateOverrides(),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        final Index[] concreteIndices = clusterService.state()
                            .projectState(projectResolver.getProjectId())
                            .metadata()
                            .dataStreams()
                            .get(dataStreamName)
                            .getIndices()
                            .toArray(Index.EMPTY_ARRAY);
                        final Settings requestSettings = request.getTemplateOverrides().template().settings();
                        final Settings.Builder settingsBuilder = Settings.builder();
                        for (String setting : requestSettings.keySet()) {
                            if (APPLY_TO_BACKING_INDICES.contains(setting)) {
                                settingsBuilder.put(setting, requestSettings.get(setting));
                            }
                        }
                        Settings settingsToUpdate = settingsBuilder.build();
                        // TODO do we want to do this once per setting per index?
                        updateSettingsService.updateSettings(
                            new UpdateSettingsClusterStateUpdateRequest(
                                projectResolver.getProjectId(),
                                request.masterNodeTimeout(),
                                request.ackTimeout(),
                                settingsToUpdate,
                                UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                                UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES,
                                concreteIndices
                            ),
                            new ActionListener<>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    DataStream dataStream = clusterService.state()
                                        .projectState(projectResolver.getProjectId())
                                        .metadata()
                                        .dataStreams()
                                        .get(dataStreamName);
                                    List<PostDataStreamAction.DataStreamResponse.IndexSettingResult> indexSettingResults =
                                        new ArrayList<>();
                                    for (String setting : requestSettings.keySet()) {
                                        PostDataStreamAction.DataStreamResponse.IndexSettingResult indexSettingResult =
                                            new PostDataStreamAction.DataStreamResponse.IndexSettingResult(
                                                setting,
                                                APPLY_TO_BACKING_INDICES.contains(setting),
                                                List.of()
                                            );
                                        indexSettingResults.add(indexSettingResult);
                                    }
                                    PostDataStreamAction.DataStreamResponse dataStreamResponse =
                                        new PostDataStreamAction.DataStreamResponse(
                                            dataStreamName,
                                            true,
                                            null,
                                            dataStream.getEffectiveIndexTemplate(
                                                clusterService.state().projectState(projectResolver.getProjectId()).metadata()
                                            ),
                                            indexSettingResults
                                        );
                                    PostDataStreamAction.Response response = new PostDataStreamAction.Response(List.of(dataStreamResponse));
                                    listener.onResponse(response);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.debug(
                                        () -> "failed to update settings on indices [" + Arrays.toString(concreteIndices) + "]",
                                        e
                                    );
                                    if (e instanceof IllegalArgumentException) {
                                        DataStream dataStream = clusterService.state()
                                            .projectState(projectResolver.getProjectId())
                                            .metadata()
                                            .dataStreams()
                                            .get(dataStreamName);
                                        List<PostDataStreamAction.DataStreamResponse.IndexSettingResult> indexSettingResults =
                                            new ArrayList<>();
                                        for (String setting : requestSettings.keySet()) {
                                            boolean attemptedToSet = APPLY_TO_BACKING_INDICES.contains(setting);
                                            List<PostDataStreamAction.DataStreamResponse.IndexSettingError> indexSettingErrors;
                                            if (attemptedToSet) {
                                                indexSettingErrors = new ArrayList<>();
                                                for (Index index : concreteIndices) {
                                                    indexSettingErrors.add(
                                                        new PostDataStreamAction.DataStreamResponse.IndexSettingError(
                                                            index.getName(),
                                                            e.getMessage()
                                                        )
                                                    );
                                                }
                                            } else {
                                                indexSettingErrors = List.of();
                                            }
                                            PostDataStreamAction.DataStreamResponse.IndexSettingResult indexSettingResult =
                                                new PostDataStreamAction.DataStreamResponse.IndexSettingResult(
                                                    setting,
                                                    attemptedToSet,
                                                    indexSettingErrors
                                                );
                                            indexSettingResults.add(indexSettingResult);
                                        }
                                        PostDataStreamAction.DataStreamResponse dataStreamResponse =
                                            new PostDataStreamAction.DataStreamResponse(
                                                dataStreamName,
                                                true,
                                                null,
                                                dataStream.getEffectiveIndexTemplate(
                                                    clusterService.state().projectState(projectResolver.getProjectId()).metadata()
                                                ),
                                                indexSettingResults
                                            );
                                        PostDataStreamAction.Response response = new PostDataStreamAction.Response(
                                            List.of(dataStreamResponse)
                                        );
                                        listener.onResponse(response);
                                    } else {
                                        listener.onFailure(e);
                                    }
                                }
                            }
                        );
                    } else {
                        listener.onFailure(new ElasticsearchException("Unable to set "));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PostDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
