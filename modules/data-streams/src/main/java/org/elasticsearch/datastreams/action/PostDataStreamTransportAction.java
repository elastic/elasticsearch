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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.datastreams.PostDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        String dataStreamNamePattern = request.getName();
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterService.state(),
            IndicesOptions.DEFAULT,
            dataStreamNamePattern
        );
        List<PostDataStreamAction.DataStreamResponse> dataStreamResponses = new ArrayList<>();
        CountDownActionListener countDownListener = new CountDownActionListener(dataStreamNames.size(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                listener.onResponse(new PostDataStreamAction.Response(dataStreamResponses));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e); // TODO ?
            }
        });
        // TODO there's a much better way
        for (String dataStreamName : dataStreamNames) {
            updateSingleDataStream(
                dataStreamName,
                request.getTemplateOverrides(),
                request.masterNodeTimeout(),
                request.ackTimeout(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(PostDataStreamAction.DataStreamResponse dataStreamResponse) {
                        dataStreamResponses.add(dataStreamResponse);
                        countDownListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // TODO this is not actually correct at all:
                        dataStreamResponses.add(
                            new PostDataStreamAction.DataStreamResponse(dataStreamName, false, e.getMessage(), null, List.of())
                        );
                        countDownListener.onResponse(null);
                    }
                }
            );
        }
    }

    private void updateSingleDataStream(
        String dataStreamName,
        ComposableIndexTemplate templateOverrides,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<PostDataStreamAction.DataStreamResponse> listener
    ) {
        if (systemIndices.isSystemDataStream(dataStreamName)) {
            listener.onResponse(
                new PostDataStreamAction.DataStreamResponse(dataStreamName, false, "Cannot update a system data stream", null, List.of())
            );
            return;
        }
        metadataDataStreamsService.updateTemplateOverrides(
            projectResolver,
            masterNodeTimeout,
            ackTimeout,
            dataStreamName,
            templateOverrides,
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        final List<Index> concreteIndices = clusterService.state()
                            .projectState(projectResolver.getProjectId())
                            .metadata()
                            .dataStreams()
                            .get(dataStreamName)
                            .getIndices();
                        final Settings requestSettings = templateOverrides.template().settings();
                        final List<PostDataStreamAction.DataStreamResponse.IndexSettingResult> indexSettingResults = new ArrayList<>();
                        CountDownActionListener settingCountDownListener = new CountDownActionListener(
                            requestSettings.size() + 1,
                            new ActionListener<>() {
                                @Override
                                public void onResponse(Void unused) {
                                    ComposableIndexTemplate effectiveIndexTemplate = clusterService.state()
                                        .projectState(projectResolver.getProjectId())
                                        .metadata()
                                        .dataStreams()
                                        .get(dataStreamName)
                                        .getEffectiveIndexTemplate(
                                            clusterService.state().projectState(projectResolver.getProjectId()).metadata()
                                        );
                                    listener.onResponse(
                                        new PostDataStreamAction.DataStreamResponse(
                                            dataStreamName,
                                            true,
                                            null,
                                            effectiveIndexTemplate,
                                            indexSettingResults
                                        )
                                    );
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            }
                        );
                        settingCountDownListener.onResponse(null); // handles the case when there were zero settings
                        ActionListener<PostDataStreamAction.DataStreamResponse.IndexSettingResult> indexSettingResultListener =
                            new ActionListener<>() {
                                // Called each time we have results for all indices for a single setting
                                @Override
                                public void onResponse(PostDataStreamAction.DataStreamResponse.IndexSettingResult indexSettingResult) {
                                    indexSettingResults.add(indexSettingResult);
                                    settingCountDownListener.onResponse(null);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    settingCountDownListener.onFailure(e);
                                }
                            };
                        for (String setting : requestSettings.keySet()) {
                            if (APPLY_TO_BACKING_INDICES.contains(setting)) {
                                final List<PostDataStreamAction.DataStreamResponse.IndexSettingError> errors = new ArrayList<>();
                                CountDownActionListener indexCountDownListener = new CountDownActionListener(
                                    concreteIndices.size() + 1,
                                    new ActionListener<>() {
                                        @Override
                                        public void onResponse(Void unused) {
                                            indexSettingResultListener.onResponse(
                                                new PostDataStreamAction.DataStreamResponse.IndexSettingResult(setting, true, errors)
                                            );
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            indexSettingResultListener.onFailure(e);
                                        }
                                    }
                                );
                                indexCountDownListener.onResponse(null); // handles the case where there were zero indices
                                for (Index index : concreteIndices) {
                                    updateSingleSettingForSingleIndex(
                                        setting,
                                        requestSettings.get(setting),
                                        index,
                                        masterNodeTimeout,
                                        ackTimeout,
                                        new ActionListener<>() {
                                            @Override
                                            public void onResponse(AcknowledgedResponse response) {
                                                if (response.isAcknowledged() == false) {
                                                    errors.add(
                                                        new PostDataStreamAction.DataStreamResponse.IndexSettingError(
                                                            index.getName(),
                                                            "Updating setting not acknowledged for unknown reason"
                                                        )
                                                    );
                                                }
                                                indexCountDownListener.onResponse(null);

                                            }

                                            @Override
                                            public void onFailure(Exception e) {
                                                errors.add(
                                                    new PostDataStreamAction.DataStreamResponse.IndexSettingError(
                                                        index.getName(),
                                                        e.getMessage()
                                                    )
                                                );
                                                indexCountDownListener.onResponse(null);
                                            }
                                        }
                                    );
                                }
                            } else {
                                // This is not a setting that we will apply to backing indices
                                indexSettingResults.add(
                                    new PostDataStreamAction.DataStreamResponse.IndexSettingResult(setting, false, List.of())
                                );
                                settingCountDownListener.onResponse(null);
                            }
                        }
                    } else {
                        listener.onResponse(
                            new PostDataStreamAction.DataStreamResponse(
                                dataStreamName,
                                false,
                                "Updating template overrides not accepted for unknown reasons",
                                null,
                                List.of()
                            )
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    private void updateSingleSettingForSingleIndex(
        String settingName,
        Object settingValue,
        Index index,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        updateSettingsService.updateSettings(
            new UpdateSettingsClusterStateUpdateRequest(
                projectResolver.getProjectId(),
                masterNodeTimeout,
                ackTimeout,
                Settings.builder().loadFromMap(Map.of(settingName, settingValue)).build(),
                UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES,
                index
            ),
            listener
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PostDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
