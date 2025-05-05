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
import org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.EMPTY;
import static org.elasticsearch.common.settings.Settings.builder;

public class TransportUpdateDataStreamSettingsAction extends TransportMasterNodeAction<
    UpdateDataStreamSettingsAction.Request,
    UpdateDataStreamSettingsAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportUpdateDataStreamSettingsAction.class);
    private static final Set<String> APPLY_TO_BACKING_INDICES = Set.of("index.lifecycle.name");
    private static final Set<String> APPLY_TO_DATA_STREAM_ONLY = Set.of("index.number_of_shards");
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final MetadataUpdateSettingsService updateSettingsService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportUpdateDataStreamSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        MetadataDataStreamsService metadataDataStreamsService,
        MetadataUpdateSettingsService updateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        SettingsFilter settingsFilter
    ) {
        super(
            UpdateDataStreamSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateDataStreamSettingsAction.Request::new,
            UpdateDataStreamSettingsAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
        this.metadataDataStreamsService = metadataDataStreamsService;
        this.updateSettingsService = updateSettingsService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateDataStreamSettingsAction.Request request,
        ClusterState state,
        ActionListener<UpdateDataStreamSettingsAction.Response> listener
    ) throws Exception {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterService.state(),
            IndicesOptions.DEFAULT,
            request.indices()
        );
        List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> dataStreamSettingsResponse = new ArrayList<>();
        CountDownActionListener countDownListener = new CountDownActionListener(dataStreamNames.size() + 1, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                listener.onResponse(new UpdateDataStreamSettingsAction.Response(dataStreamSettingsResponse));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
        countDownListener.onResponse(null);
        for (String dataStreamName : dataStreamNames) {
            updateSingleDataStream(
                dataStreamName,
                request.getSettings(),
                request.masterNodeTimeout(),
                request.ackTimeout(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(UpdateDataStreamSettingsAction.DataStreamSettingsResponse dataStreamResponse) {
                        dataStreamSettingsResponse.add(dataStreamResponse);
                        countDownListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        dataStreamSettingsResponse.add(
                            new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                                dataStreamName,
                                false,
                                e.getMessage(),
                                EMPTY,
                                EMPTY,
                                UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult.EMPTY
                            )
                        );
                        countDownListener.onResponse(null);
                    }
                }
            );
        }
    }

    private void updateSingleDataStream(
        String dataStreamName,
        Settings settingsOverrides,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> listener
    ) {
        logger.debug("updating settings for {}", dataStreamName);
        Set<String> settingsToReject = new HashSet<>();
        for (String settingName : settingsOverrides.keySet()) {
            if (APPLY_TO_BACKING_INDICES.contains(settingName) == false && APPLY_TO_DATA_STREAM_ONLY.contains(settingName) == false) {
                settingsToReject.add(settingName);
            }
        }
        if (settingsToReject.isEmpty() == false) {
            listener.onResponse(
                new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                    dataStreamName,
                    false,
                    Strings.format("Cannot set the following settings on a data stream: [%s]", String.join(",", settingsToReject)),
                    EMPTY,
                    EMPTY,
                    UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult.EMPTY
                )
            );
            return;
        }

        if (systemIndices.isSystemDataStream(dataStreamName)) {
            listener.onResponse(
                new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                    dataStreamName,
                    false,
                    "Cannot update a system data stream",
                    EMPTY,
                    EMPTY,
                    UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult.EMPTY
                )
            );
            return;
        }
        metadataDataStreamsService.updateSettings(
            projectResolver.getProjectId(),
            masterNodeTimeout,
            ackTimeout,
            dataStreamName,
            settingsOverrides,
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        updateSettingsOnIndices(dataStreamName, settingsOverrides, masterNodeTimeout, ackTimeout, listener);
                    } else {
                        listener.onResponse(
                            new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                                dataStreamName,
                                false,
                                "Updating settings not accepted for unknown reasons",
                                EMPTY,
                                EMPTY,
                                UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult.EMPTY
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

    private void updateSettingsOnIndices(
        String dataStreamName,
        Settings requestSettings,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<UpdateDataStreamSettingsAction.DataStreamSettingsResponse> listener
    ) {
        Map<String, Object> settingsToApply = new HashMap<>();
        List<String> appliedToDataStreamOnly = new ArrayList<>();
        List<String> appliedToDataStreamAndBackingIndices = new ArrayList<>();
        for (String settingName : requestSettings.keySet()) {
            if (APPLY_TO_BACKING_INDICES.contains(settingName)) {
                settingsToApply.put(settingName, requestSettings.get(settingName));
                appliedToDataStreamAndBackingIndices.add(settingName);
            } else if (APPLY_TO_DATA_STREAM_ONLY.contains(settingName)) {
                appliedToDataStreamOnly.add(settingName);
            }
        }
        final List<Index> concreteIndices = clusterService.state()
            .projectState(projectResolver.getProjectId())
            .metadata()
            .dataStreams()
            .get(dataStreamName)
            .getIndices();
        final List<UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError> indexSettingErrors = new ArrayList<>();

        CountDownActionListener indexCountDownListener = new CountDownActionListener(concreteIndices.size() + 1, new ActionListener<>() {
            // Called when all indices for all settings are complete
            @Override
            public void onResponse(Void unused) {
                DataStream dataStream = clusterService.state()
                    .projectState(projectResolver.getProjectId())
                    .metadata()
                    .dataStreams()
                    .get(dataStreamName);
                listener.onResponse(
                    new UpdateDataStreamSettingsAction.DataStreamSettingsResponse(
                        dataStreamName,
                        true,
                        null,
                        settingsFilter.filter(dataStream.getSettings()),
                        settingsFilter.filter(
                            dataStream.getEffectiveSettings(clusterService.state().projectState(projectResolver.getProjectId()).metadata())
                        ),
                        new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndicesSettingsResult(
                            appliedToDataStreamOnly,
                            appliedToDataStreamAndBackingIndices,
                            indexSettingErrors
                        )
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
        indexCountDownListener.onResponse(null); // handles the case where there were zero indices
        Settings applyToIndexSettings = builder().loadFromMap(settingsToApply).build();
        for (Index index : concreteIndices) {
            updateSettingsOnSingleIndex(index, applyToIndexSettings, masterNodeTimeout, ackTimeout, new ActionListener<>() {
                @Override
                public void onResponse(UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError indexSettingError) {
                    if (indexSettingError != null) {
                        indexSettingErrors.add(indexSettingError);
                    }
                    indexCountDownListener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    indexCountDownListener.onFailure(e);
                }
            });
        }
    }

    private void updateSettingsOnSingleIndex(
        Index index,
        Settings requestSettings,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError> listener
    ) {
        if (requestSettings.isEmpty()) {
            listener.onResponse(null);
        } else {
            final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
            ClusterBlockException blockException = clusterService.state()
                .blocks()
                .indicesBlockedException(projectMetadata.id(), ClusterBlockLevel.METADATA_WRITE, new String[] { index.getName() });
            if (blockException != null) {
                listener.onResponse(
                    new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError(
                        index.getName(),
                        blockException.getMessage()
                    )
                );
                return;
            }
            updateSettingsService.updateSettings(
                new UpdateSettingsClusterStateUpdateRequest(
                    projectResolver.getProjectId(),
                    masterNodeTimeout,
                    ackTimeout,
                    requestSettings,
                    UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                    UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES,
                    index
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse response) {
                        UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError error;
                        if (response.isAcknowledged() == false) {
                            error = new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError(
                                index.getName(),
                                "Updating settings not acknowledged for unknown reason"
                            );
                        } else {
                            error = null;
                        }
                        listener.onResponse(error);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onResponse(
                            new UpdateDataStreamSettingsAction.DataStreamSettingsResponse.IndexSettingError(index.getName(), e.getMessage())
                        );
                    }
                }
            );
        }

    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDataStreamSettingsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
