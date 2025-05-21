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
import org.elasticsearch.action.datastreams.UpdateDataStreamMappingsAction;
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
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TransportUpdateDataStreamMappingsAction extends TransportMasterNodeAction<
    UpdateDataStreamMappingsAction.Request,
    UpdateDataStreamMappingsAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportUpdateDataStreamMappingsAction.class);
    private static final Set<String> APPLY_TO_BACKING_INDICES = Set.of("index.lifecycle.name");
    private static final Set<String> APPLY_TO_DATA_STREAM_ONLY = Set.of("index.number_of_shards");
    private final MetadataDataStreamsService metadataDataStreamsService;
    private final MetadataUpdateSettingsService updateSettingsService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ProjectResolver projectResolver;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportUpdateDataStreamMappingsAction(
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
            UpdateDataStreamMappingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateDataStreamMappingsAction.Request::new,
            UpdateDataStreamMappingsAction.Response::new,
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
        UpdateDataStreamMappingsAction.Request request,
        ClusterState state,
        ActionListener<UpdateDataStreamMappingsAction.Response> listener
    ) throws Exception {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterService.state(),
            IndicesOptions.DEFAULT,
            request.indices()
        );
        List<UpdateDataStreamMappingsAction.DataStreamMappingsResponse> dataStreamSettingsResponse = new ArrayList<>();
        CountDownActionListener countDownListener = new CountDownActionListener(dataStreamNames.size() + 1, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                listener.onResponse(new UpdateDataStreamMappingsAction.Response(dataStreamSettingsResponse));
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
                request.getMappings(),
                request.masterNodeTimeout(),
                request.ackTimeout(),
                new ActionListener<>() {
                    @Override
                    public void onResponse(UpdateDataStreamMappingsAction.DataStreamMappingsResponse dataStreamResponse) {
                        dataStreamSettingsResponse.add(dataStreamResponse);
                        countDownListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        dataStreamSettingsResponse.add(
                            new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                                dataStreamName,
                                false,
                                e.getMessage(),
                                Mapping.EMPTY.toCompressedXContent(),
                                Mapping.EMPTY.toCompressedXContent()
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
        CompressedXContent mappingsOverrides,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        ActionListener<UpdateDataStreamMappingsAction.DataStreamMappingsResponse> listener
    ) {
        logger.debug("updating mappings for {}", dataStreamName);
        if (systemIndices.isSystemDataStream(dataStreamName)) {
            listener.onResponse(
                new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                    dataStreamName,
                    false,
                    "Cannot update a system data stream",
                    Mapping.EMPTY.toCompressedXContent(),
                    Mapping.EMPTY.toCompressedXContent()
                )
            );
            return;
        }
        metadataDataStreamsService.updateMappings(
            projectResolver.getProjectId(),
            masterNodeTimeout,
            ackTimeout,
            dataStreamName,
            mappingsOverrides,
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged()) {
                        DataStream dataStream = clusterService.state()
                            .projectState(projectResolver.getProjectId())
                            .metadata()
                            .dataStreams()
                            .get(dataStreamName);
                        try {
                            listener.onResponse(
                                new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                                    dataStreamName,
                                    true,
                                    null,
                                    mappingsOverrides,
                                    dataStream.getEffectiveMappings(
                                        clusterService.state().projectState(projectResolver.getProjectId()).metadata()
                                    )
                                )
                            );
                        } catch (IOException e) {
                            listener.onResponse(
                                new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                                    dataStreamName,
                                    false,
                                    e.getMessage(),
                                    Mapping.EMPTY.toCompressedXContent(),
                                    Mapping.EMPTY.toCompressedXContent()
                                )
                            );
                        }
                    } else {
                        listener.onResponse(
                            new UpdateDataStreamMappingsAction.DataStreamMappingsResponse(
                                dataStreamName,
                                false,
                                "Updating mappings not accepted for unknown reasons",
                                Mapping.EMPTY.toCompressedXContent(),
                                Mapping.EMPTY.toCompressedXContent()
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

    @Override
    protected ClusterBlockException checkBlock(UpdateDataStreamMappingsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
