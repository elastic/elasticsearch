/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.DataStreamsActionUtil;
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the data streams from the cluster state, filters the ones that do not have a data stream lifecycle configured and then returns
 * a list of the data stream name and respective lifecycle configuration.
 */
public class TransportGetDataStreamLifecycleAction extends TransportLocalProjectMetadataAction<
    GetDataStreamLifecycleAction.Request,
    GetDataStreamLifecycleAction.Response> {
    private final ClusterSettings clusterSettings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetDataStreamLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(
            GetDataStreamLifecycleAction.INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
        clusterSettings = clusterService.getClusterSettings();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.globalRetentionSettings = globalRetentionSettings;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetDataStreamLifecycleAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataStreamLifecycleAction.Request request,
        ProjectState state,
        ActionListener<GetDataStreamLifecycleAction.Response> listener
    ) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(
            indexNameExpressionResolver,
            state.metadata(),
            request.getNames(),
            request.indicesOptions()
        );
        Map<String, DataStream> dataStreams = state.metadata().dataStreams();

        ((CancellableTask) task).ensureNotCancelled();
        listener.onResponse(
            new GetDataStreamLifecycleAction.Response(
                results.stream()
                    .map(dataStreams::get)
                    .filter(Objects::nonNull)
                    .map(
                        dataStream -> new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(
                            dataStream.getName(),
                            dataStream.getDataLifecycle(),
                            dataStream.isSystem()
                        )
                    )
                    .sorted(Comparator.comparing(GetDataStreamLifecycleAction.Response.DataStreamLifecycle::dataStreamName))
                    .toList(),
                request.includeDefaults() ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING) : null,
                globalRetentionSettings.get()
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamLifecycleAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
