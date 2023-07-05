/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class GetDataStreamsTransportAction extends TransportMasterNodeReadAction<
    GetDataStreamAction.Request,
    GetDataStreamAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(GetDataStreamsTransportAction.class);
    private final SystemIndices systemIndices;
    private final ClusterSettings clusterSettings;

    @Inject
    public GetDataStreamsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamAction.Request::new,
            indexNameExpressionResolver,
            GetDataStreamAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.systemIndices = systemIndices;
        clusterSettings = clusterService.getClusterSettings();
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamAction.Response> listener
    ) throws Exception {
        listener.onResponse(innerOperation(state, request, indexNameExpressionResolver, systemIndices, clusterSettings));
    }

    static GetDataStreamAction.Response innerOperation(
        ClusterState state,
        GetDataStreamAction.Request request,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        ClusterSettings clusterSettings
    ) {
        List<DataStream> dataStreams = getDataStreams(state, indexNameExpressionResolver, request);
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = new ArrayList<>(dataStreams.size());
        for (DataStream dataStream : dataStreams) {
            final String indexTemplate;
            String ilmPolicyName = null;
            if (dataStream.isSystem()) {
                SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(dataStream.getName());
                indexTemplate = dataStreamDescriptor != null ? dataStreamDescriptor.getDataStreamName() : null;
                if (dataStreamDescriptor != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(
                        dataStreamDescriptor.getComposableIndexTemplate(),
                        dataStreamDescriptor.getComponentTemplates()
                    );
                    ilmPolicyName = settings.get("index.lifecycle.name");
                }
            } else {
                indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dataStream.getName(), false);
                if (indexTemplate != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
                    ilmPolicyName = settings.get("index.lifecycle.name");
                } else {
                    LOGGER.warn(
                        "couldn't find any matching template for data stream [{}]. has it been restored (and possibly renamed)"
                            + "from a snapshot?",
                        dataStream.getName()
                    );
                }
            }

            ClusterStateHealth streamHealth = new ClusterStateHealth(
                state,
                dataStream.getIndices().stream().map(Index::getName).toArray(String[]::new)
            );

            GetDataStreamAction.Response.TimeSeries timeSeries = null;
            if (dataStream.getIndexMode() == IndexMode.TIME_SERIES) {
                List<Tuple<Instant, Instant>> ranges = new ArrayList<>();
                Tuple<Instant, Instant> current = null;
                String previousIndexName = null;
                for (Index index : dataStream.getIndices()) {
                    IndexMetadata metadata = state.getMetadata().index(index);
                    if (metadata.getIndexMode() != IndexMode.TIME_SERIES) {
                        continue;
                    }
                    Instant start = metadata.getTimeSeriesStart();
                    Instant end = metadata.getTimeSeriesEnd();
                    if (current == null) {
                        current = new Tuple<>(start, end);
                    } else if (current.v2().compareTo(start) == 0) {
                        current = new Tuple<>(current.v1(), end);
                    } else if (current.v2().compareTo(start) < 0) {
                        ranges.add(current);
                        current = new Tuple<>(start, end);
                    } else {
                        String message = "previous backing index ["
                            + previousIndexName
                            + "] range ["
                            + current.v1()
                            + "/"
                            + current.v2()
                            + "] range is colliding with current backing ["
                            + index.getName()
                            + "] index range ["
                            + start
                            + "/"
                            + end
                            + "]";
                        assert current.v2().compareTo(start) < 0 : message;
                        LOGGER.warn(message);
                    }
                    previousIndexName = index.getName();
                }
                if (current != null) {
                    ranges.add(current);
                }
                timeSeries = new GetDataStreamAction.Response.TimeSeries(ranges);
            }

            dataStreamInfos.add(
                new GetDataStreamAction.Response.DataStreamInfo(
                    dataStream,
                    streamHealth.getStatus(),
                    indexTemplate,
                    ilmPolicyName,
                    timeSeries
                )
            );
        }
        return new GetDataStreamAction.Response(
            dataStreamInfos,
            request.includeDefaults() && DataStreamLifecycle.isEnabled()
                ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING)
                : null
        );
    }

    static List<DataStream> getDataStreams(
        ClusterState clusterState,
        IndexNameExpressionResolver iner,
        GetDataStreamAction.Request request
    ) {
        List<String> results = DataStreamsActionUtil.getDataStreamNames(iner, clusterState, request.getNames(), request.indicesOptions());
        Map<String, DataStream> dataStreams = clusterState.metadata().dataStreams();

        return results.stream().map(dataStreams::get).sorted(Comparator.comparing(DataStream::getName)).toList();
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
