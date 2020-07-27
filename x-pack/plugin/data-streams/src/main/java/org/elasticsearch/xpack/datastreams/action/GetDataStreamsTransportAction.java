/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetDataStreamsTransportAction extends TransportMasterNodeReadAction<
    GetDataStreamAction.Request,
    GetDataStreamAction.Response> {

    private static final Logger LOGGER = LogManager.getLogger(GetDataStreamsTransportAction.class);

    @Inject
    public GetDataStreamsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDataStreamAction.Request::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDataStreamAction.Response read(StreamInput in) throws IOException {
        return new GetDataStreamAction.Response(in);
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDataStreamAction.Request request,
        ClusterState state,
        ActionListener<GetDataStreamAction.Response> listener
    ) throws Exception {
        List<DataStream> dataStreams = getDataStreams(state, indexNameExpressionResolver, request);
        List<GetDataStreamAction.Response.DataStreamInfo> dataStreamInfos = new ArrayList<>(dataStreams.size());
        for (DataStream dataStream : dataStreams) {
            String indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dataStream.getName(), false);
            String ilmPolicyName = null;
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
            ClusterStateHealth streamHealth = new ClusterStateHealth(
                state,
                dataStream.getIndices().stream().map(Index::getName).toArray(String[]::new)
            );
            dataStreamInfos.add(
                new GetDataStreamAction.Response.DataStreamInfo(dataStream, streamHealth.getStatus(), indexTemplate, ilmPolicyName)
            );
        }
        listener.onResponse(new GetDataStreamAction.Response(dataStreamInfos));
    }

    static List<DataStream> getDataStreams(
        ClusterState clusterState,
        IndexNameExpressionResolver iner,
        GetDataStreamAction.Request request
    ) {
        List<String> results = iner.dataStreamNames(clusterState, request.indicesOptions(), request.getNames());
        Map<String, DataStream> dataStreams = clusterState.metadata().dataStreams();

        return results.stream().map(dataStreams::get).sorted(Comparator.comparing(DataStream::getName)).collect(Collectors.toList());
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
