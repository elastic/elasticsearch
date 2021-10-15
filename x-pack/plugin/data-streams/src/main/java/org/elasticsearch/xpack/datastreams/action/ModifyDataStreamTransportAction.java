/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.ModifyDataStreamsAction;

public class ModifyDataStreamTransportAction extends AcknowledgedTransportMasterNodeAction<
    MetadataDataStreamsService.ModifyDataStreamRequest> {

    private final MetadataDataStreamsService metadataDataStreamsService;

    @Inject
    public ModifyDataStreamTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        MetadataDataStreamsService metadataDataStreamsService
    ) {
        super(
            ModifyDataStreamsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MetadataDataStreamsService.ModifyDataStreamRequest::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.metadataDataStreamsService = metadataDataStreamsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        MetadataDataStreamsService.ModifyDataStreamRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        metadataDataStreamsService.modifyDataStream(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(MetadataDataStreamsService.ModifyDataStreamRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
