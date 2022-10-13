/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataMigrateToDataStreamService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class MigrateToDataStreamTransportAction extends AcknowledgedTransportMasterNodeAction<MigrateToDataStreamAction.Request> {

    private final MetadataMigrateToDataStreamService metadataMigrateToDataStreamService;

    @Inject
    public MigrateToDataStreamTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        super(
            MigrateToDataStreamAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MigrateToDataStreamAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.metadataMigrateToDataStreamService = new MetadataMigrateToDataStreamService(
            threadPool,
            clusterService,
            indicesService,
            metadataCreateIndexService
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        MigrateToDataStreamAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest updateRequest =
            new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                request.getAliasName(),
                request.masterNodeTimeout(),
                request.timeout()
            );
        metadataMigrateToDataStreamService.migrateToDataStream(updateRequest, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(MigrateToDataStreamAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
