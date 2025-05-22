/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataMigrateToDataStreamService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportMigrateToDataStreamAction extends AcknowledgedTransportMasterNodeProjectAction<MigrateToDataStreamAction.Request> {

    private final MetadataMigrateToDataStreamService metadataMigrateToDataStreamService;

    @Inject
    public TransportMigrateToDataStreamAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
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
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
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
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest updateRequest =
            new MetadataMigrateToDataStreamService.MigrateToDataStreamClusterStateUpdateRequest(
                request.getAliasName(),
                request.masterNodeTimeout(),
                request.ackTimeout()
            );
        metadataMigrateToDataStreamService.migrateToDataStream(state.projectId(), updateRequest, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(MigrateToDataStreamAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
