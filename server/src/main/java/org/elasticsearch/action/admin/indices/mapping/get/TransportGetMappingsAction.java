/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

public class TransportGetMappingsAction extends TransportLocalProjectMetadataAction<GetMappingsRequest, GetMappingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetMappingsAction.class);

    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetMappingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        ProjectResolver projectResolver
    ) {
        super(
            GetMappingsAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indicesService = indicesService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetMappingsRequest::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected ClusterBlockException checkBlock(GetMappingsRequest request, ProjectState state) {
        return state.blocks()
            .indicesBlockedException(
                state.projectId(),
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state.metadata(), request)
            );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        final GetMappingsRequest request,
        final ProjectState state,
        final ActionListener<GetMappingsResponse> listener
    ) {
        ((CancellableTask) task).ensureNotCancelled();
        logger.trace("serving getMapping request based on version {}", state.cluster().version());
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state.metadata(), request);
        final Map<String, MappingMetadata> mappings = state.metadata()
            .findMappings(concreteIndices, indicesService.getFieldFilter(), () -> checkCancellation(task));
        listener.onResponse(new GetMappingsResponse(mappings));
    }

    private static void checkCancellation(Task task) {
        if (task instanceof CancellableTask cancellableTask) {
            cancellableTask.ensureNotCancelled();
        }
    }
}
