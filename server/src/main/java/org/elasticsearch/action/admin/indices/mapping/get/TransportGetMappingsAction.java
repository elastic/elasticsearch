/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CancellationException;

public class TransportGetMappingsAction extends TransportClusterInfoAction<GetMappingsRequest, GetMappingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetMappingsAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetMappingsAction(TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
        super(GetMappingsAction.NAME, transportService, clusterService, threadPool, actionFilters, GetMappingsRequest::new,
                indexNameExpressionResolver, GetMappingsResponse::new);
        this.indicesService = indicesService;
    }

    @Override
    protected void doMasterOperation(Task task, final GetMappingsRequest request, String[] concreteIndices, final ClusterState state,
                                     final ActionListener<GetMappingsResponse> listener) {
        logger.trace("serving getMapping request based on version {}", state.version());
        final Metadata metadata = state.metadata();
        final ImmutableOpenMap<String, MappingMetadata> mappings =
            metadata.findMappings(concreteIndices, indicesService.getFieldFilter(), () -> checkCancellation(task));
        listener.onResponse(new GetMappingsResponse(mappings));
    }

    private void checkCancellation(Task task) {
        if (task instanceof CancellableTask && ((CancellableTask) task).isCancelled()) {
            throw new CancellationException("Task cancelled");
        }
    }
}
