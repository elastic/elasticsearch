/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportGetDesiredNodesAction extends TransportMasterNodeReadAction<
    GetDesiredNodesAction.Request,
    GetDesiredNodesAction.Response> {
    @Inject
    public TransportGetDesiredNodesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            GetDesiredNodesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDesiredNodesAction.Request::new,
            GetDesiredNodesAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetDesiredNodesAction.Request request,
        ClusterState state,
        ActionListener<GetDesiredNodesAction.Response> listener
    ) throws Exception {
        final DesiredNodes latestDesiredNodes = DesiredNodes.latestFromClusterState(state);
        if (latestDesiredNodes == null) {
            listener.onFailure(new ResourceNotFoundException("Desired nodes not found"));
        } else {
            listener.onResponse(new GetDesiredNodesAction.Response(latestDesiredNodes));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDesiredNodesAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
