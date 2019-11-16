/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.role;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesAction;
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesResponse;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;

import java.io.IOException;
import java.util.List;

public class TransportGetFileRolesAction extends TransportNodesAction<GetFileRolesRequest, GetFileRolesResponse,
    GetFileRolesAction.NodeRequest, GetFileRolesAction.NodeResponse> {

    private final FileRolesStore fileRolesStore;

    @Inject
    public TransportGetFileRolesAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                          ActionFilters actionFilters, FileRolesStore fileRolesStore) {
        super(GetFileRolesAction.NAME, threadPool, clusterService, transportService, actionFilters,
            GetFileRolesRequest::new,
            GetFileRolesAction.NodeRequest::new,
            ThreadPool.Names.GENERIC, //TODO: Is this the right threadpool?
            GetFileRolesAction.NodeResponse.class);
        this.fileRolesStore = fileRolesStore;
    }

    @Override
    protected GetFileRolesResponse newResponse(GetFileRolesRequest request, List<GetFileRolesAction.NodeResponse> nodeResponses,
                                               List<FailedNodeException> failures) {
        return new GetFileRolesResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected GetFileRolesAction.NodeRequest newNodeRequest(GetFileRolesRequest request) {
        return new GetFileRolesAction.NodeRequest(request);
    }

    @Override
    protected GetFileRolesAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetFileRolesAction.NodeResponse(in);
    }

    @Override
    protected GetFileRolesAction.NodeResponse nodeOperation(GetFileRolesAction.NodeRequest request, Task task) {
        return new GetFileRolesAction.NodeResponse(transportService.getLocalNode(), fileRolesStore.getAllRoleDescriptors());
    }
}
