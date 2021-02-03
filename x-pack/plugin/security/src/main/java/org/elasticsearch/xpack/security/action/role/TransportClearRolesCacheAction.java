/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheRequest;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheResponse;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.io.IOException;
import java.util.List;

public class TransportClearRolesCacheAction extends TransportNodesAction<ClearRolesCacheRequest, ClearRolesCacheResponse,
        ClearRolesCacheRequest.Node, ClearRolesCacheResponse.Node> {

    private final CompositeRolesStore rolesStore;

    @Inject
    public TransportClearRolesCacheAction(ThreadPool threadPool, ClusterService clusterService,
                                          TransportService transportService, ActionFilters actionFilters, CompositeRolesStore rolesStore) {
        super(ClearRolesCacheAction.NAME, threadPool, clusterService, transportService, actionFilters, ClearRolesCacheRequest::new,
            ClearRolesCacheRequest.Node::new, ThreadPool.Names.MANAGEMENT, ClearRolesCacheResponse.Node.class);
        this.rolesStore = rolesStore;
    }

    @Override
    protected ClearRolesCacheResponse newResponse(ClearRolesCacheRequest request,
                                                  List<ClearRolesCacheResponse.Node> responses, List<FailedNodeException> failures) {
        return new ClearRolesCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearRolesCacheRequest.Node newNodeRequest(ClearRolesCacheRequest request) {
        return new ClearRolesCacheRequest.Node(request);
    }

    @Override
    protected ClearRolesCacheResponse.Node newNodeResponse(StreamInput in) throws IOException {
        return new ClearRolesCacheResponse.Node(in);
    }

    @Override
    protected ClearRolesCacheResponse.Node nodeOperation(ClearRolesCacheRequest.Node request, Task task) {
        if (request.getNames() == null || request.getNames().length == 0) {
            rolesStore.invalidateAll();
        } else {
            for (String role : request.getNames()) {
                rolesStore.invalidate(role);
            }
        }
        return new ClearRolesCacheResponse.Node(clusterService.localNode());
    }

}
