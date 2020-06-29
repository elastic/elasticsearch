/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action.privilege;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheResponse;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;

import java.io.IOException;
import java.util.List;

public class TransportClearPrivilegesCacheAction extends TransportNodesAction<ClearPrivilegesCacheRequest, ClearPrivilegesCacheResponse,
    ClearPrivilegesCacheRequest.Node, ClearPrivilegesCacheResponse.Node> {

    private final NativePrivilegeStore privilegesStore;
    private final CompositeRolesStore rolesStore;

    @Inject
    public TransportClearPrivilegesCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NativePrivilegeStore privilegesStore,
        CompositeRolesStore rolesStore) {
        super(
            ClearPrivilegesCacheAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearPrivilegesCacheRequest::new,
            ClearPrivilegesCacheRequest.Node::new,
            ThreadPool.Names.MANAGEMENT,
            ClearPrivilegesCacheResponse.Node.class);
        this.privilegesStore = privilegesStore;
        this.rolesStore = rolesStore;
    }

    @Override
    protected ClearPrivilegesCacheResponse newResponse(
        ClearPrivilegesCacheRequest request, List<ClearPrivilegesCacheResponse.Node> nodes, List<FailedNodeException> failures) {
        return new ClearPrivilegesCacheResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected ClearPrivilegesCacheRequest.Node newNodeRequest(ClearPrivilegesCacheRequest request) {
        return new ClearPrivilegesCacheRequest.Node(request);
    }

    @Override
    protected ClearPrivilegesCacheResponse.Node newNodeResponse(StreamInput in) throws IOException {
        return new ClearPrivilegesCacheResponse.Node(in);
    }

    @Override
    protected ClearPrivilegesCacheResponse.Node nodeOperation(ClearPrivilegesCacheRequest.Node request, Task task) {
        if (request.getApplicationNames() == null || request.getApplicationNames().length == 0) {
            privilegesStore.invalidateAll();
        } else {
            privilegesStore.invalidate(List.of(request.getApplicationNames()));
        }
        if (request.clearRolesCache()) {
            rolesStore.invalidateAll();
        }
        return new ClearPrivilegesCacheResponse.Node(clusterService.localNode());
    }
}
