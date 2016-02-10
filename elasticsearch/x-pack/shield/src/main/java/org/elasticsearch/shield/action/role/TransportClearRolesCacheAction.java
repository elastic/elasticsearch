/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportClearRolesCacheAction extends TransportNodesAction<ClearRolesCacheRequest, ClearRolesCacheResponse,
        ClearRolesCacheRequest.Node, ClearRolesCacheResponse.Node> {

    private final NativeRolesStore rolesStore;

    @Inject
    public TransportClearRolesCacheAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                          ClusterService clusterService, TransportService transportService, ActionFilters actionFilters,
                                          NativeRolesStore rolesStore, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ClearRolesCacheAction.NAME, clusterName, threadPool, clusterService, transportService,
                actionFilters, indexNameExpressionResolver, ClearRolesCacheRequest::new, ClearRolesCacheRequest.Node::new,
                ThreadPool.Names.MANAGEMENT);
        this.rolesStore = rolesStore;
    }

    @Override
    protected ClearRolesCacheResponse newResponse(ClearRolesCacheRequest request, AtomicReferenceArray nodesResponses) {
        List<ClearRolesCacheResponse.Node> responses = new ArrayList<>(nodesResponses.length());
        for (int i = 0; i < nodesResponses.length(); i++) {
            Object resp = nodesResponses.get(i);
            if (resp instanceof ClearRolesCacheResponse.Node) {
                responses.add((ClearRolesCacheResponse.Node) resp);
            } else if (resp == null) {
                // null is possible if there is an error and we do not accumulate exceptions...
                throw new IllegalArgumentException("node response [" + resp.getClass() + "] is not the correct type");
            }
        }
        return new ClearRolesCacheResponse(clusterName, responses.toArray(new ClearRolesCacheResponse.Node[responses.size()]));
    }

    @Override
    protected ClearRolesCacheRequest.Node newNodeRequest(String nodeId, ClearRolesCacheRequest request) {
        return new ClearRolesCacheRequest.Node(request, nodeId);
    }

    @Override
    protected ClearRolesCacheResponse.Node newNodeResponse() {
        return new ClearRolesCacheResponse.Node();
    }

    @Override
    protected ClearRolesCacheResponse.Node nodeOperation(ClearRolesCacheRequest.Node request) {
        if (request.names == null || request.names.length == 0) {
            rolesStore.invalidateAll();
        } else {
            for (String role : request.names) {
                rolesStore.invalidate(role);
            }
        }
        return new ClearRolesCacheResponse.Node(clusterService.localNode());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }
}
